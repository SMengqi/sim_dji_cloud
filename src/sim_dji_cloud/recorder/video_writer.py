"""Eager-launch + restart ffmpeg supervisor for recording RTMP video.

Design (v3.1):

The supervisor thread launches ffmpeg *immediately* — no probe phase. This
matters because the v2 probe consumed a full GOP (~5 seconds) from the RTMP
publisher every time before the real recording started, and that data is lost
forever (RTMP doesn't replay history to new subscribers). Under the
record→play-video→record self-check workflow that 5 s loss compounded every
cycle and the video eventually evaporated.

Each attempt:

1. **Eager launch.** Supervisor enters its loop and immediately ``Popen`` the
   real ffmpeg writing to a placeholder ``main_<launch_ms>.mp4`` (epoch ms at
   the moment of ``Popen``). ffmpeg gets ``-rw_timeout 5000000`` so an
   unreachable / no-publisher endpoint makes it exit within ~5 s on its own.

2. **Progress reader thread.** ffmpeg is also given
   ``-progress pipe:1 -stats_period 0.1``. A daemon thread drains stdout and,
   on the first non-zero ``out_time_us=…`` line, back-calculates the wall
   clock of the first input frame as ``now_ms − out_time_us/1000`` and
   **renames** the file to ``main_<first_frame_wall_ms>.mp4`` (POSIX rename is
   atomic — ffmpeg's open fd keeps writing through the inode). The same
   value is patched into the segment's ``ffmpeg_start_wall_ms`` so the
   manifest is accurate.

   This matters for downstream tools that align video PTS with MQTT
   ``recv_ts_ms``: ``frame_at_PTS_X_recv_ms == ffmpeg_start_wall_ms + X``. With
   the old "Popen time" timestamp, that equation was off by 1–3 s (RTMP
   handshake + I-frame wait) and the error compounded across
   record→play→record cycles.

3. **Wait for ffmpeg to exit (or for ``stop()`` to fire).** The supervisor
   polls the subprocess on a 0.5 s tick interleaved with the stop event.

4. **Classify the exit.**
   - ffmpeg still alive when stop_event fires → user-initiated finalize, keep
     the segment. ``stop()`` SIGINTs ffmpeg so the mp4 trailer is written.
   - ``ran_sec >= success_min_seconds`` (default 15 s) → ffmpeg necessarily
     received real packets, treat as completed recording, do **not** retry.
   - Else → failed start (no publisher yet / transient glitch). Delete the
     partial mp4 (under whichever name it currently has — placeholder or
     renamed), pop the segment entry, sleep ``retry_interval_s``, retry.

5. **stderr is appended, not overwritten.** ``main.ffmpeg.log`` is opened in
   ``"a"`` mode each attempt with a ``=== ffmpeg attempt at wall_ms=… ===``
   separator so failure messages across attempts are all preserved.

Not in scope:
- Concatenating multiple successful ffmpeg runs into one logical recording
  (segment stitching). If ffmpeg dies mid-flight after running ≥ 15 s, we stop
  there and the rest of the flight isn't captured.
"""

from __future__ import annotations

import json
import signal
import subprocess
import threading
import time
from pathlib import Path
from typing import Optional

from loguru import logger


class VideoWriter:
    """Supervise an ffmpeg subprocess that pulls RTMP into ``video/main_<ms>.mp4``.

    See module docstring for the eager-launch + restart-on-fast-exit +
    first-frame-rename design.

    Parameters
    ----------
    source_url:
        RTMP / HTTP-FLV URL to pull from.
    output_dir:
        ``<flight_dir>/video/``. Created if missing.
    extra_args:
        Extra ffmpeg flags appended just before the output filename. The default
        command already supplies ``-rw_timeout``, ``-c copy``, ``-f mp4``, the
        ``-map 0:v -map 0:a?`` pair, and ``-progress pipe:1 -stats_period 0.1``
        (for the first-frame detector).
    retry_interval_s:
        Sleep between a failed-start (fast exit) and the next launch attempt.
    success_min_seconds:
        How long ffmpeg has to keep running before we count the recording as
        "real" rather than "failed start". Must be greater than the
        ``-rw_timeout`` budget inside ``_build_cmd`` (5 s) plus some slack —
        15 s is chosen so a slow RTMP handshake still falls inside the
        rw_timeout budget and gets retried, but any genuine packet ingestion
        is well past it.
    """

    def __init__(
        self,
        source_url: str,
        output_dir: Path,
        extra_args: Optional[list[str]] = None,
        retry_interval_s: float = 2.0,
        success_min_seconds: float = 15.0,
    ):
        self.source_url = source_url
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.extra_args = list(extra_args or [])
        self._retry_interval_s = retry_interval_s
        self._success_min_seconds = success_min_seconds

        self._proc: Optional[subprocess.Popen] = None
        self._stderr_log = None
        self._segments: list[dict] = []
        self._filename: Optional[str] = None   # whichever name the current attempt's file has on disk

        self._stop_event = threading.Event()
        self._supervisor_thread: Optional[threading.Thread] = None
        # Protects concurrent writes to ``_filename`` / ``_segments`` between
        # the supervisor thread (cleanup) and the progress reader thread (rename).
        self._state_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Kick off the supervisor thread; ffmpeg launches inside it immediately."""
        self._stop_event.clear()
        self._supervisor_thread = threading.Thread(
            target=self._supervise,
            name="video-writer-supervisor",
            daemon=True,
        )
        self._supervisor_thread.start()
        logger.info("video supervisor started, source {}", self.source_url)

    def is_alive(self) -> bool:
        """True iff the current ffmpeg subprocess is running."""
        return self._proc is not None and self._proc.poll() is None

    def stop(self, timeout_s: float = 10.0) -> None:
        """Stop supervisor + finalize ffmpeg + write timing.json + close log."""
        self._stop_event.set()
        if self._supervisor_thread is not None:
            self._supervisor_thread.join(timeout=timeout_s)

        if self._proc is not None and self._proc.poll() is None:
            self._proc.send_signal(signal.SIGINT)
            try:
                self._proc.wait(timeout=timeout_s)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                self._proc.wait()

        if self._stderr_log is not None:
            try:
                self._stderr_log.close()
            except Exception:
                pass
            self._stderr_log = None

        self._write_timing()

    def manifest_video_block(self, duration_ms: int) -> dict:
        """Return ``manifest.video`` block. ``file=None`` & ``segments=[]`` if no
        ffmpeg attempt was ever classified as successful.

        Reads filename from the segment list (not ``self._filename``) so the
        block is consistent with what's on disk even if rename happened in a
        different thread.
        """
        with self._state_lock:
            if not self._segments:
                return {
                    "file": None,
                    "source_url": self.source_url,
                    "started_at_recv_ms": None,
                    "duration_ms": duration_ms,
                    "segments": [],
                }
            seg = self._segments[0]
            started = seg["ffmpeg_start_wall_ms"]
            video_rel = f"video/{seg['file']}"
        return {
            "file": video_rel,
            "source_url": self.source_url,
            "started_at_recv_ms": started,
            "duration_ms": duration_ms,
            "segments": [
                {"start_ms": 0, "end_ms": duration_ms, "file": video_rel},
            ],
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _supervise(self) -> None:
        """Eager-launch loop: ffmpeg, wait, classify, maybe retry.

        Classification is driven by ``self._proc.poll()`` (is ffmpeg still
        running?), not by ``self._stop_event``. This matters in the corner case
        where ffmpeg fast-exits *and* ``stop()`` fires concurrently — we still
        need to clean up the failed attempt so it doesn't leave an orphan
        segment in the manifest.
        """
        while not self._stop_event.is_set():
            launch_t = time.monotonic()
            self._launch_ffmpeg()

            # Wait until ffmpeg exits OR stop() fires. 0.5 s tick keeps stop()
            # latency low without busy-spinning.
            while self._proc.poll() is None and not self._stop_event.is_set():
                self._stop_event.wait(timeout=0.5)

            # If ffmpeg is still alive, stop() must be the reason we got here.
            # Keep the current segment — stop() will SIGINT ffmpeg so the
            # trailer gets written.
            if self._proc.poll() is None:
                return

            # ffmpeg exited on its own (or before our stop() got to it).
            # Classify by how long it ran.
            ran_sec = time.monotonic() - launch_t
            if ran_sec >= self._success_min_seconds:
                logger.info(
                    "ffmpeg exited after {:.1f}s (≥ {}s) — treating as completed recording",
                    ran_sec, self._success_min_seconds,
                )
                return

            logger.warning(
                "ffmpeg exited within {:.1f}s (< {}s threshold) — treating as failed "
                "start, will retry in {:.1f}s; stderr in main.ffmpeg.log",
                ran_sec, self._success_min_seconds, self._retry_interval_s,
            )
            self._cleanup_failed_attempt()
            if self._stop_event.is_set():
                break
            self._stop_event.wait(timeout=self._retry_interval_s)

        # Outer loop ended without any successful run.
        if not self._segments:
            logger.warning(
                "video supervisor stopped before any successful ffmpeg run — "
                "no video recorded for this flight (RTMP source {})",
                self.source_url,
            )

    def _launch_ffmpeg(self) -> None:
        """One ffmpeg attempt. Records a tentative segment entry and starts a
        progress-reader daemon thread to detect the first frame and rename the
        file to its wall-clock timestamp.
        """
        launch_ms = int(time.time() * 1000)
        placeholder_filename = f"main_{launch_ms}.mp4"
        cmd = self._build_cmd(placeholder_filename)

        # Append, not overwrite — preserve failure traces from earlier attempts.
        log_path = self.output_dir / "main.ffmpeg.log"
        self._stderr_log = open(log_path, "a")
        self._stderr_log.write(
            f"\n=== ffmpeg attempt at wall_ms={launch_ms} (file={placeholder_filename}) ===\n"
        )
        self._stderr_log.flush()

        self._proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,   # for -progress
            stderr=self._stderr_log,
        )
        with self._state_lock:
            self._filename = placeholder_filename
            self._segments.append({
                "file": placeholder_filename,
                "ffmpeg_start_wall_ms": launch_ms,   # patched by progress reader
                "pts_offset_ms": 0,
            })
        logger.info("ffmpeg launched: video/{} (pid={})", placeholder_filename, self._proc.pid)

        progress_thread = threading.Thread(
            target=self._read_progress_and_rename,
            args=(self._proc, placeholder_filename),
            name="video-progress",
            daemon=True,
        )
        progress_thread.start()

    def _read_progress_and_rename(
        self,
        proc: subprocess.Popen,
        placeholder_filename: str,
    ) -> None:
        """Drain ffmpeg's ``-progress`` stdout. On the first non-zero
        ``out_time_us=<N>`` line, back-calculate first-frame wall ms and rename
        the output file. Keeps reading until the pipe closes (ffmpeg exits) so
        the kernel pipe never fills up and blocks ffmpeg.
        """
        if proc.stdout is None:
            return
        renamed = False
        try:
            for raw in proc.stdout:
                if renamed:
                    continue
                line = raw.decode("ascii", errors="ignore").strip()
                if not line.startswith("out_time_us="):
                    continue
                try:
                    us = int(line.split("=", 1)[1])
                except ValueError:
                    continue
                if us <= 0:
                    # ffmpeg sometimes emits a zero block before any output;
                    # skip and keep watching for the first real frame.
                    continue
                now_ms = int(time.time() * 1000)
                first_frame_wall_ms = now_ms - us // 1000
                self._rename_to_first_frame_time(
                    placeholder_filename, first_frame_wall_ms
                )
                renamed = True
        except Exception:
            logger.exception("video progress reader errored")
        finally:
            try:
                if proc.stdout is not None:
                    proc.stdout.close()
            except Exception:
                pass

    def _rename_to_first_frame_time(
        self,
        placeholder_filename: str,
        first_frame_wall_ms: int,
    ) -> None:
        """Atomically rename ``main_<launch_ms>.mp4`` → ``main_<first_frame_ms>.mp4``
        and patch the segment entry's ``file`` + ``ffmpeg_start_wall_ms``.

        If the file-system rename fails (extremely rare — typically only if
        cleanup got there first and deleted the placeholder), the in-memory
        segment ``ffmpeg_start_wall_ms`` is still patched to the accurate
        first-frame value. That way ``manifest.video`` stays correct even if
        the on-disk filename is stale.
        """
        new_filename = f"main_{first_frame_wall_ms}.mp4"
        if new_filename == placeholder_filename:
            # Extremely tight Popen-to-first-frame loop (< 1 ms); nothing to do.
            return

        old_path = self.output_dir / placeholder_filename
        new_path = self.output_dir / new_filename
        rename_ok = False
        try:
            old_path.rename(new_path)   # POSIX-atomic; ffmpeg's fd unaffected
            rename_ok = True
        except FileNotFoundError:
            # Cleanup may have just unlinked the placeholder; nothing to recover.
            return
        except Exception:
            logger.exception(
                "failed to rename {} → {}; manifest will still get accurate ts",
                placeholder_filename, new_filename,
            )

        with self._state_lock:
            # Only patch if this attempt's segment is still the tail of the list
            # (cleanup hasn't popped it yet).
            if self._segments and self._segments[-1]["file"] == placeholder_filename:
                if rename_ok:
                    self._segments[-1]["file"] = new_filename
                    self._filename = new_filename
                self._segments[-1]["ffmpeg_start_wall_ms"] = first_frame_wall_ms
                logger.info(
                    "first frame detected; first_frame_wall_ms={}{}",
                    first_frame_wall_ms,
                    f"; renamed video/{placeholder_filename} → video/{new_filename}"
                    if rename_ok else " (rename failed; manifest ts patched anyway)",
                )

    def _cleanup_failed_attempt(self) -> None:
        """Roll back the bookkeeping of an attempt that died too fast.

        Reads the *current* on-disk filename from the segment tail (could be
        the placeholder or the post-rename name), deletes that file, pops the
        segment, and closes the log handle.
        """
        with self._state_lock:
            if self._segments:
                seg = self._segments[-1]
                filename = seg["file"]
                p = self.output_dir / filename
                if p.exists():
                    try:
                        p.unlink()
                    except Exception:
                        logger.exception("failed to delete partial mp4 {}", p)
                self._segments.pop()
                self._filename = None

        if self._stderr_log is not None:
            try:
                self._stderr_log.close()
            except Exception:
                pass
            self._stderr_log = None

    def _build_cmd(self, filename: str) -> list[str]:
        return [
            "ffmpeg",
            "-y",
            # 5s socket I/O timeout: ffmpeg exits non-zero quickly when the
            # RTMP endpoint has no publisher yet, so the supervisor's restart
            # loop can keep probing without consuming a GOP per probe like
            # the old ffmpeg-probe did.
            "-rw_timeout", "5000000",
            "-i", self.source_url,
            # Only take video stream (+ optional audio); drop data streams
            # some docks inject (e.g. "Stream Data:none") which break the mp4
            # muxer.
            "-map", "0:v", "-map", "0:a?",
            "-c", "copy",
            "-movflags", "+faststart+frag_keyframe",
            "-f", "mp4",
            # -progress on stdout + 0.1s stats_period: feeds the progress
            # reader thread so it can back-calculate the first-frame wall ms
            # and rename the output file accordingly.
            "-progress", "pipe:1",
            "-stats_period", "0.1",
            *self.extra_args,
            str(self.output_dir / filename),
        ]

    def _write_timing(self) -> None:
        (self.output_dir / "main.timing.json").write_text(
            json.dumps({"segments": self._segments}, indent=2)
        )


def resolve_video_source_url(video_cfg: dict) -> str | None:
    """解析视频拉流 URL。

    v1：只返回 source_url_override（去空白后），空/缺失返回 None。
    OSD 自动提取（source_url_field）延后实现。
    """
    return (video_cfg.get("source_url_override") or "").strip() or None
