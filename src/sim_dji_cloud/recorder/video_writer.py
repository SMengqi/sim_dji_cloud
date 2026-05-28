"""Eager-launch + restart ffmpeg supervisor for recording RTMP video.

Design (v3):

The supervisor thread launches ffmpeg *immediately* — no probe phase. This
matters because the v2 probe consumed a full GOP (~5 seconds) from the RTMP
publisher every time before the real recording started, and that data is lost
forever (RTMP doesn't replay history to new subscribers). Under the
record→play-video→record self-check workflow that 5 s loss compounded every
cycle and the video eventually evaporated.

Instead:

1. **Eager launch.** Supervisor enters its loop and immediately ``Popen`` the
   real ffmpeg writing to ``main_<epoch_ms>.mp4``. ffmpeg gets
   ``-rw_timeout 5000000`` so an unreachable server / no-publisher endpoint
   makes it exit within ~5 s on its own.

2. **Wait for ffmpeg to exit (or for ``stop()`` to fire).** The supervisor
   polls the subprocess on a 0.5 s tick interleaved with the stop event so it
   stays responsive.

3. **Classify the exit.**
   - If ``stop_event`` is set → the user asked us to finalize. Whatever was
     written stays in the segment list; ``stop()`` SIGINTs ffmpeg so the mp4
     trailer is written cleanly.
   - Else if ``ran_sec >= success_min_seconds`` (default 15 s, ≥ 3× the
     ``-rw_timeout``) → ffmpeg necessarily received real packets. Treat as a
     completed recording even if the stream then ended; do **not** retry.
   - Else (ffmpeg died fast and we didn't ask it to) → it was a failed start
     (no publisher yet / transient handshake glitch). Delete the partial mp4,
     pop the segment entry, sleep ``retry_interval_s``, retry.

4. **stderr is appended, not overwritten.** ``main.ffmpeg.log`` is opened
   in ``"a"`` mode each attempt with a ``=== ffmpeg attempt at wall_ms=… ===``
   separator written before ffmpeg starts. This preserves failure messages
   from earlier attempts for postmortem.

What this is **not** doing:
- Concatenating multiple successful ffmpeg runs into one logical recording.
  If ffmpeg dies mid-flight after running for ≥ success_min_seconds, we stop
  there and the rest of the flight isn't captured. Continuous re-recording
  would need segment stitching in the manifest and a different ``stop_event``
  contract; out of scope.
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

    See module docstring for the eager-launch + restart-on-fast-exit design.

    Parameters
    ----------
    source_url:
        RTMP / HTTP-FLV URL to pull from.
    output_dir:
        ``<flight_dir>/video/``. Created if missing.
    extra_args:
        Extra ffmpeg flags appended just before the output filename. The default
        command already supplies ``-rw_timeout``, ``-c copy``, ``-f mp4`` and the
        ``-map 0:v -map 0:a?`` pair to drop the data stream DJI sometimes injects.
    retry_interval_s:
        Sleep between a failed-start (fast exit) and the next launch attempt.
    success_min_seconds:
        How long ffmpeg has to keep running before we count the recording as
        "real" rather than "failed start". Must be greater than the ``-rw_timeout``
        budget inside ``_build_cmd`` (5 s) plus some slack — 15 s is chosen so a
        slow RTMP handshake on a flaky link still falls inside the rw_timeout
        budget and gets retried, but any genuine packet ingestion is well past it.
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
        self._stderr_log = None   # file handle for main.ffmpeg.log (re-opened per attempt)
        self._segments: list[dict] = []   # final list = only successful attempt(s)
        self._filename: Optional[str] = None   # current attempt's filename

        self._stop_event = threading.Event()
        self._supervisor_thread: Optional[threading.Thread] = None

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
        """
        if not self._segments:
            return {
                "file": None,
                "source_url": self.source_url,
                "started_at_recv_ms": None,
                "duration_ms": duration_ms,
                "segments": [],
            }
        started = self._segments[0]["ffmpeg_start_wall_ms"]
        video_rel = f"video/{self._filename}"
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
                # stop() asked us to finalize; don't sleep into a retry.
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
        """One ffmpeg attempt: open log in APPEND mode, write separator, Popen.

        Records a tentative segment entry; ``_cleanup_failed_attempt`` removes
        it if the attempt turns out to be a fast-failure.
        """
        launch_ms = int(time.time() * 1000)
        self._filename = f"main_{launch_ms}.mp4"
        cmd = self._build_cmd(self._filename)

        # Append, not overwrite — preserve failure traces from earlier attempts.
        log_path = self.output_dir / "main.ffmpeg.log"
        self._stderr_log = open(log_path, "a")
        self._stderr_log.write(
            f"\n=== ffmpeg attempt at wall_ms={launch_ms} (file={self._filename}) ===\n"
        )
        self._stderr_log.flush()

        self._proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=self._stderr_log,
        )
        self._segments.append({
            "file": self._filename,
            "ffmpeg_start_wall_ms": launch_ms,
            "pts_offset_ms": 0,
        })
        logger.info("ffmpeg launched: video/{} (pid={})", self._filename, self._proc.pid)

    def _cleanup_failed_attempt(self) -> None:
        """Roll back the bookkeeping of an attempt that died too fast.

        - Delete the partial mp4 if it exists (typically 0 bytes or a header stub).
        - Pop the segment list entry we tentatively pushed at launch.
        - Close the log handle so the next attempt re-opens with a fresh fd
          (Popen's stderr= still works either way; closing is for tidiness and
          ensures the separator written above precedes any next-attempt content).
        """
        if self._filename is not None:
            p = self.output_dir / self._filename
            if p.exists():
                try:
                    p.unlink()
                except Exception:
                    logger.exception("failed to delete partial mp4 {}", p)
            if self._segments and self._segments[-1]["file"] == self._filename:
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
            # 5s socket I/O timeout: makes ffmpeg exit non-zero quickly when the
            # RTMP endpoint has no publisher yet, so the supervisor's restart loop
            # can keep probing without consuming a GOP per probe like the old
            # ffmpeg-probe did.
            "-rw_timeout", "5000000",
            "-i", self.source_url,
            # Only take video stream (+ optional audio); drop data streams that
            # some docks inject (e.g. "Stream Data:none") which break mp4 muxer.
            "-map", "0:v", "-map", "0:a?",
            "-c", "copy",
            "-movflags", "+faststart+frag_keyframe",
            "-f", "mp4",
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
