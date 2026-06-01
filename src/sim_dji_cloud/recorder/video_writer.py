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
   ``-progress pipe:1 -stats_period 0.1``. A daemon thread drains stdout,
   tracks both the **first** and **latest** non-zero ``out_time_us=…``
   observations plus the wall ms at which the latest one arrived, and
   back-calculates the wall clock of the first input frame as
   ``last_observed_at_ms - (last_us - first_us) // 1000``. It then
   **renames** the file to ``main_<first_frame_wall_ms>.mp4`` (POSIX rename
   is atomic — ffmpeg's open fd keeps writing through the inode). The same
   value is patched into the segment's ``ffmpeg_start_wall_ms`` so the
   manifest is accurate.

   Why the PTS-delta form and not the older ``now_ms - out_time_us/1000``:
   ffmpeg with ``-c copy`` from RTMP passes the source's PTS straight
   through to the output. The source's PTS is the **encoder's monotonic
   clock** (often "seconds since the encoder booted"), so it does NOT start
   at 0. The old form silently assumed ``first_frame_pts == 0`` and produced
   wall times days before ``popen_wall_ms`` whenever the encoder had been up
   for any non-trivial time. ``(last_us - first_us)`` is the file's recorded
   duration regardless of where the source PTS started, so subtracting it
   from the latest wall observation lands on the actual first-frame wall.

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
        # 每次 attempt 起一个 progress reader 线程；stop() 必须 join 完才能
        # 写 timing.json，否则会跟"reader 还在 rename"撞 race。
        self._progress_threads: list[threading.Thread] = []
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
        """Stop supervisor + finalize ffmpeg + drain progress readers + write
        timing.json + close log.

        Order matters: progress reader threads must finish (= apply their
        final rename + patch to ``_segments[-1]``) **before** ``_write_timing``
        snapshots ``_segments`` into ``main.timing.json``. Otherwise we'd
        record the placeholder ``main_<popen_ms>.mp4`` filename even when the
        rename was about to happen.
        """
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

        # ffmpeg now exited (or was killed); progress readers' stdout pipes
        # close, their for-loops exit, their finally fires the rename. Join
        # them so the rename lands in self._segments before _write_timing.
        for t in self._progress_threads:
            t.join(timeout=timeout_s)

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
                    "popen_at_recv_ms": None,
                    "duration_ms": duration_ms,
                    "segments": [],
                }
            seg = self._segments[0]
            started = seg["ffmpeg_start_wall_ms"]
            # 老段（升级前的 timing.json）可能没有 ffmpeg_popen_wall_ms 字段，
            # 这种情况下退回 started_at_recv_ms（语义上视频"开始拉流"≈"已开始录"）。
            popen = seg.get("ffmpeg_popen_wall_ms", started)
            video_rel = f"video/{seg['file']}"
        return {
            "file": video_rel,
            "source_url": self.source_url,
            "started_at_recv_ms": started,
            # popen_at_recv_ms：ffmpeg Popen 时的墙钟（"开始拉流"），可作为下游
            # 视频对齐的备选锚点；如果回放时视频比数据晚，可以尝试用这个替代
            # started_at_recv_ms 看效果。
            "popen_at_recv_ms": popen,
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
                # ffmpeg_popen_wall_ms：Popen() 返回那一刻的墙钟，永远是"开始拉流"
                # 的时间锚，progress reader **不会**改写它。下游回放时可以用它做
                # 视频对齐的"早锚点"（vs ffmpeg_start_wall_ms 的"第一帧锚点"），
                # 比较两种 anchor 哪种实测同步效果更好。
                "ffmpeg_popen_wall_ms": launch_ms,
                # ffmpeg_start_wall_ms：默认与 popen 相同，progress reader 拿到
                # 第一帧后会被 patch 成"第一帧到达"的墙钟。
                "ffmpeg_start_wall_ms": launch_ms,
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
        self._progress_threads.append(progress_thread)

    def _read_progress_and_rename(
        self,
        proc: subprocess.Popen,
        placeholder_filename: str,
    ) -> None:
        """Drain ffmpeg's ``-progress`` stdout. Track the FIRST and LATEST
        non-zero ``out_time_*`` observations and, after the pipe closes
        (ffmpeg exits or we stop it), back-calculate first-frame wall ms via
        the PTS-delta form, then rename the output file.

        Formula:

            first_frame_wall_ms = last_observed_at_ms - (last_us - first_us) // 1000

        Why the **delta** form and not the older ``now_ms - out_time_us``:
            ffmpeg ``-c copy`` from RTMP forwards the source's PTS directly
            into the output. Most live encoders (e.g. DJI Dock) emit PTS that
            is their **monotonic uptime**, not 0-relative. The old form
            ``now - out_time_us/1000`` assumed ``first_frame_pts == 0`` and
            therefore mis-attributed encoder uptime as "file duration",
            producing wall times days before ``popen_wall_ms`` (one such
            production log: 6.8 days early).

            ``(last_us - first_us)`` is the file's recorded duration
            regardless of where source PTS started, so subtracting it from
            the latest wall observation lands on the actual first-frame
            wall.

        Why we still pick the **latest** wall observation:
            ffmpeg's stdout for ``-progress pipe:1`` goes through libc stdio
            block buffering (4KB) for non-TTY pipes. For short recordings
            (~15s, ~30KB) the buffer may not fill mid-stream; at exit, the
            whole batch flushes and our reader observes all blocks within
            milliseconds, *all with ``now_ms`` ≈ exit_ms*. Using
            ``last_observed_at_ms`` keeps the formula correct in both
            real-time-progress and buffered-flush cases — the PTS-delta
            captures the file duration; the latest wall captures the most
            up-to-date alignment with reality.

        Why we also accept ``out_time_ms=``:
            Older ffmpeg releases (pre-4.x) emit only ``out_time_ms=`` and
            ``out_time=hh:mm:ss.uuuuuu``; the ``out_time_us=`` line was added
            later. Parsing both keeps us version-portable.

        Why rename happens in ``finally`` (after pipe close), not inline:
            Guarantees we always use the **latest** observation, including
            the tail blocks flushed at ffmpeg exit. ``stop()`` joins this
            thread before writing ``main.timing.json``, so the renamed
            filename lands in the timing record.
        """
        if proc.stdout is None:
            return
        line_count = 0
        out_time_hits = 0
        # first_us：首条 >0 的 out_time_us，作为"文件首帧 PTS"的锚。
        # 在 -c copy 下输出 PTS 直接来自源流，源 PTS 不从 0 开始，
        # 必须减掉 first_us 才能得到"文件录制时长"。
        first_us: Optional[int] = None
        last_us: Optional[int] = None
        last_observed_at_ms: Optional[int] = None
        try:
            for raw in proc.stdout:
                line_count += 1
                line = raw.decode("ascii", errors="ignore").strip()
                # Parse out_time_us= preferentially; fall back to out_time_ms=.
                us: Optional[int] = None
                if line.startswith("out_time_us="):
                    try:
                        us = int(line.split("=", 1)[1])
                    except ValueError:
                        continue
                elif line.startswith("out_time_ms="):
                    try:
                        us = int(line.split("=", 1)[1]) * 1000
                    except ValueError:
                        continue
                else:
                    continue
                if us <= 0:
                    # ffmpeg sometimes emits a zero block before any output;
                    # skip but keep counting hits for diagnostic logging.
                    continue
                out_time_hits += 1
                if first_us is None:
                    first_us = us
                last_us = us
                last_observed_at_ms = int(time.time() * 1000)
        except Exception:
            logger.exception(
                "video progress reader errored after {} lines", line_count,
            )
        finally:
            try:
                if proc.stdout is not None:
                    proc.stdout.close()
            except Exception:
                pass
        if (
            first_us is None
            or last_us is None
            or last_observed_at_ms is None
        ):
            # 没看到任何可用的 out_time_*。常见原因：ffmpeg 太老（不支持 -progress）、
            # progress 被 stderr 截获、连接没成功就退出。文件名维持 popen-time，
            # 下游回放仍可用 popen_at_recv_ms 做锚点，只是没了 first-frame 精度。
            logger.warning(
                "video progress reader: read {} lines, {} usable out_time_* "
                "observations but none > 0 → 没拿到第一帧时间，文件名保持 "
                "popen-time；timing.json 里 ffmpeg_popen_wall_ms == "
                "ffmpeg_start_wall_ms 是这种情况的标志",
                line_count, out_time_hits,
            )
            return
        # PTS-delta back-calc：(last_us - first_us) 是文件录制时长，
        # 与源 PTS 是否从 0 起无关。
        first_frame_wall_ms = last_observed_at_ms - (last_us - first_us) // 1000
        logger.info(
            "video progress: {} lines observed, {} usable; first_us={}, "
            "last_us={} at wall_ms={} → first_frame_wall_ms={} "
            "(duration={}ms)",
            line_count, out_time_hits,
            first_us, last_us, last_observed_at_ms, first_frame_wall_ms,
            (last_us - first_us) // 1000,
        )
        self._rename_to_first_frame_time(placeholder_filename, first_frame_wall_ms)

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
