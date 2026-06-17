"""Eager-launch + restart ffmpeg supervisor for recording RTMP video.

Design (v3.2):

Anchor (= ``ffmpeg_start_wall_ms``, file name's epoch) is chosen by a
three-tier fallback chain, best-effort upgrade from left to right:

    popen-time  ←  in-band PTS-delta back-calc (sanity-checked)
                ←  ffprobe-based exit_wall − file_duration (authoritative)

1. **Popen-time placeholder**: when ``_launch_ffmpeg`` runs, the file is
   created as ``main_<launch_ms>.mp4`` and the segment is recorded with
   ``ffmpeg_start_wall_ms == ffmpeg_popen_wall_ms == launch_ms``. This is
   the "保留开始拉流的时间戳" baseline — works no matter what else
   breaks. Off by RTMP-handshake delay (≈1–5 s early).

2. **In-band PTS-delta back-calc** (in ``_read_progress_and_rename``):
   while ffmpeg runs, we track first/last ``out_time_us`` from
   ``-progress pipe:1`` and compute
   ``first_frame_wall_ms = last_observed_at_ms − (last_us − first_us)/1000``.
   Sanity-checked to fall in ``[popen_wall_ms, first_observed_at_ms]``;
   rejected values keep popen-time. This handles linear/monotonic source
   PTS even when it doesn't start at 0 (e.g. encoder uptime baseline).
   Defeated by sources whose ``out_time_us`` is non-monotonic at wall
   rate — sanity check then degrades gracefully to popen-time.

3. **ffprobe-based override** (in ``_finalize_segment_via_ffprobe``,
   called from ``stop()`` after ffmpeg exits): ``ffprobe`` the on-disk mp4
   for its container duration, then back-calc
   ``first_frame_wall_ms = exit_wall_ms − duration_ms``. ffprobe reads
   the muxer's actual PTS so it's immune to ``-progress``'s
   unreliability; this is the authoritative answer when ffprobe is
   available. Sanity-checked the same way; rejection keeps the in-band
   anchor.

The reason both (2) and (3) exist: (2) makes the on-disk filename
roughly correct as soon as ffmpeg has been running for a couple seconds
(useful for tail-following / interactive observation), (3) corrects it
authoritatively when ffmpeg finally exits (useful for downstream
playback alignment).

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
   - ``ran_sec >= success_min_seconds`` (default 15 s) AND the segment actually
     produced frames (``had_any_frames``) → **completed segment**, keep it. A
     source drop is NOT "recording complete": as long as ``stop()`` hasn't
     fired the flight is still in progress, so finalize this segment in place
     (join its reader, ffprobe its anchor) and **reconnect** — the next ffmpeg
     run becomes the next segment. This is record-side segment stitching: a
     mid-flight RTMP ``Error during demuxing: Input/output error`` (DJI Dock 3
     does this routinely on a congested link) no longer ends video capture for
     the rest of the flight. Regression: ``test_reconnects_after_long_run_self_exit``.
   - ``ran_sec >= success_min_seconds`` but **no frames** → treat as a failed
     start, not a completed (empty) segment. With ``-reconnect`` on an http
     source, an *unreachable / not-yet-publishing* endpoint makes ffmpeg retry
     the open and burn past the 15 s threshold before exiting with
     ``Error opening input: Connection timed out`` — that's not a recording, so
     delete the partial, drop the segment, and retry like any failed start
     (otherwise the reconnect loop accumulates phantom no-frame segments + junk
     files). Regression: ``test_no_frame_self_exit_is_failed_start_not_phantom_segment``.
     (The *stop()-interrupted* no-frame shell is a separate, terminal case — it
     is kept on disk for diagnostics; see ``test_empty_shell_mp4_skipped_in_manifest``.)
   - ``ran_sec < success_min_seconds`` → failed start (no publisher yet /
     transient glitch). Delete the partial mp4 (under whichever name it
     currently has — placeholder or renamed), pop the segment entry, sleep
     ``retry_interval_s``, retry.

5. **stderr is appended, not overwritten.** ``main.ffmpeg.log`` is opened in
   ``"a"`` mode each attempt with a ``=== ffmpeg attempt at wall_ms=… ===``
   separator so failure messages across attempts are all preserved.

Per-segment finalize: each segment is finalized with its OWN exit wall time —
the reconnect path in ``_supervise`` finalizes a dropped segment *before*
launching the next one, and ``stop()`` finalizes only the segment it itself
interrupts (re-running ffprobe on an already-finalized segment with a later
``now()`` would push its first-frame anchor wrong). ``manifest_video_block``
emits every ``had_any_frames`` segment with per-segment offsets; the top-level
``file`` / ``started_at_recv_ms`` stay pointed at the first segment for
backward compatibility.

Not in scope:
- Multi-segment **playback**. The recorder now captures and catalogues every
  segment, but the player (``player/video_pusher.py``) still pushes only the
  first segment (top-level ``manifest.video.file``). Replaying segments 2..N at
  their offsets is a follow-up.
- Probe budget: see ``_build_cmd`` / ``probesize`` / ``analyzeduration`` —
  DJI's late/sparse H.264 sequence header needs a larger probe than ffmpeg's
  default or the mp4 muxer fails with "dimensions not set".
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
        probesize: str = "20M",
        analyzeduration: str = "10M",
    ):
        self.source_url = source_url
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.extra_args = list(extra_args or [])
        self._retry_interval_s = retry_interval_s
        self._success_min_seconds = success_min_seconds
        # 输入探测预算。DJI Dock 的 H.264-over-RTMP 序列头(SPS/PPS)来得晚 /
        # 稀疏，ffmpeg 默认 probesize(5MB)/analyzeduration 内常常拿不到画面
        # 尺寸 → mp4 muxer "dimensions not set / Could not write header" → 录成
        # 空壳。实测把这两个调大(20M/10M)后 ffmpeg 才解析进真正的码流。
        # 必须作为 **输入** 选项放在 -i 之前才生效(见 _build_cmd)。
        # 空字符串 = 不显式传该 flag(用 ffmpeg 默认)。
        self._probesize = probesize
        self._analyzeduration = analyzeduration

        self._proc: Optional[subprocess.Popen] = None
        self._stderr_log = None
        self._segments: list[dict] = []
        self._filename: Optional[str] = None   # whichever name the current attempt's file has on disk

        self._stop_event = threading.Event()
        self._supervisor_thread: Optional[threading.Thread] = None
        # 每次 attempt 起一个 progress reader 线程；stop() 必须 join 完才能
        # 写 timing.json，否则会跟"reader 还在 rename"撞 race。
        self._progress_threads: list[threading.Thread] = []
        # 当前 attempt 的 progress reader。重连前必须 join 它，确保它的 rename /
        # had_any_frames patch 落在正确的（当时的 tail）segment 上，之后才 append
        # 新 segment——否则 _segments[-1] 会指向新段，patch 错位。
        self._current_progress_thread: Optional[threading.Thread] = None
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
        """Stop supervisor + finalize ffmpeg + drain progress readers +
        run ffprobe-based finalize (authoritative anchor override) +
        write timing.json + close log.

        Order matters:
          1. Stop supervisor + signal ffmpeg to exit cleanly.
          2. Capture ``exit_wall_ms`` AS SOON AS ffmpeg exited — this is the
             "now" used by the ffprobe finalize formula
             ``first_frame_wall_ms = exit_wall_ms - file_duration_ms``.
          3. Join progress reader threads so any in-band rename completes
             before ffprobe reads the file (otherwise the path read by
             ffprobe might lag the most recent rename).
          4. ``_finalize_segment_via_ffprobe`` overrides the in-band
             anchor with a ffprobe-derived one when possible (more
             authoritative — bypasses ffmpeg ``-progress``'s unreliable
             ``out_time_us`` field entirely).
          5. ``_write_timing`` snapshots ``_segments`` into ``main.timing.json``.

        Progress readers must finish (= apply their final rename + patch to
        ``_segments[-1]``) **before** ``_write_timing``. Otherwise we'd
        record the placeholder ``main_<popen_ms>.mp4`` filename even when the
        rename was about to happen. ffprobe finalize must run AFTER the
        progress readers' join so it sees the latest filename and can
        re-rename if it has a better anchor.
        """
        self._stop_event.set()
        if self._supervisor_thread is not None:
            self._supervisor_thread.join(timeout=timeout_s)

        if self._proc is not None and self._proc.poll() is None:
            # stop() 是结束这一段的人：SIGINT 让 ffmpeg 写完 mp4 trailer。
            self._proc.send_signal(signal.SIGINT)
            try:
                self._proc.wait(timeout=timeout_s)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                # SIGKILL 后再等一段时间；ffmpeg 卡在内核 D 状态时
                # 裸 wait() 会无限阻塞。timeout=timeout_s 保上限。
                # Regression: test_stop_does_not_block_forever_when_ffmpeg_hangs.
                try:
                    self._proc.wait(timeout=timeout_s)
                except subprocess.TimeoutExpired:
                    logger.error(
                        "ffmpeg SIGKILL 后 {}s 仍未退出 (pid={})；放弃等待，"
                        "可能是内核 D 状态。后续 reaper 处理僵尸。",
                        timeout_s, self._proc.pid,
                    )
            # ffmpeg 已退出（clean 或 killed）。用退出时刻 finalize 这一段
            # （join reader + 关 log + ffprobe 权威锚点）。
            exit_wall_ms = int(time.time() * 1000)
            self._finalize_current_segment(exit_wall_ms)
        else:
            # ffmpeg 在 stop() 之前就自己退了（源端断开的那一段）：它已在
            # _supervise 重连循环里用 *它自己的* exit_wall finalize 过了；这里再用
            # now() 重算会把首帧锚点算晚。所以只做防御性收尾。
            for t in self._progress_threads:
                t.join(timeout=timeout_s)
            if self._stderr_log is not None:
                try:
                    self._stderr_log.close()
                except Exception:
                    pass
                self._stderr_log = None

        self._write_timing()

    def _finalize_current_segment(self, exit_wall_ms: int) -> None:
        """Finalize the just-exited (tail) segment with its OWN exit wall time.

        Joins this attempt's progress reader so any in-band rename /
        ``had_any_frames`` patch lands, closes its stderr log, then runs the
        authoritative ffprobe anchor override (``first_frame_wall =
        exit_wall - duration``).

        Called once per segment, each with that segment's exit wall:
          - the reconnect path in ``_supervise`` (a ≥ ``success_min`` run the
            source dropped), *before* the next ffmpeg launches; and
          - ``stop()`` for the segment stop() itself interrupts.

        Joining the reader before the next launch is what keeps multi-segment
        metadata from cross-contaminating: the reader patches ``_segments[-1]``,
        so it must finish while the tail is still its own segment.
        """
        self._join_current_progress_reader()
        self._close_stderr_log()
        # Authoritative override: ffprobe the on-disk mp4 for its real duration,
        # immune to ``-progress out_time_us`` unreliability (2026-06-01 bug).
        self._finalize_segment_via_ffprobe(exit_wall_ms)

    def _join_current_progress_reader(self) -> None:
        """Join this attempt's progress reader so its in-band rename /
        ``had_any_frames`` patch lands on the (still-tail) segment before we
        decide what to do with it. Must run before the next ffmpeg launches."""
        t = self._current_progress_thread
        if t is not None:
            t.join(timeout=10.0)
            self._current_progress_thread = None

    def _close_stderr_log(self) -> None:
        """Close the current attempt's ffmpeg stderr log handle (idempotent)."""
        if self._stderr_log is not None:
            try:
                self._stderr_log.close()
            except Exception:
                pass
            self._stderr_log = None

    def manifest_video_block(self, duration_ms: int) -> dict:
        """Return ``manifest.video`` block covering ALL recorded segments.

        断点续录后一次飞行可能有多段：每次源端断开 + 重连产生一段。本方法把所有
        "真录到帧"的段(had_any_frames=True)按时间顺序列进 ``segments[]``，每段带：

          - ``file``                  相对 flight_dir 的路径（video/main_<ms>.mp4）
          - ``start_ms`` / ``end_ms`` 相对**第一段首帧**的视频时间轴偏移
          - ``started_at_recv_ms``    该段首帧的绝对墙钟（回放对齐锚点）
          - ``popen_at_recv_ms``      该段"开始拉流"的绝对墙钟
          - ``duration_ms``           ffprobe 测得的本段时长（None=未知）

        顶层 ``file`` / ``started_at_recv_ms`` / ``popen_at_recv_ms`` 仍指向
        **第一段**，保持对旧版单段播放器的向后兼容（当前回放只推第一段；多段
        回放是后续工作）。

        空壳段(had_any_frames=False，握手成功但 demuxing 立即 I/O error → ~800B
        只有 moov header 的 mp4)被过滤掉。没有任何真段 → file=None / segments=[]。
        老段（升级前 timing.json 缺 had_any_frames 字段）按"出过帧"放行。

        从段列表读 filename（非 ``self._filename``），即使 rename 发生在别的线程，
        block 也跟磁盘一致。
        """
        empty_block = {
            "file": None,
            "source_url": self.source_url,
            "started_at_recv_ms": None,
            "popen_at_recv_ms": None,
            "duration_ms": duration_ms,
            "segments": [],
        }
        with self._state_lock:
            # had_any_frames 缺失 = 老段，按"出过帧"放行（向后兼容）。
            real = [s for s in self._segments if s.get("had_any_frames", True)]
            if not real:
                if self._segments:
                    logger.warning(
                        "manifest.video skipped: {} segment(s) recorded but none "
                        "had detected frames (all empty shells). Partial mp4(s) "
                        "kept on disk for diagnostics.",
                        len(self._segments),
                    )
                return empty_block

            first_started = real[0]["ffmpeg_start_wall_ms"]
            seg_entries: list[dict] = []
            for i, seg in enumerate(real):
                started = seg["ffmpeg_start_wall_ms"]
                popen = seg.get("ffmpeg_popen_wall_ms", started)
                start_ms = started - first_started
                seg_dur = seg.get("duration_ms")
                if seg_dur is not None:
                    end_ms = start_ms + seg_dur
                elif i + 1 < len(real):
                    # 本段时长未知（ffprobe 失败）→ 填到下一段起点。
                    end_ms = real[i + 1]["ffmpeg_start_wall_ms"] - first_started
                else:
                    # 最后一段时长未知 → 退回飞行时长（单段场景向后兼容：
                    # end_ms == duration_ms，跟旧 {start_ms:0,end_ms:duration} 一致）。
                    end_ms = max(start_ms, duration_ms)
                seg_entries.append({
                    "file": f"video/{seg['file']}",
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                    "started_at_recv_ms": started,
                    "popen_at_recv_ms": popen,
                    "duration_ms": seg_dur,
                })

            top_started = first_started
            top_popen = real[0].get("ffmpeg_popen_wall_ms", first_started)
            top_file = seg_entries[0]["file"]
        return {
            "file": top_file,
            "source_url": self.source_url,
            "started_at_recv_ms": top_started,
            # popen_at_recv_ms：第一段 ffmpeg Popen 墙钟（"开始拉流"），下游视频
            # 对齐的备选早锚点。
            "popen_at_recv_ms": top_popen,
            "duration_ms": duration_ms,
            "segments": seg_entries,
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
            try:
                self._launch_ffmpeg()
            except FileNotFoundError as e:
                # ffmpeg 不在 PATH。retry 救不了，明确告警后退出循环 —
                # 比让 daemon 线程静默死亡好太多。
                # Regression: test_supervisor_logs_warning_when_ffmpeg_missing.
                logger.error(
                    "video supervisor: ffmpeg executable not found ({}); "
                    "no video will be recorded. Install ffmpeg and restart.",
                    e,
                )
                return
            except Exception:
                logger.exception(
                    "video supervisor: unexpected error launching ffmpeg; "
                    "stopping video recording for this flight"
                )
                return

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
                # ffmpeg 自己退了且跑够了阈值。先 join 这次 attempt 的 progress
                # reader，让 had_any_frames / rename patch 落定，再判类型。
                self._join_current_progress_reader()
                with self._state_lock:
                    had_frames = (
                        bool(self._segments)
                        and self._segments[-1].get("had_any_frames", False)
                    )
                if had_frames:
                    # 真录到帧的完整段。关键语义：**源端断开 ≠ 整个飞行结束**。
                    # 只要 stop() 没触发，飞行仍在进行，必须重连续录把后续画面录成
                    # *新的* segment（断点续录 / segment stitching）。先就地 finalize
                    # 本段：关 stderr log、用本段自己的 exit_wall_ms 跑 ffprobe 拿权威
                    # 锚点（在 append 下一段之前完成，确保各段 metadata 不串）。
                    exit_wall_ms = int(time.time() * 1000)
                    self._close_stderr_log()
                    self._finalize_segment_via_ffprobe(exit_wall_ms)
                    logger.info(
                        "ffmpeg exited after {:.1f}s (≥ {}s) — 段已保存；源端断开但"
                        "飞行未结束，{:.1f}s 后重连续录下一段",
                        ran_sec, self._success_min_seconds, self._retry_interval_s,
                    )
                else:
                    # 跑够阈值但一帧都没出。两种典型：①源端不可达 / 没在发布，
                    # `-reconnect` 把"打不开输入"拖过了阈值（Connection timed out）；
                    # ②握手成功但立即 demuxing I/O error。无论哪种都**不是完整段**，
                    # 按启动失败处理：删掉空文件、不留 segment、据实记日志，再重连。
                    # （诊断追踪在 main.ffmpeg.log 里已保留；不再攒幽灵段 / 垃圾文件。）
                    # 注意：用户 stop() 中断的无帧段仍由 stop() 保留给诊断，不走这里。
                    logger.warning(
                        "ffmpeg exited after {:.1f}s (≥ {}s) but produced no frames — "
                        "source likely unreachable / not publishing, or connected then "
                        "stalled (see main.ffmpeg.log). Treating as failed start; "
                        "retry in {:.1f}s",
                        ran_sec, self._success_min_seconds, self._retry_interval_s,
                    )
                    self._cleanup_failed_attempt()
                if self._stop_event.is_set():
                    break
                self._stop_event.wait(timeout=self._retry_interval_s)
                continue

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
                # 第一帧后会被 patch 成"第一帧到达"的墙钟。即使 back-calc 被
                # sanity reject（值留 = popen），had_any_frames 仍为 True，下游
                # 据此判断"录到了，只是不知道精确锚点"vs"完全空壳"。
                "ffmpeg_start_wall_ms": launch_ms,
                # 真有视频数据到达的 marker。progress reader 看到任一非零
                # out_time_us 时置 True；ffprobe 算出 duration > 0 也置 True。
                # 空壳 mp4（RTMP 握手成 + demuxing I/O error 立即断）保持 False。
                "had_any_frames": False,
                "pts_offset_ms": 0,
            })
        logger.info("ffmpeg launched: video/{} (pid={})", placeholder_filename, self._proc.pid)

        progress_thread = threading.Thread(
            target=self._read_progress_and_rename,
            args=(self._proc, placeholder_filename, launch_ms),
            name="video-progress",
            daemon=True,
        )
        progress_thread.start()
        self._progress_threads.append(progress_thread)
        self._current_progress_thread = progress_thread

    def _read_progress_and_rename(
        self,
        proc: subprocess.Popen,
        placeholder_filename: str,
        popen_wall_ms: int,
    ) -> None:
        """Drain ffmpeg's ``-progress`` stdout. Track the FIRST and LATEST
        non-zero ``out_time_*`` observations (and their wall times), back-
        calculate first-frame wall ms via the PTS-delta form, sanity-check
        the result against ``[popen_wall_ms, first_observed_at_ms]``, then —
        only if the candidate is physically plausible — rename the output
        file. If sanity check fails, the placeholder popen-time filename
        stays put and we log enough diagnostic info to chase the root cause.

        Formula:

            first_frame_wall_ms = last_observed_at_ms - (last_us - first_us) // 1000

        Sanity bound:

            popen_wall_ms <= first_frame_wall_ms <= first_observed_at_ms

        Lower bound is physical: the first frame can't possibly be written
        before ffmpeg's ``Popen`` returns. Upper bound is observational: by
        the time we read the first ``out_time_us > 0`` line from the pipe,
        the first frame has already been muxed; so wall-of-first-frame can't
        be in the future relative to first_observed.

        Why the **delta** form and not the older ``now_ms - out_time_us``:
            ffmpeg ``-c copy`` from RTMP forwards the source's PTS directly
            into the output progress stream. Most live encoders (e.g. DJI
            Dock) emit PTS that is their **monotonic uptime**, not 0-
            relative. The old form ``now - out_time_us/1000`` assumed
            ``first_frame_pts == 0`` and therefore mis-attributed encoder
            uptime as "file duration", producing wall times days before
            ``popen_wall_ms`` (one such production log: 6.8 days early).

            ``(last_us - first_us)`` is the **observed** PTS delta;
            subtracting it from the latest wall observation gives the wall
            time when the first observed PTS frame was muxed. For a real-
            time monotonic-PTS source this equals first-frame wall to within
            ~100 ms.

        Why we **still need** the sanity check on top of the delta form:
            Some sources/ffmpeg-versions report ``out_time_us`` that is
            non-monotonic at wall-rate (e.g. 2026-06-01 production log:
            ``first_us=5293000`` then ``last_us=93270000000`` across only
            ~98s of wall time, a 25.9-hour jump). ``ffprobe`` on the same
            file showed actual frame PTS = 0..92.981s, so the muxer wrote
            sane PTS but ffmpeg's ``-progress`` field reported something
            else entirely (suspected: source DTS, multi-stream timebase
            clash, or a re-init discontinuity). The delta formula then
            produces a "first frame wall" 25.9 hours **before** popen.

            Rather than try to guess the root cause from outside, we just
            reject any back-calc that lands outside the physically possible
            window and degrade to popen-time naming — which is exactly the
            "保留开始拉流的时间戳" semantics the user originally asked for.
            Cost: anchor is RTMP-handshake-delay early (~1-5 s, consistent
            and player-correctable) instead of arbitrarily wrong.

        Why we still pick the **latest** wall observation in the formula:
            ffmpeg's stdout for ``-progress pipe:1`` goes through libc stdio
            block buffering (4KB) for non-TTY pipes. For short recordings
            (~15s, ~30KB) the buffer may not fill mid-stream; at exit, the
            whole batch flushes and our reader observes all blocks within
            milliseconds. Using ``last_observed_at_ms`` keeps the formula
            correct in both real-time-progress and buffered-flush cases —
            the PTS-delta captures the file duration; the latest wall
            captures the most up-to-date alignment with reality.

        Why we also accept ``out_time_ms=``:
            Older ffmpeg releases (pre-4.x) emit only ``out_time_ms=`` and
            ``out_time=hh:mm:ss.uuuuuu``; the ``out_time_us=`` line was added
            later. Parsing both keeps us version-portable.

        Why rename happens in ``finally`` (after pipe close), not inline:
            Guarantees we always use the **latest** observation, including
            the tail blocks flushed at ffmpeg exit. ``stop()`` joins this
            thread before writing ``main.timing.json``, so the renamed
            filename (or the popen-time fallback) lands in the timing
            record.
        """
        if proc.stdout is None:
            return
        line_count = 0
        out_time_hits = 0
        # first_us：首条 >0 的 out_time_us。在 -c copy 下我们 *希望* 它等于
        # 文件首帧 PTS，从而 (last_us - first_us) 是文件实际录制时长。
        # 不是所有源都满足这个假设——sanity check 在下面兜底。
        first_us: Optional[int] = None
        first_observed_at_ms: Optional[int] = None
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
                now_ms = int(time.time() * 1000)
                if first_us is None:
                    first_us = us
                    first_observed_at_ms = now_ms
                    # 第一次见到非零 out_time_us = ffmpeg 真出了帧。
                    # 即使后面 back-calc 被 sanity reject，had_any_frames
                    # 仍为 True，manifest 会保留 video 段（用 popen-time
                    # 作锚点 fallback）。
                    with self._state_lock:
                        if (self._segments
                                and self._segments[-1]["file"]
                                == placeholder_filename):
                            self._segments[-1]["had_any_frames"] = True
                last_us = us
                last_observed_at_ms = now_ms
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
            or first_observed_at_ms is None
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
        # PTS-delta back-calc 候选值。
        first_frame_wall_ms_candidate = (
            last_observed_at_ms - (last_us - first_us) // 1000
        )
        # Sanity check：候选值必须落在 [popen_wall_ms, first_observed_at_ms]。
        # 越界说明 out_time_us 非 wall-rate 单调（多 stream 时基冲突、源 PTS
        # discontinuity、或 ffmpeg 报的根本不是 output PTS），back-calc 不可信。
        if not (
            popen_wall_ms <= first_frame_wall_ms_candidate <= first_observed_at_ms
        ):
            logger.warning(
                "video back-calc out of plausible window — keeping popen-time "
                "filename. first_us={}, last_us={}, duration_implied={}ms, "
                "popen_wall_ms={}, first_observed_at_ms={}, "
                "last_observed_at_ms={}, candidate={}. "
                "Likely cause: source RTMP PTS non-monotonic at wall rate "
                "(timebase clash / discontinuity / ffmpeg version reporting "
                "source DTS instead of output PTS). Verify file is fine via "
                "`ffprobe -show_packets v:0`; if mp4 PTS is sane 0..N, the "
                "lying field is ffmpeg's -progress, not the data.",
                first_us, last_us, (last_us - first_us) // 1000,
                popen_wall_ms, first_observed_at_ms, last_observed_at_ms,
                first_frame_wall_ms_candidate,
            )
            return
        logger.info(
            "video progress: {} lines observed, {} usable; first_us={}, "
            "last_us={} at wall_ms={} → first_frame_wall_ms={} "
            "(duration={}ms, popen+{}ms)",
            line_count, out_time_hits,
            first_us, last_us, last_observed_at_ms,
            first_frame_wall_ms_candidate,
            (last_us - first_us) // 1000,
            first_frame_wall_ms_candidate - popen_wall_ms,
        )
        self._rename_to_first_frame_time(
            placeholder_filename, first_frame_wall_ms_candidate,
        )

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

    def _finalize_segment_via_ffprobe(self, exit_wall_ms: int) -> None:
        """Use ffprobe on the actual on-disk mp4 to derive first-frame wall
        ms authoritatively, then rename + patch the tail segment.

        Why this is more reliable than the in-band ``-progress`` back-calc:
        ffmpeg ``-c copy`` writes a mp4 whose container PTS the muxer
        rebases to start near 0; ``ffprobe -show_entries format=duration``
        reads that real duration. Combined with ``exit_wall_ms`` (the
        wall when ffmpeg stopped, captured in ``stop()``), we get
        ``first_frame_wall_ms = exit_wall_ms - duration_ms`` without ever
        touching ffmpeg's ``out_time_us`` field — which we have seen
        report values inconsistent with the actual mp4 PTS (e.g. 2026-06-01
        production log: ``out_time_us`` reported 25.9 hours across 98s of
        wall time, but ``ffprobe`` on the same file showed PTS 0..92.981s).

        Failure modes (any → return silently, keep in-band path's result):
          - ffprobe not in PATH (``FileNotFoundError``)
          - ffprobe non-zero exit (corrupt mp4, format unknown)
          - duration string unparseable
          - derived first_frame_wall_ms < popen_wall_ms (sanity check —
            implausible since the first frame can't predate ``Popen``)

        The in-band path's anchor (whether the back-calc result or the
        popen-time fallback) is kept on failure, so the segment ALWAYS has
        SOME anchor — best-effort upgrade, never a downgrade.
        """
        with self._state_lock:
            if not self._segments:
                return
            seg = self._segments[-1]
            current_filename = seg["file"]
            popen_wall_ms = seg["ffmpeg_popen_wall_ms"]

        file_path = self.output_dir / current_filename
        if not file_path.exists():
            # Cleanup may have unlinked the partial file; nothing to probe.
            return

        duration_ms = self._probe_duration_ms(file_path)
        if duration_ms is None:
            return  # ffprobe missing/failed; keep in-band path's result

        # 记录本段真实时长（ffprobe 读容器 PTS），供 manifest_video_block 算各段
        # 在飞行时间轴上的 end_ms。即使下面锚点 sanity-reject，时长仍然可靠。
        with self._state_lock:
            if (self._segments
                    and self._segments[-1]["file"] == current_filename):
                self._segments[-1]["duration_ms"] = duration_ms

        first_frame_wall_ms = exit_wall_ms - duration_ms
        if first_frame_wall_ms < popen_wall_ms:
            logger.warning(
                "ffprobe-derived first_frame_wall_ms={} is before "
                "popen_wall_ms={} (Δ={}ms, file_duration={}ms, "
                "exit_wall_ms={}); implausible — keeping in-band anchor. "
                "Likely cause: ffprobe read a corrupted/incomplete duration "
                "from the mp4 trailer.",
                first_frame_wall_ms, popen_wall_ms,
                popen_wall_ms - first_frame_wall_ms,
                duration_ms, exit_wall_ms,
            )
            return

        logger.info(
            "ffprobe-derived first_frame_wall_ms={} "
            "(exit_wall={} − duration={}ms, popen+{}ms); "
            "overriding any in-band anchor",
            first_frame_wall_ms, exit_wall_ms, duration_ms,
            first_frame_wall_ms - popen_wall_ms,
        )
        # ffprobe 算出 duration > 0 = mp4 里实际有视频帧。即使 in-band
        # progress reader 没拿到一条非零 out_time_us（典型场景 ffmpeg 太老 /
        # progress 走 stderr），ffprobe 这一路确认了 had_any_frames。
        if duration_ms > 0:
            with self._state_lock:
                if (self._segments
                        and self._segments[-1]["file"] == current_filename):
                    self._segments[-1]["had_any_frames"] = True
        self._rename_to_first_frame_time(current_filename, first_frame_wall_ms)

    def _probe_duration_ms(self, file_path: Path) -> Optional[int]:
        """Call ``ffprobe`` to get the container duration in milliseconds.

        Returns ``None`` (with a WARNING log) on any failure — ffprobe
        missing, non-zero exit, unparseable output, timeout. Caller is
        expected to fall back to the in-band anchor in that case.
        """
        try:
            result = subprocess.run(
                [
                    "ffprobe", "-v", "error",
                    "-show_entries", "format=duration",
                    "-of", "csv=p=0",
                    str(file_path),
                ],
                capture_output=True, text=True, timeout=10,
            )
        except FileNotFoundError:
            logger.warning(
                "ffprobe not in PATH; cannot derive accurate first-frame "
                "time — keeping in-band anchor (popen-time fallback if "
                "back-calc was also rejected). Install ffmpeg suite to "
                "enable authoritative first-frame anchor.",
            )
            return None
        except subprocess.TimeoutExpired:
            logger.warning(
                "ffprobe timed out on {} (>10s); keeping in-band anchor",
                file_path.name,
            )
            return None
        except Exception:
            # Belt-and-suspenders: subprocess.run goes through Popen
            # internally, and various test mocks / system quirks can break
            # the communicate() unpack. Any unhandled error → silent
            # degrade to in-band anchor, never crash stop().
            logger.exception(
                "ffprobe call raised unexpected exception; keeping in-band "
                "anchor for {}", file_path.name,
            )
            return None

        if result.returncode != 0:
            logger.warning(
                "ffprobe rc={} for {}; stderr: {}",
                result.returncode, file_path.name,
                result.stderr.strip()[:200],
            )
            return None

        try:
            duration_s = float(result.stdout.strip())
        except ValueError:
            logger.warning(
                "ffprobe duration unparseable for {}: {!r}",
                file_path.name, result.stdout.strip()[:60],
            )
            return None

        return int(duration_s * 1000)

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
        # Input-side probe budget. MUST precede -i (input options); appended
        # after -i they'd be treated as output options and silently ignored,
        # which is exactly why ``ffmpeg_extra_args`` (which lands after -i)
        # can't fix the "unspecified size" muxer failure.
        probe_args: list[str] = []
        if self._probesize:
            probe_args += ["-probesize", self._probesize]
        if self._analyzeduration:
            probe_args += ["-analyzeduration", self._analyzeduration]
        # HTTP(S)-only native reconnect. ffmpeg 对 http 协议支持断线自愈：单个
        # ffmpeg 进程在源端断流后自动重连、**续写同一个 mp4**（"存成一个文件"
        # 的方案）。RTMP **不支持** 这些 flag（加了无效/反而干扰），所以按协议
        # gate：只有 http:// https:// 源才加。
        #   -reconnect_on_network_error 1  接住 mid-stream I/O error（关键：DJI
        #                                  源那个 "Error during demuxing: I/O error"）
        #   -reconnect_at_eof 1            接住源端 EOF（推流端短暂停了）
        #   -reconnect_streamed 1          非可 seek 的流也重连
        #   -reconnect_delay_max 2         重试退避上限 2s
        # 必须作为 **输入** 选项放在 -i 之前。若 http 重连扛不住（实测确认），
        # supervisor 的重连+多段逻辑仍作兜底（那时会回到多文件）。
        reconnect_args: list[str] = []
        if self.source_url.lower().startswith(("http://", "https://")):
            reconnect_args = [
                "-reconnect", "1",
                "-reconnect_at_eof", "1",
                "-reconnect_streamed", "1",
                "-reconnect_on_network_error", "1",
                "-reconnect_delay_max", "2",
            ]
        return [
            "ffmpeg",
            "-y",
            # 5s socket I/O timeout: ffmpeg exits non-zero quickly when the
            # RTMP endpoint has no publisher yet, so the supervisor's restart
            # loop can keep probing without consuming a GOP per probe like
            # the old ffmpeg-probe did.
            "-rw_timeout", "5000000",
            *reconnect_args,
            *probe_args,
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
