"""Tests for the v3.1 VideoWriter: eager-launch + restart-on-fast-exit +
first-frame rename.

Behavior is driven by what ``subprocess.Popen`` returns:
  - ``proc.poll()`` returning ``None`` = ffmpeg "still running"; supervisor
    waits until ``stop()``.
  - ``proc.poll()`` returning a non-``None`` exit code immediately = ffmpeg
    "died fast"; supervisor classifies as failed start and retries.
  - ``proc.stdout`` is an iterable of bytes-lines that simulates ffmpeg's
    ``-progress pipe:1`` output. To exercise the first-frame-rename path,
    yield a line like ``b"out_time_us=33333\\n"``.

Tiny ``retry_interval_s`` / ``success_min_seconds`` keep tests fast.
"""
from __future__ import annotations

import io
import json
import signal
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

from sim_dji_cloud.recorder.video_writer import VideoWriter


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _empty_stdout() -> io.BytesIO:
    """A closed-immediately stdout for procs that don't need to test
    -progress parsing. Iterating yields nothing, then EOF."""
    return io.BytesIO(b"")


def _progress_stdout(*lines: bytes) -> io.BytesIO:
    """A stdout containing ffmpeg-progress lines (each must end with \\n).
    Wrap in BytesIO so the iter-by-newline behavior matches a real pipe."""
    return io.BytesIO(b"".join(lines))


def _running_proc(stdout: io.BytesIO | None = None) -> MagicMock:
    """Mock subprocess.Popen result that 'stays alive' (poll() returns None)."""
    p = MagicMock()
    p.poll.return_value = None
    p.stdout = stdout if stdout is not None else _empty_stdout()
    return p


def _exited_proc(exit_code: int = 1, stdout: io.BytesIO | None = None) -> MagicMock:
    """Mock subprocess.Popen result that is already 'dead' (poll() returns code)."""
    p = MagicMock()
    p.poll.return_value = exit_code
    p.stdout = stdout if stdout is not None else _empty_stdout()
    return p


def _make_partial_file(cmd: list, content: bytes = b"") -> Path:
    """Write a fake partial mp4 at the path ffmpeg was told to use.

    Mimics what real ffmpeg does when it fails fast (it may create the output
    file with a tiny header or just 0 bytes before dying).
    """
    output_path = Path(cmd[-1])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(content)
    return output_path


# ---------------------------------------------------------------------------
# 1. Eager launch: ffmpeg starts immediately, no probe gate
# ---------------------------------------------------------------------------

def test_eager_launch_no_probe_gate(tmp_path: Path):
    video_dir = tmp_path / "video"
    launched_event = threading.Event()

    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen") as popen_mock:
        proc = _running_proc()

        def side_effect(*args, **kwargs):
            launched_event.set()
            return proc

        popen_mock.side_effect = side_effect

        vw = VideoWriter(
            source_url="rtmp://example/live/abc",
            output_dir=video_dir,
            retry_interval_s=0.001,
            success_min_seconds=0.05,
        )
        vw.start()
        assert launched_event.wait(timeout=2.0), (
            "ffmpeg should launch immediately, no probe phase"
        )
        vw.stop()

    # Popen should have been called at least once on supervisor entry.
    assert popen_mock.call_count >= 1


# ---------------------------------------------------------------------------
# 2. Long-running ffmpeg → manifest reflects the recording
# ---------------------------------------------------------------------------

def test_long_running_ffmpeg_is_in_manifest(tmp_path: Path):
    video_dir = tmp_path / "video"
    launched_event = threading.Event()
    fake_now = 1779937234.567
    expected_ms = int(fake_now * 1000)

    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen") as popen_mock, \
         patch("sim_dji_cloud.recorder.video_writer.time.time", return_value=fake_now):
        proc = _running_proc()

        def side_effect(*args, **kwargs):
            launched_event.set()
            return proc

        popen_mock.side_effect = side_effect

        vw = VideoWriter(
            source_url="rtmp://example/live/abc",
            output_dir=video_dir,
            retry_interval_s=0.001,
            success_min_seconds=0.05,
        )
        vw.start()
        launched_event.wait(timeout=2.0)
        vw.stop()

    block = vw.manifest_video_block(duration_ms=10000)
    expected_filename = f"main_{expected_ms}.mp4"

    assert block["file"] == f"video/{expected_filename}"
    assert block["source_url"] == "rtmp://example/live/abc"
    assert block["started_at_recv_ms"] == expected_ms
    assert block["duration_ms"] == 10000
    assert len(block["segments"]) == 1
    seg = block["segments"][0]
    assert seg["file"] == f"video/{expected_filename}"
    assert seg["start_ms"] == 0
    assert seg["end_ms"] == 10000


# ---------------------------------------------------------------------------
# 3. Fast-fail then long-run → only the success is in the manifest
# ---------------------------------------------------------------------------

def test_fast_fail_then_success_keeps_only_the_success(tmp_path: Path):
    video_dir = tmp_path / "video"
    fail_proc = _exited_proc(exit_code=1)
    run_proc = _running_proc()
    second_launched = threading.Event()

    call_count = {"n": 0}

    def side_effect(*args, **kwargs):
        call_count["n"] += 1
        cmd = args[0]
        _make_partial_file(cmd, b"\x00" * 100)
        if call_count["n"] == 1:
            return fail_proc
        second_launched.set()
        return run_proc

    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen",
               side_effect=side_effect):
        vw = VideoWriter(
            source_url="rtmp://example/live/abc",
            output_dir=video_dir,
            retry_interval_s=0.01,
            success_min_seconds=0.05,
        )
        vw.start()
        assert second_launched.wait(timeout=2.0), "should retry after first failure"
        # Let supervisor settle into the inner wait on proc #2.
        time.sleep(0.05)
        vw.stop()

    block = vw.manifest_video_block(duration_ms=10000)
    assert block["file"] is not None
    assert len(block["segments"]) == 1, "failed attempt must not contribute a segment"


# ---------------------------------------------------------------------------
# 4. Partial mp4 from a failed attempt gets deleted
# ---------------------------------------------------------------------------

def test_failed_attempt_partial_file_is_deleted(tmp_path: Path):
    video_dir = tmp_path / "video"
    created_files: list[Path] = []
    call_count = {"n": 0}

    def side_effect(*args, **kwargs):
        call_count["n"] += 1
        cmd = args[0]
        p = _make_partial_file(cmd, b"\x00" * 100)
        created_files.append(p)
        if call_count["n"] == 1:
            return _exited_proc(exit_code=1)
        return _running_proc()

    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen",
               side_effect=side_effect):
        vw = VideoWriter(
            source_url="rtmp://example/live/abc",
            output_dir=video_dir,
            retry_interval_s=0.01,
            success_min_seconds=0.05,
        )
        vw.start()
        # Wait long enough for at least two Popen calls.
        for _ in range(200):
            if len(created_files) >= 2:
                break
            time.sleep(0.005)
        time.sleep(0.05)
        vw.stop()

    assert len(created_files) >= 2, "need at least one fail + one success attempt"
    assert not created_files[0].exists(), (
        f"failed attempt mp4 should be deleted: {created_files[0]}"
    )
    assert created_files[-1].exists(), (
        f"successful attempt mp4 should remain: {created_files[-1]}"
    )


# ---------------------------------------------------------------------------
# 5. Never any successful run → empty manifest block
# ---------------------------------------------------------------------------

def test_only_failures_yields_empty_manifest_block(tmp_path: Path):
    video_dir = tmp_path / "video"

    def side_effect(*args, **kwargs):
        cmd = args[0]
        _make_partial_file(cmd, b"\x00" * 50)
        return _exited_proc(exit_code=1)

    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen",
               side_effect=side_effect):
        vw = VideoWriter(
            source_url="rtmp://example/live/abc",
            output_dir=video_dir,
            retry_interval_s=0.001,
            # High threshold: no real run can ever exceed it before we stop().
            success_min_seconds=10.0,
        )
        vw.start()
        # Let the supervisor cycle through a few quick failures.
        time.sleep(0.05)
        vw.stop()

    block = vw.manifest_video_block(duration_ms=5000)
    assert block["file"] is None
    assert block["started_at_recv_ms"] is None
    assert block["segments"] == []


# ---------------------------------------------------------------------------
# 6. timing.json is always written, even with no successful run
# ---------------------------------------------------------------------------

def test_timing_json_written_even_when_no_success(tmp_path: Path):
    video_dir = tmp_path / "video"

    def side_effect(*args, **kwargs):
        cmd = args[0]
        _make_partial_file(cmd, b"")
        return _exited_proc(exit_code=1)

    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen",
               side_effect=side_effect):
        vw = VideoWriter(
            source_url="rtmp://example/live/abc",
            output_dir=video_dir,
            retry_interval_s=0.001,
            success_min_seconds=10.0,
        )
        vw.start()
        time.sleep(0.05)
        vw.stop()

    timing = json.loads((video_dir / "main.timing.json").read_text())
    assert timing["segments"] == []


# ---------------------------------------------------------------------------
# 7. ffmpeg cmd includes -rw_timeout before -i (so ffmpeg self-bounds no-stream)
# ---------------------------------------------------------------------------

def test_rw_timeout_in_cmd_before_input(tmp_path: Path):
    video_dir = tmp_path / "video"
    launched_event = threading.Event()

    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen") as popen_mock:
        proc = _running_proc()

        def side_effect(*args, **kwargs):
            launched_event.set()
            return proc

        popen_mock.side_effect = side_effect

        vw = VideoWriter(
            source_url="rtmp://example/live/abc",
            output_dir=video_dir,
            retry_interval_s=0.001,
            success_min_seconds=0.05,
        )
        vw.start()
        launched_event.wait(timeout=2.0)
        vw.stop()

    args, _ = popen_mock.call_args
    cmd = args[0]
    assert "-rw_timeout" in cmd, "ffmpeg cmd must include -rw_timeout"
    assert cmd.index("-rw_timeout") < cmd.index("-i"), (
        "-rw_timeout must appear before -i (otherwise it's an output option)"
    )


# ---------------------------------------------------------------------------
# 8. main.ffmpeg.log: append mode, separator per attempt, earlier attempts preserved
# ---------------------------------------------------------------------------

def test_ffmpeg_log_appends_with_separator_per_attempt(tmp_path: Path):
    video_dir = tmp_path / "video"

    def side_effect(*args, **kwargs):
        cmd = args[0]
        _make_partial_file(cmd, b"")
        # ffmpeg "stderr" — write through the file handle the supervisor opened.
        stderr_file = kwargs.get("stderr")
        if stderr_file is not None and hasattr(stderr_file, "write"):
            stderr_file.write("simulated ffmpeg stderr\n")
            stderr_file.flush()
        return _exited_proc(exit_code=1)

    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen",
               side_effect=side_effect):
        vw = VideoWriter(
            source_url="rtmp://example/live/abc",
            output_dir=video_dir,
            retry_interval_s=0.005,
            success_min_seconds=10.0,
        )
        vw.start()
        time.sleep(0.06)   # ~12 retries at 5ms interval — plenty
        vw.stop()

    log_path = video_dir / "main.ffmpeg.log"
    assert log_path.exists(), "main.ffmpeg.log must be created"
    content = log_path.read_text()
    separator_count = content.count("=== ffmpeg attempt at wall_ms=")
    assert separator_count >= 2, (
        f"main.ffmpeg.log should accumulate separators across attempts; "
        f"got {separator_count}:\n{content[:500]}"
    )
    assert "simulated ffmpeg stderr" in content, (
        "earlier attempts' stderr must be preserved (append mode)"
    )


# ---------------------------------------------------------------------------
# 9. stop() signals a running ffmpeg with SIGINT
# ---------------------------------------------------------------------------

def test_stop_signals_running_ffmpeg(tmp_path: Path):
    video_dir = tmp_path / "video"
    launched_event = threading.Event()

    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen") as popen_mock:
        proc = _running_proc()

        def side_effect(*args, **kwargs):
            launched_event.set()
            return proc

        popen_mock.side_effect = side_effect

        vw = VideoWriter(
            source_url="rtmp://example/live/abc",
            output_dir=video_dir,
            retry_interval_s=0.001,
            success_min_seconds=10.0,
        )
        vw.start()
        launched_event.wait(timeout=2.0)
        vw.stop()

    proc.send_signal.assert_called_once_with(signal.SIGINT)


# ---------------------------------------------------------------------------
# 10. is_alive() reflects subprocess state
# ---------------------------------------------------------------------------

def test_is_alive_reflects_subprocess_state(tmp_path: Path):
    video_dir = tmp_path / "video"
    launched_event = threading.Event()

    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen") as popen_mock:
        proc = _running_proc()

        def side_effect(*args, **kwargs):
            launched_event.set()
            return proc

        popen_mock.side_effect = side_effect

        vw = VideoWriter(
            source_url="rtmp://example/live/abc",
            output_dir=video_dir,
            retry_interval_s=0.001,
            success_min_seconds=10.0,
        )
        assert not vw.is_alive()
        vw.start()
        launched_event.wait(timeout=2.0)
        assert vw.is_alive()

        proc.poll.return_value = 0   # ffmpeg "exited"
        assert not vw.is_alive()

        vw.stop()


# ---------------------------------------------------------------------------
# 11. -c copy and -map 0:v / -map 0:a? still in cmd; extra_args appended
# ---------------------------------------------------------------------------

def test_build_cmd_maps_video_and_optional_audio_and_passes_extra_args(tmp_path: Path):
    video_dir = tmp_path / "video"
    launched_event = threading.Event()

    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen") as popen_mock:
        proc = _running_proc()

        def side_effect(*args, **kwargs):
            launched_event.set()
            return proc

        popen_mock.side_effect = side_effect

        vw = VideoWriter(
            source_url="rtmp://example/live/abc",
            output_dir=video_dir,
            extra_args=["-loglevel", "warning"],
            retry_interval_s=0.001,
            success_min_seconds=0.05,
        )
        vw.start()
        launched_event.wait(timeout=2.0)
        vw.stop()

    args, _ = popen_mock.call_args
    cmd = args[0]

    assert "rtmp://example/live/abc" in cmd
    assert "-c" in cmd and "copy" in cmd
    assert "-f" in cmd and "mp4" in cmd
    assert "-loglevel" in cmd and "warning" in cmd

    map_vals = [cmd[i + 1] for i, a in enumerate(cmd) if a == "-map"]
    assert "0:v" in map_vals, "must map video stream"
    assert "0:a?" in map_vals, "must map optional audio stream"
    assert cmd.index("-map") > cmd.index("-i"), "-map must come after -i"


# ---------------------------------------------------------------------------
# 12. Filename uses epoch ms of the *successful* launch
# ---------------------------------------------------------------------------

def test_filename_uses_epoch_ms(tmp_path: Path):
    video_dir = tmp_path / "video"
    launched_event = threading.Event()
    fake_now = 1779937234.567
    expected_ms = int(fake_now * 1000)

    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen") as popen_mock, \
         patch("sim_dji_cloud.recorder.video_writer.time.time", return_value=fake_now):
        proc = _running_proc()

        def side_effect(*args, **kwargs):
            launched_event.set()
            return proc

        popen_mock.side_effect = side_effect

        vw = VideoWriter(
            source_url="rtmp://example/live/abc",
            output_dir=video_dir,
            retry_interval_s=0.001,
            success_min_seconds=0.05,
        )
        vw.start()
        launched_event.wait(timeout=2.0)
        vw.stop()

    args, _ = popen_mock.call_args
    cmd = args[0]
    output_file = cmd[-1]
    assert output_file.endswith(f"main_{expected_ms}.mp4"), (
        f"expected filename ending with main_{expected_ms}.mp4, got: {output_file}"
    )


# ---------------------------------------------------------------------------
# 13. timing.json has correct segment on successful run
# ---------------------------------------------------------------------------

def test_timing_json_has_correct_segment_on_success(tmp_path: Path):
    video_dir = tmp_path / "video"
    launched_event = threading.Event()
    fake_now = 1779937234.567
    expected_ms = int(fake_now * 1000)

    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen") as popen_mock, \
         patch("sim_dji_cloud.recorder.video_writer.time.time", return_value=fake_now):
        proc = _running_proc()

        def side_effect(*args, **kwargs):
            launched_event.set()
            return proc

        popen_mock.side_effect = side_effect

        vw = VideoWriter(
            source_url="rtmp://example/live/abc",
            output_dir=video_dir,
            retry_interval_s=0.001,
            success_min_seconds=0.05,
        )
        vw.start()
        launched_event.wait(timeout=2.0)
        vw.stop()

    timing = json.loads((video_dir / "main.timing.json").read_text())
    assert len(timing["segments"]) == 1
    seg = timing["segments"][0]
    assert seg["file"] == f"main_{expected_ms}.mp4"
    assert seg["ffmpeg_start_wall_ms"] == expected_ms
    # Popen 时间戳：跟 start_wall_ms 在"没收到 first frame"路径下相等。
    assert seg["ffmpeg_popen_wall_ms"] == expected_ms
    assert seg["pts_offset_ms"] == 0


# ---------------------------------------------------------------------------
# 17. ffmpeg_popen_wall_ms 不会被 first-frame rename patch（保留"开始拉流"语义）
# ---------------------------------------------------------------------------

def test_ffmpeg_popen_wall_ms_unchanged_after_first_frame_rename(tmp_path: Path):
    """rename 后 ffmpeg_start_wall_ms 被 patch 成第一帧墙钟，
    但 ffmpeg_popen_wall_ms 必须保留为 Popen 那一刻的值（"开始拉流"语义）。
    下游回放对齐可以用两个里随便挑一个做时间锚点。
    """
    video_dir = tmp_path / "video"
    launched_event = threading.Event()

    # 同 test 15 的时间安排：launch_ms=1000000, first_frame_wall_ms=1000034
    # 新公式：单条观测 → first_us == last_us，delta=0，
    # first_frame_wall_ms = last_observed_at_ms = 1000034
    times = iter([1000.000, 1000.034])
    expected_launch_ms = 1000000
    expected_first_frame_ms = 1000034
    progress_lines = (b"out_time_us=33333\n", b"progress=continue\n")

    def side_effect(*args, **kwargs):
        cmd = args[0]
        out_path = Path(cmd[-1])
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(b"\x00" * 256)
        proc = _running_proc(stdout=_progress_stdout(*progress_lines))
        launched_event.set()
        return proc

    def fake_time():
        return next(times, 1000.034)

    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen",
               side_effect=side_effect), \
         patch("sim_dji_cloud.recorder.video_writer.time.time", side_effect=fake_time):
        vw = VideoWriter(
            source_url="rtmp://example/live/abc",
            output_dir=video_dir,
            retry_interval_s=0.001,
            success_min_seconds=10.0,
        )
        vw.start()
        assert launched_event.wait(timeout=2.0)
        # 等 rename 完成
        new_path = video_dir / f"main_{expected_first_frame_ms}.mp4"
        for _ in range(400):
            if new_path.exists():
                break
            time.sleep(0.005)
        vw.stop()

    timing = json.loads((video_dir / "main.timing.json").read_text())
    seg = timing["segments"][0]
    # 文件名被 rename 到第一帧
    assert seg["file"] == f"main_{expected_first_frame_ms}.mp4"
    # ffmpeg_start_wall_ms 是第一帧墙钟（被 progress reader patch 过）
    assert seg["ffmpeg_start_wall_ms"] == expected_first_frame_ms
    # ffmpeg_popen_wall_ms 是 Popen 那一刻，**不变**
    assert seg["ffmpeg_popen_wall_ms"] == expected_launch_ms

    # manifest block 同时暴露两个时间锚
    block = vw.manifest_video_block(duration_ms=10000)
    assert block["started_at_recv_ms"] == expected_first_frame_ms
    assert block["popen_at_recv_ms"] == expected_launch_ms


# ---------------------------------------------------------------------------
# 17.5. 缓冲场景：所有 progress 在 ffmpeg 退出时一次性 flush 出来，
#       使用"最后一条" out_time_us 才能得到正确的第一帧时间。
# ---------------------------------------------------------------------------

def test_buffered_progress_flush_uses_latest_observation(tmp_path: Path):
    """复现并固化生产 bug：ffmpeg stdout 走 libc block-buffered，所有 progress
    在退出那一刻一次性 flush。如果按"首条非零 out_time_us"做反算，
    `now - out_time_us` 会把 first frame 算到接近 exit 的时间（错），
    必须用"最后一条" out_time_us 才对。

    新公式 ``last_obs_wall - (last_us - first_us)/1000`` 在 buffered-flush
    场景下：last_obs_wall = exit_wall (所有行 flush 在 exit)，
    first_us / last_us 是文件首末帧 PTS，差值 = 文件录制时长 →
    first_frame_wall = exit_wall - duration = 正确。
    """
    video_dir = tmp_path / "video"
    launched_event = threading.Event()

    # 时间线模拟（秒）：
    #   T0 = 2000.000        ← Popen，对应 launch_ms = 2000000
    #   T0 + 0.100           ← 第一帧到达（RTMP handshake 之后）
    #   T0 + 15.500          ← ffmpeg 退出，libc 缓冲一次性 flush
    # 所有 progress 都在 flush 那一刻被读到 → time.time() 都返回 2015.500
    times = iter([2000.000, 2015.500, 2015.500, 2015.500, 2015.500])
    expected_launch_ms = 2000000
    # first_us=33333, last_us=15400000 → delta = 15366667us = 15366ms
    # first_frame_wall_ms = 2015500 - 15366 = 2000134 (≈ T0 + 134ms)
    expected_first_frame_ms = 2000134

    progress_lines = (
        b"out_time_us=33333\n",       # 33ms 流：first_us（文件首帧 PTS）
        b"out_time_us=1000000\n",     # 1s 流
        b"out_time_us=10000000\n",    # 10s 流
        b"out_time_us=15400000\n",    # 15.4s 流：last_us（文件末帧 PTS）
    )

    def side_effect(*args, **kwargs):
        cmd = args[0]
        out_path = Path(cmd[-1])
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(b"\x00" * 256)
        proc = _running_proc(stdout=_progress_stdout(*progress_lines))
        launched_event.set()
        return proc

    def fake_time():
        return next(times, 2015.500)

    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen",
               side_effect=side_effect), \
         patch("sim_dji_cloud.recorder.video_writer.time.time", side_effect=fake_time):
        vw = VideoWriter(
            source_url="rtmp://example/live/abc",
            output_dir=video_dir,
            retry_interval_s=0.001,
            success_min_seconds=10.0,
        )
        vw.start()
        assert launched_event.wait(timeout=2.0)
        vw.stop()  # ← stop() 必须 join progress thread 之后再写 timing.json

    timing = json.loads((video_dir / "main.timing.json").read_text())
    seg = timing["segments"][0]
    assert seg["file"] == f"main_{expected_first_frame_ms}.mp4", (
        f"buffered-flush 场景下应该用最后一条 out_time_us 反算，"
        f"期望 main_{expected_first_frame_ms}.mp4，得到 {seg['file']}"
    )
    assert seg["ffmpeg_start_wall_ms"] == expected_first_frame_ms
    assert seg["ffmpeg_popen_wall_ms"] == expected_launch_ms  # popen 不变


# ---------------------------------------------------------------------------
# 17.6. 老 ffmpeg 只输出 out_time_ms= 也要能解（不是只认 out_time_us=）
# ---------------------------------------------------------------------------

def test_progress_accepts_out_time_ms_fallback(tmp_path: Path):
    """ffmpeg 4 之前的版本可能只输出 out_time_ms= 不带 out_time_us=。
    progress reader 必须兼容这种格式，否则老 ffmpeg 上 first-frame rename
    永远失败、文件名永远停在 popen-time。"""
    video_dir = tmp_path / "video"
    launched_event = threading.Event()

    times = iter([1000.000, 1000.034])
    expected_launch_ms = 1000000
    # 新公式：单条观测 → delta=0，first_frame_wall_ms = last_observed_at_ms
    expected_first_frame_ms = 1000034

    # 只用 out_time_ms=（没有 out_time_us=）
    progress_lines = (
        b"frame=1\n",
        b"out_time_ms=33\n",          # 33 ms PTS
        b"progress=continue\n",
    )

    def side_effect(*args, **kwargs):
        cmd = args[0]
        out_path = Path(cmd[-1])
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(b"\x00" * 256)
        proc = _running_proc(stdout=_progress_stdout(*progress_lines))
        launched_event.set()
        return proc

    def fake_time():
        return next(times, 1000.034)

    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen",
               side_effect=side_effect), \
         patch("sim_dji_cloud.recorder.video_writer.time.time", side_effect=fake_time):
        vw = VideoWriter(
            source_url="rtmp://example/live/abc",
            output_dir=video_dir,
            retry_interval_s=0.001,
            success_min_seconds=10.0,
        )
        vw.start()
        assert launched_event.wait(timeout=2.0)
        vw.stop()

    timing = json.loads((video_dir / "main.timing.json").read_text())
    seg = timing["segments"][0]
    assert seg["file"] == f"main_{expected_first_frame_ms}.mp4"
    assert seg["ffmpeg_start_wall_ms"] == expected_first_frame_ms
    assert seg["ffmpeg_popen_wall_ms"] == expected_launch_ms


# ---------------------------------------------------------------------------
# 17.7. 完全没看到 out_time_*：文件名保留 popen-time，timing.json 里
#       ffmpeg_popen_wall_ms == ffmpeg_start_wall_ms（已经记的样子，
#       下游能识别"未拿到第一帧时间"这个状态）
# ---------------------------------------------------------------------------

def test_no_out_time_observation_leaves_popen_filename(tmp_path: Path):
    """progress 里没有任何 out_time_*（或全是 0）时，进入 fallback 路径：
    不 rename，文件名保持 popen-time；timing.json 里两个时间戳相等。
    """
    video_dir = tmp_path / "video"
    launched_event = threading.Event()

    fake_now = 5000.000
    expected_launch_ms = 5000000

    # 全是无关行（没有 out_time_us / out_time_ms）
    progress_lines = (
        b"frame=1\n",
        b"fps=0.00\n",
        b"progress=continue\n",
    )

    def side_effect(*args, **kwargs):
        cmd = args[0]
        out_path = Path(cmd[-1])
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(b"\x00" * 256)
        proc = _running_proc(stdout=_progress_stdout(*progress_lines))
        launched_event.set()
        return proc

    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen",
               side_effect=side_effect), \
         patch("sim_dji_cloud.recorder.video_writer.time.time", return_value=fake_now):
        vw = VideoWriter(
            source_url="rtmp://example/live/abc",
            output_dir=video_dir,
            retry_interval_s=0.001,
            success_min_seconds=10.0,
        )
        vw.start()
        assert launched_event.wait(timeout=2.0)
        vw.stop()

    timing = json.loads((video_dir / "main.timing.json").read_text())
    seg = timing["segments"][0]
    # 文件名保留 popen-time
    assert seg["file"] == f"main_{expected_launch_ms}.mp4"
    # 两个时间戳相等 = 下游能识别"没拿到第一帧时间"这个状态
    assert seg["ffmpeg_popen_wall_ms"] == expected_launch_ms
    assert seg["ffmpeg_start_wall_ms"] == expected_launch_ms


# ---------------------------------------------------------------------------
# 17.8. 生产 bug 复现：RTMP 源 PTS 不从 0 起（编码器累计运行时间），
#       `-c copy` 不重写 PTS，旧公式 `now - last_us/1000` 反算出来的
#       first_frame_wall_ms 比 popen 早几天。新公式用 PTS 增量。
# ---------------------------------------------------------------------------

def test_pts_not_starting_at_zero_uses_delta_against_first_observation(
    tmp_path: Path,
):
    """日志重现（2026-05-29 17:07 录制）：

        popen wall_ms          1780045635673   ≈ 17:07:15.673
        latest out_time_us     588009000000    ≈ 588009s ≈ 6.8 天
        latest observed wall   1780046231726   ≈ 17:17:11.726 (popen+596s)
        旧公式 first_frame_wall_ms = 1780046231726 - 588009000
                                  = 1779458222726  ← 5/22 00:37（错 6.8 天）

    源 RTMP PTS 来自编码器累计运行时间，ffmpeg ``-c copy`` 不重写 PTS，
    所以 ``out_time_us`` 是源 PTS 不是"输出已写时长"。修复：用首条 / 末条
    观测的 PTS **增量** 当输出已写时长。

    本测试构造一次 15.4s 录制：源 PTS 从 587413000000 us 起，到末尾
    587428400000 us（涨 15.4s）。两次观测都是实时拿到的（非缓冲 flush）：

        T_popen   = 3000.000s             → launch_ms       = 3000000
        T_first   = 3000.100s (popen+100ms) → first PTS=587413000000
        T_last    = 3015.500s (popen+15.4s) → last  PTS=587428400000

    新公式 first_frame_wall_ms = 3015500 - (587428400000 - 587413000000)/1000
                              = 3015500 - 15400 = 3000100 (= T_first 墙钟)

    旧公式会把 first_frame 算到 (3015500 - 587428400) = 大负数，文件名直接
    变成 ``main_-584412900.mp4`` 之类的乱码 → 这条断言能区分新旧实现。
    """
    video_dir = tmp_path / "video"
    launched_event = threading.Event()

    times = iter([3000.000, 3000.100, 3015.500])
    expected_launch_ms = 3000000
    expected_first_frame_ms = 3000100  # = T_first 墙钟

    progress_lines = (
        b"out_time_us=587413000000\n",   # 源 PTS：编码器已运行 587413s
        b"out_time_us=587428400000\n",   # 15.4s 后
    )

    def side_effect(*args, **kwargs):
        cmd = args[0]
        out_path = Path(cmd[-1])
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(b"\x00" * 256)
        proc = _running_proc(stdout=_progress_stdout(*progress_lines))
        launched_event.set()
        return proc

    def fake_time():
        return next(times, 3015.500)

    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen",
               side_effect=side_effect), \
         patch("sim_dji_cloud.recorder.video_writer.time.time", side_effect=fake_time):
        vw = VideoWriter(
            source_url="rtmp://example/live/abc",
            output_dir=video_dir,
            retry_interval_s=0.001,
            success_min_seconds=10.0,
        )
        vw.start()
        assert launched_event.wait(timeout=2.0)
        vw.stop()

    timing = json.loads((video_dir / "main.timing.json").read_text())
    seg = timing["segments"][0]
    assert seg["file"] == f"main_{expected_first_frame_ms}.mp4", (
        f"PTS-not-zero 场景应该用 (last_us - first_us) 增量倒推 first_frame_wall_ms，"
        f"期望 main_{expected_first_frame_ms}.mp4，得到 {seg['file']}"
    )
    assert seg["ffmpeg_start_wall_ms"] == expected_first_frame_ms
    # ffmpeg_popen_wall_ms 仍是 Popen 时刻（"开始拉流"语义不变）
    assert seg["ffmpeg_popen_wall_ms"] == expected_launch_ms


# ---------------------------------------------------------------------------
# 17.9. Sanity-check 兜底：source PTS 非线性 / 多 stream 时基冲突时，
#       back-calc 算出的 first_frame_wall_ms 跑到 [popen, first_observed]
#       之外，必须放弃 rename、保留 popen-time 文件名。
# ---------------------------------------------------------------------------

def test_implausible_back_calc_falls_back_to_popen(tmp_path: Path):
    """2026-06-01 09:58 production log repro：

        popen wall_ms       = 1780279102278   ≈ 09:58:22.278 CST
        first_us            = 5293000          (5.3 s)
        last_us             = 93270000000      (93270 s ≈ 25.9 hr)
        实际 wall 流逝     ≈ 98 s              (popen → last observed)
        ffprobe 显示 mp4 内部 PTS 是 0..92.981 s（正常）

    ffmpeg ``-c copy`` 模式下 ``-progress out_time_us`` 报的数跟落盘 mp4
    的 PTS 不一致——它报的是源 DTS / 多 stream 合成时间之类的东西，
    跨 98 s wall 居然能跳 25.9 hr。新公式
    ``last_obs - (last_us - first_us)/1000`` 得到的候选值落在 popen
    **之前** 25 小时（物理上不可能：第一帧不可能在 ffmpeg 启动前就写出）。

    Sanity check 把候选值约束在 ``[popen_wall_ms, first_observed_at_ms]``
    内，超出就放弃 rename、文件名 / ``ffmpeg_start_wall_ms`` 都维持 popen 值
    （= 用户最初的诉求"保留开始拉流的时间戳"）。

    本测试构造同构的极端数据：
        T_popen     = 5000.000 s       → launch_ms = 5_000_000
        T_first_obs = 5005.293 s       → first_observed_at_ms = 5_005_293
        T_last_obs  = 5098.000 s       → last_observed_at_ms  = 5_098_000
        first_us    = 5_293_000        (5.3 s 流, 同生产日志)
        last_us     = 93_270_000_000   (93270 s 流, 同生产日志)

    候选 = 5098000 - (93270000000 - 5293000)/1000 = 5098000 - 93264707
         = -88166707  ← 负数，肯定 < popen_wall_ms
    sanity 拒绝 → 文件名保持 main_5000000.mp4，两个 ts 相等。
    """
    video_dir = tmp_path / "video"
    launched_event = threading.Event()

    times = iter([5000.000, 5005.293, 5098.000])
    expected_launch_ms = 5_000_000

    progress_lines = (
        b"out_time_us=5293000\n",       # 5.3s
        b"out_time_us=93270000000\n",   # 25.9hr（同 06/01 production log）
    )

    def side_effect(*args, **kwargs):
        cmd = args[0]
        out_path = Path(cmd[-1])
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(b"\x00" * 256)
        proc = _running_proc(stdout=_progress_stdout(*progress_lines))
        launched_event.set()
        return proc

    def fake_time():
        return next(times, 5098.000)

    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen",
               side_effect=side_effect), \
         patch("sim_dji_cloud.recorder.video_writer.time.time", side_effect=fake_time):
        vw = VideoWriter(
            source_url="rtmp://example/live/abc",
            output_dir=video_dir,
            retry_interval_s=0.001,
            success_min_seconds=10.0,
        )
        vw.start()
        assert launched_event.wait(timeout=2.0)
        vw.stop()

    timing = json.loads((video_dir / "main.timing.json").read_text())
    seg = timing["segments"][0]
    # sanity 拒绝 → 文件名 / ffmpeg_start_wall_ms 都保持 popen-time
    assert seg["file"] == f"main_{expected_launch_ms}.mp4", (
        f"implausible back-calc 应该被 sanity check 拒绝、文件名保持 "
        f"main_{expected_launch_ms}.mp4，得到 {seg['file']}"
    )
    assert seg["ffmpeg_start_wall_ms"] == expected_launch_ms
    assert seg["ffmpeg_popen_wall_ms"] == expected_launch_ms

    # manifest block 暴露的两个 anchor 也都是 popen 值
    block = vw.manifest_video_block(duration_ms=10000)
    assert block["started_at_recv_ms"] == expected_launch_ms
    assert block["popen_at_recv_ms"] == expected_launch_ms


# ---------------------------------------------------------------------------
# 18. 空 manifest 也带 popen_at_recv_ms=None（前端 / player 读得一致）
# ---------------------------------------------------------------------------

def test_empty_manifest_has_popen_at_recv_ms_none(tmp_path: Path):
    """没有任何成功 ffmpeg 的飞行，manifest.video 里 popen_at_recv_ms=None，
    跟 started_at_recv_ms=None 平行，让消费者写一致逻辑。"""
    video_dir = tmp_path / "video"

    def side_effect(*args, **kwargs):
        cmd = args[0]
        _make_partial_file(cmd, b"")
        return _exited_proc(exit_code=1)

    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen",
               side_effect=side_effect):
        vw = VideoWriter(
            source_url="rtmp://example/live/abc",
            output_dir=video_dir,
            retry_interval_s=0.001,
            success_min_seconds=10.0,
        )
        vw.start()
        time.sleep(0.05)
        vw.stop()

    block = vw.manifest_video_block(duration_ms=5000)
    assert block["started_at_recv_ms"] is None
    assert block["popen_at_recv_ms"] is None


# ---------------------------------------------------------------------------
# 14. -progress pipe:1 + -stats_period 0.1 in cmd (drives first-frame detect)
# ---------------------------------------------------------------------------

def test_progress_pipe_and_stats_period_in_cmd(tmp_path: Path):
    video_dir = tmp_path / "video"
    launched_event = threading.Event()

    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen") as popen_mock:
        proc = _running_proc()

        def side_effect(*args, **kwargs):
            launched_event.set()
            return proc

        popen_mock.side_effect = side_effect

        vw = VideoWriter(
            source_url="rtmp://example/live/abc",
            output_dir=video_dir,
            retry_interval_s=0.001,
            success_min_seconds=0.05,
        )
        vw.start()
        launched_event.wait(timeout=2.0)
        vw.stop()

    args, _ = popen_mock.call_args
    cmd = args[0]
    # -progress must point at pipe:1; stats_period must be present so first
    # frame is emitted with low latency.
    assert "-progress" in cmd
    progress_val = cmd[cmd.index("-progress") + 1]
    assert progress_val == "pipe:1"
    assert "-stats_period" in cmd
    period_val = cmd[cmd.index("-stats_period") + 1]
    assert float(period_val) <= 0.5
    # And the Popen call has to grab stdout (otherwise the reader can't drain).
    kwargs = popen_mock.call_args.kwargs
    import subprocess as _sp
    assert kwargs.get("stdout") == _sp.PIPE


# ---------------------------------------------------------------------------
# 15. First-frame detected on -progress line → file renamed + manifest patched
# ---------------------------------------------------------------------------

def test_first_frame_detected_renames_file_and_patches_manifest(tmp_path: Path):
    """Mock proc.stdout to emit a real-looking ffmpeg-progress block.

    The supervisor's progress-reader thread should pick up the first non-zero
    out_time_us, back-calculate first_frame_wall_ms via the PTS-delta formula
    ``last_obs_wall - (last_us - first_us) // 1000``, then:
      (a) rename the placeholder mp4 on disk
      (b) patch self._segments[-1]["file"] and ["ffmpeg_start_wall_ms"]
    """
    video_dir = tmp_path / "video"
    launched_event = threading.Event()
    renamed_event = threading.Event()

    # time.time() returns 1000.000 at Popen, then 1000.034 when progress reader
    # observes out_time_us=33333.
    # 单条观测 → first_us == last_us，delta=0，
    # first_frame_wall_ms = last_observed_at_ms = 1000034.
    times = iter([1000.000, 1000.034])
    expected_launch_ms = 1000000
    expected_first_frame_ms = 1000034

    progress_lines = (
        b"frame=1\n",
        b"out_time_us=33333\n",
        b"progress=continue\n",
    )

    def side_effect(*args, **kwargs):
        # Create the placeholder file on disk so the rename has something to rename.
        cmd = args[0]
        out_path = Path(cmd[-1])
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(b"\x00" * 256)
        proc = _running_proc(stdout=_progress_stdout(*progress_lines))
        launched_event.set()
        return proc

    def fake_time():
        return next(times, 1000.034)

    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen",
               side_effect=side_effect), \
         patch("sim_dji_cloud.recorder.video_writer.time.time", side_effect=fake_time):
        vw = VideoWriter(
            source_url="rtmp://example/live/abc",
            output_dir=video_dir,
            retry_interval_s=0.001,
            success_min_seconds=10.0,   # high so we don't accidentally cleanup
        )
        vw.start()
        assert launched_event.wait(timeout=2.0)
        # Give the progress thread up to 2 s to consume stdout and rename.
        new_path = video_dir / f"main_{expected_first_frame_ms}.mp4"
        for _ in range(400):
            if new_path.exists():
                renamed_event.set()
                break
            time.sleep(0.005)
        vw.stop()

    # File on disk:
    placeholder_path = video_dir / f"main_{expected_launch_ms}.mp4"
    first_frame_path = video_dir / f"main_{expected_first_frame_ms}.mp4"
    assert renamed_event.is_set(), (
        f"file should have been renamed to {first_frame_path.name}; "
        f"saw: {[p.name for p in video_dir.iterdir()]}"
    )
    assert first_frame_path.exists()
    assert not placeholder_path.exists(), "placeholder must be gone after rename"

    # Manifest reflects first_frame_wall_ms, not launch_ms.
    block = vw.manifest_video_block(duration_ms=20000)
    assert block["file"] == f"video/main_{expected_first_frame_ms}.mp4"
    assert block["started_at_recv_ms"] == expected_first_frame_ms
    assert block["segments"][0]["file"] == f"video/main_{expected_first_frame_ms}.mp4"


# ---------------------------------------------------------------------------
# 16. Rename fails → file stays at placeholder name but manifest ts is patched
# ---------------------------------------------------------------------------

def test_rename_failure_leaves_placeholder_but_patches_manifest_ts(tmp_path: Path):
    """If pathlib.Path.rename raises a non-FileNotFoundError, the supervisor
    falls back to: keep the placeholder filename on disk, but still patch the
    in-memory segment's ffmpeg_start_wall_ms to the accurate first-frame value.
    This keeps the manifest correct even if the FS rename is broken.
    """
    video_dir = tmp_path / "video"
    launched_event = threading.Event()

    # time.time() returns 2000.000 at Popen, 2000.500 when progress reader sees
    # out_time_us=750000.
    # 单条观测 → first_us == last_us，delta=0，
    # ⇒ launch_ms = 2000000;  first_frame_wall_ms = last_observed_at_ms = 2000500
    times = iter([2000.000, 2000.500])
    expected_launch_ms = 2000000
    expected_first_frame_ms = 2000500

    progress_lines = (b"out_time_us=750000\n", b"progress=continue\n")

    def fake_rename(self, target):
        # Simulate an OSError from the FS that is NOT FileNotFoundError.
        raise PermissionError("simulated rename failure")

    def side_effect(*args, **kwargs):
        cmd = args[0]
        out_path = Path(cmd[-1])
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(b"\x00" * 256)
        proc = _running_proc(stdout=_progress_stdout(*progress_lines))
        launched_event.set()
        return proc

    def fake_time():
        return next(times, 2000.500)

    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen",
               side_effect=side_effect), \
         patch("sim_dji_cloud.recorder.video_writer.time.time", side_effect=fake_time), \
         patch.object(Path, "rename", fake_rename):
        vw = VideoWriter(
            source_url="rtmp://example/live/abc",
            output_dir=video_dir,
            retry_interval_s=0.001,
            success_min_seconds=10.0,
        )
        vw.start()
        assert launched_event.wait(timeout=2.0)
        # Wait for the progress thread to process the line — manifest patch
        # happens after rename fails, so we poll for the segment ts to flip.
        for _ in range(400):
            block = vw.manifest_video_block(duration_ms=1)
            if block["started_at_recv_ms"] == expected_first_frame_ms:
                break
            time.sleep(0.005)
        vw.stop()

    # File on disk: still at the placeholder name (rename failed).
    placeholder_path = video_dir / f"main_{expected_launch_ms}.mp4"
    assert placeholder_path.exists(), "rename failed, placeholder must remain"

    # Manifest ts patched even though the file name didn't change.
    block = vw.manifest_video_block(duration_ms=20000)
    assert block["started_at_recv_ms"] == expected_first_frame_ms, (
        f"manifest ts should be patched to first-frame ms even when rename fails"
    )
    # File field still references the placeholder (truthful about what's on disk).
    assert block["file"] == f"video/main_{expected_launch_ms}.mp4"
