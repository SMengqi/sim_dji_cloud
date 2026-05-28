"""Tests for the v3 VideoWriter: eager-launch + restart-on-fast-exit.

No probe phase any more. Behavior is driven entirely by what ``subprocess.Popen``
returns:
  - A proc whose ``poll()`` keeps returning ``None`` = ffmpeg "still running".
    The supervisor leaves it alone until ``stop()`` is called.
  - A proc whose ``poll()`` returns a non-``None`` exit code immediately =
    ffmpeg "died fast". Supervisor classifies it as a failed start (because
    ran_sec ≪ success_min_seconds) and retries after retry_interval_s.

The tests use tiny ``retry_interval_s`` / ``success_min_seconds`` so they finish
in milliseconds.
"""
from __future__ import annotations

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

def _running_proc() -> MagicMock:
    """Mock subprocess.Popen result that 'stays alive' (poll() returns None)."""
    p = MagicMock()
    p.poll.return_value = None
    return p


def _exited_proc(exit_code: int = 1) -> MagicMock:
    """Mock subprocess.Popen result that is already 'dead' (poll() returns code)."""
    p = MagicMock()
    p.poll.return_value = exit_code
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
    assert seg["pts_offset_ms"] == 0
