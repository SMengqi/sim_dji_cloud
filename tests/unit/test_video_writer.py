"""Tests for the new VideoWriter: probe-then-launch with retry, deferred filename,
stderr log capture, and empty-video manifest block.

All tests inject a `probe_factory` so no real ffprobe / ffmpeg is ever called.
The supervisor thread runs, but `retry_interval_s=0.001` keeps it snappy.
Tests that need to confirm ffmpeg was launched use a threading.Event exposed via
a custom probe_factory + Popen mock.
"""
import json
import re
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

from sim_dji_cloud.recorder.video_writer import VideoWriter


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _always_succeed(url: str) -> bool:
    """Probe factory that immediately returns True (stream ready)."""
    return True


def _always_fail(url: str) -> bool:
    """Probe factory that always returns False (stream never ready)."""
    return False


def _fail_n_then_succeed(n: int):
    """Returns a probe factory that fails the first n times then succeeds."""
    counter = {"calls": 0}

    def probe(url: str) -> bool:
        counter["calls"] += 1
        if counter["calls"] <= n:
            return False
        return True

    return probe


# ---------------------------------------------------------------------------
# 1. Probe succeeds immediately → ffmpeg launched with epoch-ms filename
# ---------------------------------------------------------------------------

def test_probe_succeeds_immediately_launches_ffmpeg_with_ms_filename(tmp_path: Path):
    video_dir = tmp_path / "video"
    launched_event = threading.Event()
    fixed_ms = 1779937234567  # what time.time() * 1000 should produce

    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen") as popen_mock, \
         patch("sim_dji_cloud.recorder.video_writer.time.time", return_value=fixed_ms / 1000):
        proc = MagicMock()
        proc.poll.return_value = None

        def side_effect(*args, **kwargs):
            launched_event.set()
            return proc

        popen_mock.side_effect = side_effect

        vw = VideoWriter(
            source_url="rtmp://example/live/abc",
            output_dir=video_dir,
            probe_factory=_always_succeed,
            retry_interval_s=0.001,
        )
        vw.start()
        assert launched_event.wait(timeout=2.0), "ffmpeg should have launched within 2s"

        args, kwargs = popen_mock.call_args
        cmd = args[0]

        # Must contain the epoch-ms filename
        assert f"main_{fixed_ms}.mp4" in cmd[-1], f"Expected main_{fixed_ms}.mp4 in cmd, got: {cmd}"

        vw.stop()


# ---------------------------------------------------------------------------
# 2. Probe fails N times then succeeds → ffmpeg eventually launches
# ---------------------------------------------------------------------------

def test_probe_fails_then_succeeds_eventually_launches(tmp_path: Path):
    video_dir = tmp_path / "video"
    launched_event = threading.Event()

    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen") as popen_mock:
        proc = MagicMock()
        proc.poll.return_value = None

        def side_effect(*args, **kwargs):
            launched_event.set()
            return proc

        popen_mock.side_effect = side_effect

        vw = VideoWriter(
            source_url="rtmp://example/live/abc",
            output_dir=video_dir,
            probe_factory=_fail_n_then_succeed(3),
            retry_interval_s=0.001,
        )
        vw.start()
        assert launched_event.wait(timeout=2.0), "ffmpeg should have launched after 3 probe failures"
        vw.stop()


# ---------------------------------------------------------------------------
# 3. Probe never succeeds before stop() → empty manifest block
# ---------------------------------------------------------------------------

def test_probe_never_succeeds_before_stop_yields_empty_manifest_block(tmp_path: Path):
    video_dir = tmp_path / "video"

    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen") as popen_mock:
        vw = VideoWriter(
            source_url="rtmp://example/live/abc",
            output_dir=video_dir,
            probe_factory=_always_fail,
            retry_interval_s=0.001,
        )
        vw.start()
        # Let supervisor spin briefly without success
        time.sleep(0.05)
        vw.stop()

        # ffmpeg must NOT have been launched
        popen_mock.assert_not_called()

    # manifest block must reflect "no ffmpeg"
    block = vw.manifest_video_block(duration_ms=5000)
    assert block["file"] is None
    assert block["started_at_recv_ms"] is None
    assert block["segments"] == []


# ---------------------------------------------------------------------------
# 4. main.timing.json written with empty segments when ffmpeg never launched
# ---------------------------------------------------------------------------

def test_stop_writes_timing_json_even_when_no_ffmpeg(tmp_path: Path):
    video_dir = tmp_path / "video"

    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen"):
        vw = VideoWriter(
            source_url="rtmp://example/live/abc",
            output_dir=video_dir,
            probe_factory=_always_fail,
            retry_interval_s=0.001,
        )
        vw.start()
        time.sleep(0.05)
        vw.stop()

    timing_path = video_dir / "main.timing.json"
    assert timing_path.exists(), "main.timing.json must be written even when no ffmpeg launched"
    timing = json.loads(timing_path.read_text())
    assert timing["segments"] == []


# ---------------------------------------------------------------------------
# 5. stop() signals ffmpeg (SIGINT) and closes the stderr log file
# ---------------------------------------------------------------------------

def test_stop_signals_ffmpeg_and_closes_stderr_log(tmp_path: Path):
    video_dir = tmp_path / "video"
    launched_event = threading.Event()

    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen") as popen_mock:
        proc = MagicMock()
        proc.poll.return_value = None

        def side_effect(*args, **kwargs):
            launched_event.set()
            return proc

        popen_mock.side_effect = side_effect

        vw = VideoWriter(
            source_url="rtmp://example/live/abc",
            output_dir=video_dir,
            probe_factory=_always_succeed,
            retry_interval_s=0.001,
        )
        vw.start()
        launched_event.wait(timeout=2.0)
        vw.stop()

    import signal
    proc.send_signal.assert_called_once_with(signal.SIGINT)

    # stderr log file must exist (even if ffmpeg is mocked)
    log_path = video_dir / "main.ffmpeg.log"
    assert log_path.exists(), "main.ffmpeg.log must be created when ffmpeg is launched"


# ---------------------------------------------------------------------------
# 6. Filename uses epoch ms (mock time.time)
# ---------------------------------------------------------------------------

def test_filename_uses_epoch_ms(tmp_path: Path):
    video_dir = tmp_path / "video"
    launched_event = threading.Event()
    fake_now = 1779937234.567  # time.time() return value
    expected_ms = int(fake_now * 1000)  # 1779937234567

    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen") as popen_mock, \
         patch("sim_dji_cloud.recorder.video_writer.time.time", return_value=fake_now):
        proc = MagicMock()
        proc.poll.return_value = None

        def side_effect(*args, **kwargs):
            launched_event.set()
            return proc

        popen_mock.side_effect = side_effect

        vw = VideoWriter(
            source_url="rtmp://example/live/abc",
            output_dir=video_dir,
            probe_factory=_always_succeed,
            retry_interval_s=0.001,
        )
        vw.start()
        launched_event.wait(timeout=2.0)
        vw.stop()

    args, _ = popen_mock.call_args
    cmd = args[0]
    output_file = cmd[-1]
    assert output_file.endswith(f"main_{expected_ms}.mp4"), (
        f"Expected filename ending with main_{expected_ms}.mp4, got: {output_file}"
    )


# ---------------------------------------------------------------------------
# 7. _build_cmd maps video and optional audio (-map 0:v, -map 0:a?)
# ---------------------------------------------------------------------------

def test_build_cmd_maps_video_and_optional_audio(tmp_path: Path):
    video_dir = tmp_path / "video"
    launched_event = threading.Event()

    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen") as popen_mock:
        proc = MagicMock()
        proc.poll.return_value = None

        def side_effect(*args, **kwargs):
            launched_event.set()
            return proc

        popen_mock.side_effect = side_effect

        vw = VideoWriter(
            source_url="rtmp://example/live/abc",
            output_dir=video_dir,
            extra_args=["-loglevel", "warning"],
            probe_factory=_always_succeed,
            retry_interval_s=0.001,
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
    assert "0:v" in map_vals, "Must map video stream"
    assert "0:a?" in map_vals, "Must map optional audio stream"
    assert cmd.index("-map") > cmd.index("-i"), "-map must come after -i"


# ---------------------------------------------------------------------------
# 8. manifest_video_block with successful ffmpeg → correct structure
# ---------------------------------------------------------------------------

def test_manifest_video_block_with_successful_launch(tmp_path: Path):
    video_dir = tmp_path / "video"
    launched_event = threading.Event()
    fake_now = 1779937234.567
    expected_ms = int(fake_now * 1000)

    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen") as popen_mock, \
         patch("sim_dji_cloud.recorder.video_writer.time.time", return_value=fake_now):
        proc = MagicMock()
        proc.poll.return_value = None

        def side_effect(*args, **kwargs):
            launched_event.set()
            return proc

        popen_mock.side_effect = side_effect

        vw = VideoWriter(
            source_url="rtmp://example/live/abc",
            output_dir=video_dir,
            probe_factory=_always_succeed,
            retry_interval_s=0.001,
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
# 9. is_alive() reflects subprocess state
# ---------------------------------------------------------------------------

def test_is_alive_reflects_subprocess_state(tmp_path: Path):
    video_dir = tmp_path / "video"
    launched_event = threading.Event()

    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen") as popen_mock:
        proc = MagicMock()
        proc.poll.return_value = None  # running

        def side_effect(*args, **kwargs):
            launched_event.set()
            return proc

        popen_mock.side_effect = side_effect

        vw = VideoWriter(
            source_url="rtmp://example/live/abc",
            output_dir=video_dir,
            probe_factory=_always_succeed,
            retry_interval_s=0.001,
        )
        assert not vw.is_alive()  # not started yet
        vw.start()
        launched_event.wait(timeout=2.0)
        assert vw.is_alive()  # ffmpeg running

        proc.poll.return_value = 0  # exited
        assert not vw.is_alive()

        vw.stop()


# ---------------------------------------------------------------------------
# 10. timing.json written with correct segment on successful ffmpeg
# ---------------------------------------------------------------------------

def test_timing_json_has_correct_segment_on_success(tmp_path: Path):
    video_dir = tmp_path / "video"
    launched_event = threading.Event()
    fake_now = 1779937234.567
    expected_ms = int(fake_now * 1000)

    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen") as popen_mock, \
         patch("sim_dji_cloud.recorder.video_writer.time.time", return_value=fake_now):
        proc = MagicMock()
        proc.poll.return_value = None

        def side_effect(*args, **kwargs):
            launched_event.set()
            return proc

        popen_mock.side_effect = side_effect

        vw = VideoWriter(
            source_url="rtmp://example/live/abc",
            output_dir=video_dir,
            probe_factory=_always_succeed,
            retry_interval_s=0.001,
        )
        vw.start()
        launched_event.wait(timeout=2.0)
        vw.stop()

    timing = json.loads((video_dir / "main.timing.json").read_text())
    assert len(timing["segments"]) == 1
    seg = timing["segments"][0]
    expected_filename = f"main_{expected_ms}.mp4"
    assert seg["file"] == expected_filename
    assert seg["ffmpeg_start_wall_ms"] == expected_ms
    assert seg["pts_offset_ms"] == 0
