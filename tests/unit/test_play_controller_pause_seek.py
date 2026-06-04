from pathlib import Path
from unittest.mock import patch

import pytest

from sim_dji_cloud.dashboard.play_controller import (
    PlayController, NotRunning, ControlUnavailable,
)


def test_pause_returns_404_when_play_not_running(tmp_path):
    pc = PlayController(recordings_root=tmp_path, log_dir=tmp_path)
    with pytest.raises(NotRunning):
        pc.pause()
    with pytest.raises(NotRunning):
        pc.resume()
    with pytest.raises(NotRunning):
        pc.seek(1000)


def test_pause_raises_control_unavailable_when_sidecar_missing(tmp_path):
    pc = PlayController(recordings_root=tmp_path, log_dir=tmp_path)
    with patch.object(pc, "_status_basic", return_value={
        "state": "running", "pid": 1, "flight_dir": "x",
        "speed": 1.0, "started_at_ms": 0, "log_tail": None,
    }):
        with pytest.raises(ControlUnavailable):
            pc.pause()


def test_status_includes_progress_when_running(tmp_path):
    pc = PlayController(recordings_root=tmp_path, log_dir=tmp_path)
    pc._last_progress = {"virt_ms": 5000, "total_ms": 10000,
                         "paused": False, "speed": 1.0}
    pc._progress_stale = False
    with patch.object(pc, "_status_basic", return_value={
        "state": "running", "pid": 1, "flight_dir": "x",
        "speed": 1.0, "started_at_ms": 0, "log_tail": None,
    }):
        s = pc.status()
    assert s["progress"]["virt_ms"] == 5000
    assert s["progress"]["total_ms"] == 10000
    assert s["progress"]["paused"] is False
    assert s["progress"]["stale"] is False


def test_status_progress_null_when_stopped(tmp_path):
    pc = PlayController(recordings_root=tmp_path, log_dir=tmp_path)
    pc._last_progress = {"virt_ms": 5000, "total_ms": 10000,
                         "paused": False, "speed": 1.0}
    s = pc.status()
    assert s["state"] == "stopped"
    assert s["progress"] is None


def test_seek_rejects_negative_virt_ms(tmp_path):
    pc = PlayController(recordings_root=tmp_path, log_dir=tmp_path)
    with pytest.raises(ValueError):
        pc.seek(-1)
    with pytest.raises(ValueError):
        pc.seek("100")    # type: ignore[arg-type]


def test_seek_accepts_zero(tmp_path):
    """seek(0) should not raise ValueError (0 is valid: seek to beginning).
    Only negative virt_ms is rejected."""
    pc = PlayController(recordings_root=tmp_path, log_dir=tmp_path)
    # play not running → NotRunning (zero validation happens before _ensure_running
    # is called? Actually validation is first per current impl. So check it
    # passes validation by NOT raising ValueError; we'll get NotRunning instead.)
    with pytest.raises(NotRunning):
        pc.seek(0)
