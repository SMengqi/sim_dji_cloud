from pathlib import Path
from sim_dji_cloud.recorder.stop_signal import (
    StopSignalFile, write_stop_signal, read_stop_signal_if_present,
)

def test_write_and_read_stop_signal(tmp_path: Path):
    s = StopSignalFile(state_dir=tmp_path, task_id="T123")
    assert read_stop_signal_if_present(s) is None
    write_stop_signal(s, reason="manual_stop")
    sig = read_stop_signal_if_present(s)
    assert sig is not None
    assert sig["reason"] == "manual_stop"
    assert sig["task_id"] == "T123"

def test_signal_consumed_after_read(tmp_path: Path):
    s = StopSignalFile(state_dir=tmp_path, task_id="T1")
    write_stop_signal(s, reason="manual_stop")
    assert read_stop_signal_if_present(s) is not None
    assert read_stop_signal_if_present(s) is None
