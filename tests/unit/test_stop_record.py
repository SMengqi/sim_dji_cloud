from pathlib import Path
from sim_dji_cloud.tools.stop_record_cmd import stop_record
from sim_dji_cloud.recorder.stop_signal import StopSignalFile, read_stop_signal_if_present


def test_stop_record_writes_signal_file(tmp_path: Path):
    state_dir = tmp_path / "state"
    code = stop_record(task_id="T1", state_dir=state_dir)
    assert code == 0
    sig = read_stop_signal_if_present(StopSignalFile(state_dir, "T1"))
    assert sig is not None
    assert sig["reason"] == "manual_stop"
