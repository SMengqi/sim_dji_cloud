from pathlib import Path
from sim_dji_cloud.recorder.stop_signal import StopSignalFile, write_stop_signal


def stop_record(task_id: str, state_dir: Path) -> int:
    s = StopSignalFile(state_dir=Path(state_dir), task_id=task_id)
    write_stop_signal(s, reason="manual_stop")
    print(f"stop signal written for task_id={task_id} at {s.path}")
    return 0
