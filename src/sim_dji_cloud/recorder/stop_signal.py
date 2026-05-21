import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class StopSignalFile:
    state_dir: Path
    task_id: str

    @property
    def path(self) -> Path:
        return self.state_dir / f"stop_{self.task_id}.signal"


def write_stop_signal(s: StopSignalFile, reason: str) -> None:
    s.state_dir.mkdir(parents=True, exist_ok=True)
    s.path.write_text(json.dumps({"task_id": s.task_id, "reason": reason}))


def read_stop_signal_if_present(s: StopSignalFile) -> Optional[dict]:
    if not s.path.exists():
        return None
    data = json.loads(s.path.read_text())
    s.path.unlink()
    return data
