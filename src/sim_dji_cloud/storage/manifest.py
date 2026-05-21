import json
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
REQUIRED_FIELDS = [
    "schema_version", "status", "finalize_reason",
    "task_id", "dock_sn", "drone_sn",
    "started_at_recv_ms", "ended_at_recv_ms",
    "gaps", "topics",
]


class ManifestBuilder:
    def __init__(
        self,
        flight_dir: Path,
        task_id: str,
        dock_sn: str,
        drone_sn: str,
        started_at_recv_ms: int,
    ):
        self.flight_dir = Path(flight_dir)
        self._finalized: bool = False
        self._data: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "status": "interrupted",
            "finalize_reason": None,
            "task_id": task_id,
            "dock_sn": dock_sn,
            "drone_sn": drone_sn,
            "started_at_recv_ms": started_at_recv_ms,
            "ended_at_recv_ms": None,
            "takeoff_offset_ms": None,
            "landing_offset_ms": None,
            "gaps": [],
            "topics": [],
            "video": None,
        }

    def _check_not_finalized(self) -> None:
        if self._finalized:
            raise RuntimeError("ManifestBuilder is already finalized; further mutations disallowed")

    def record_topic(
        self, topic: str, device_sn: str, direction: str,
        files: list[dict[str, Any]],
    ) -> None:
        self._check_not_finalized()
        total = sum(f["count"] for f in files)
        firsts = [f["first_ms"] for f in files if f.get("first_ms") is not None]
        lasts = [f["last_ms"] for f in files if f.get("last_ms") is not None]
        self._data["topics"].append({
            "topic": topic,
            "device_sn": device_sn,
            "direction": direction,
            "count": total,
            "first_recv_ts_ms": min(firsts) if firsts else None,
            "last_recv_ts_ms": max(lasts) if lasts else None,
            "files": files,
        })

    def set_takeoff_offset_ms(self, v: int) -> None:
        self._check_not_finalized()
        self._data["takeoff_offset_ms"] = v

    def set_landing_offset_ms(self, v: int) -> None:
        self._check_not_finalized()
        self._data["landing_offset_ms"] = v

    def set_video(self, video_meta: dict[str, Any] | None) -> None:
        self._check_not_finalized()
        self._data["video"] = video_meta

    def add_gap(self, reason: str, start_ms: int, end_ms: int) -> None:
        self._check_not_finalized()
        self._data["gaps"].append({
            "reason": reason, "start_ms": start_ms, "end_ms": end_ms,
        })

    def update_drone_sn(self, drone_sn: str) -> None:
        self._check_not_finalized()
        self._data["drone_sn"] = drone_sn

    def finalize(self, ended_at_recv_ms: int, finalize_reason: str, status: str) -> None:
        if status not in ("ok", "interrupted"):
            raise ValueError(f"status must be 'ok' or 'interrupted', got {status!r}")
        self._data["ended_at_recv_ms"] = ended_at_recv_ms
        self._data["finalize_reason"] = finalize_reason
        self._data["status"] = status
        self.flight_dir.mkdir(parents=True, exist_ok=True)
        (self.flight_dir / "manifest.json").write_text(
            json.dumps(self._data, ensure_ascii=False, indent=2)
        )
        self._finalized = True

    @property
    def data(self) -> dict[str, Any]:
        return dict(self._data)


def validate_manifest(path: Path) -> list[str]:
    """返回错误消息列表；空 list 表示通过。"""
    errors: list[str] = []
    try:
        data = json.loads(Path(path).read_text())
    except (json.JSONDecodeError, FileNotFoundError) as e:
        return [f"cannot read manifest: {e}"]

    for k in REQUIRED_FIELDS:
        if k not in data:
            errors.append(f"missing field: {k}")

    if data.get("schema_version") != SCHEMA_VERSION:
        errors.append(f"schema_version != {SCHEMA_VERSION}")

    if data.get("status") not in ("ok", "interrupted"):
        errors.append("status must be 'ok' or 'interrupted'")

    return errors
