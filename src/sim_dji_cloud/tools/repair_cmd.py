import json
import re
import sys
from pathlib import Path
from typing import Optional

from sim_dji_cloud.storage.atomic_write import atomic_write_text


_DIR_RE_LEGACY = re.compile(r"^(.+?)__(.+?)__(\d{8}-\d{6})$")
_DIR_RE_NEW = re.compile(r"^(.+)_(\d{8}-\d{6})(?:_\d{3})?$")


def _parse_dir_name(name: str) -> tuple[str, str]:
    """Return (task_id, dock_sn). 'unknown' for missing components."""
    m = _DIR_RE_LEGACY.match(name)
    if m:
        return m.group(1), m.group(2)
    m = _DIR_RE_NEW.match(name)
    if m:
        return "unknown", m.group(1)
    return "unknown", "unknown"


def _parse_topic_from_filename(filename: str) -> Optional[str]:
    base = filename.split(".0001.jsonl")[0].split(".jsonl")[0]
    base = re.sub(r"\.\d{4}$", "", base)
    if "__" not in base:
        return None
    return base.replace("__", "/")


def _scan_topic_file(path: Path) -> dict:
    count = 0
    first_ms = None
    last_ms = None
    device_sn = ""
    direction = "up"
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            count += 1
            ts = rec.get("recv_ts_ms")
            if isinstance(ts, int):
                if first_ms is None:
                    first_ms = ts
                last_ms = ts
            topic = rec.get("topic", "")
            parts = topic.split("/")
            if len(parts) >= 3:
                device_sn = parts[2]
            direction = rec.get("direction", direction)
    return {
        "count": count, "first_ms": first_ms, "last_ms": last_ms,
        "device_sn": device_sn, "direction": direction,
    }


def repair_flight(flight_dir: Path, force: bool = False) -> int:
    flight_dir = Path(flight_dir)
    manifest_path = flight_dir / "manifest.json"
    if manifest_path.exists() and not force:
        print(f"ERROR: manifest.json already exists at {manifest_path}; "
              "use --force to overwrite", file=sys.stderr)
        return 1

    dir_name = flight_dir.name
    task_id, dock_sn = _parse_dir_name(dir_name)

    topics_dir = flight_dir / "topics"
    if not topics_dir.exists():
        print(f"ERROR: no topics/ directory in {flight_dir}", file=sys.stderr)
        return 2

    topics: list[dict] = []
    overall_first = None
    overall_last = None
    drone_sn = "unknown"
    for jsonl_path in sorted(topics_dir.glob("*.jsonl")):
        topic = _parse_topic_from_filename(jsonl_path.name)
        if not topic:
            continue
        stats = _scan_topic_file(jsonl_path)
        if stats["count"] == 0:
            continue
        topics.append({
            "topic": topic,
            "device_sn": stats["device_sn"],
            "direction": stats["direction"],
            "count": stats["count"],
            "first_recv_ts_ms": stats["first_ms"],
            "last_recv_ts_ms": stats["last_ms"],
            "files": [{
                "name": f"topics/{jsonl_path.name}",
                "count": stats["count"],
                "first_ms": stats["first_ms"],
                "last_ms": stats["last_ms"],
            }],
        })
        if stats["first_ms"] is not None:
            overall_first = (stats["first_ms"] if overall_first is None
                             else min(overall_first, stats["first_ms"]))
        if stats["last_ms"] is not None:
            overall_last = (stats["last_ms"] if overall_last is None
                            else max(overall_last, stats["last_ms"]))
        if stats["device_sn"] and stats["device_sn"] != dock_sn:
            drone_sn = stats["device_sn"]

    manifest = {
        "schema_version": 1,
        "status": "interrupted",
        "finalize_reason": "repaired",
        "task_id": task_id,
        "dock_sn": dock_sn,
        "drone_sn": drone_sn,
        "started_at_recv_ms": overall_first or 0,
        "ended_at_recv_ms": overall_last or 0,
        "takeoff_offset_ms": None,
        "landing_offset_ms": None,
        "gaps": [],
        "topics": topics,
        "video": None,
    }
    atomic_write_text(manifest_path, json.dumps(manifest, ensure_ascii=False, indent=2))
    print(f"repaired manifest written: {manifest_path}")
    return 0
