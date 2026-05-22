import json
import sys
from pathlib import Path


def list_flights(root: Path) -> int:
    root = Path(root)
    if not root.exists():
        print(f"ERROR: root does not exist: {root}", file=sys.stderr)
        return 2

    entries = sorted([d for d in root.iterdir() if d.is_dir()])
    found_any = False
    for d in entries:
        manifest_path = d / "manifest.json"
        if not manifest_path.exists():
            continue
        try:
            m = json.loads(manifest_path.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        found_any = True
        dur = (m.get("ended_at_recv_ms") or 0) - (m.get("started_at_recv_ms") or 0)
        n_topics = len(m.get("topics", []))
        n_records = sum(t.get("count", 0) for t in m.get("topics", []))
        has_video = "yes" if m.get("video") else "no "
        print(
            f"{d.name}\n"
            f"  task_id={m.get('task_id')}  status={m.get('status')}  "
            f"duration={dur/1000:.1f}s  topics={n_topics}  records={n_records}  "
            f"video={has_video}"
        )
    if not found_any:
        print("no flights found")
    return 0
