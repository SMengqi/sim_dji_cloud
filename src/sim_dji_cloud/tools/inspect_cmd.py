import json
import sys
from pathlib import Path


def inspect_flight(flight_dir: Path) -> int:
    flight_dir = Path(flight_dir)
    manifest_path = flight_dir / "manifest.json"
    if not manifest_path.exists():
        print(f"ERROR: missing manifest.json in {flight_dir}", file=sys.stderr)
        return 2
    m = json.loads(manifest_path.read_text())

    print(f"Flight: {flight_dir.name}")
    print(f"  task_id        : {m.get('task_id')}")
    print(f"  dock_sn        : {m.get('dock_sn')}")
    print(f"  drone_sn       : {m.get('drone_sn')}")
    print(f"  status         : {m.get('status')}")
    print(f"  finalize_reason: {m.get('finalize_reason')}")
    dur = (m.get("ended_at_recv_ms") or 0) - (m.get("started_at_recv_ms") or 0)
    print(f"  duration       : {dur / 1000:.1f}s")
    print(f"  takeoff_offset : {m.get('takeoff_offset_ms')}")
    print(f"  landing_offset : {m.get('landing_offset_ms')}")
    print(f"  gaps           : {len(m.get('gaps', []))} gap(s)")
    for g in m.get("gaps", []):
        print(f"    - {g['reason']}: [{g['start_ms']}, {g['end_ms']}]")

    print(f"  topics ({len(m.get('topics', []))} total):")
    for t in m.get("topics", []):
        if t.get("device_sn") == m.get("dock_sn"):
            role = "dock "
        elif t.get("device_sn") == m.get("drone_sn"):
            role = "drone"
        else:
            role = "?    "
        print(
            f"    [{role}] {t['topic']}  count={t['count']}  "
            f"first={t.get('first_recv_ts_ms')}  last={t.get('last_recv_ts_ms')}"
        )

    video = m.get("video")
    if video:
        print(
            f"  video          : {video.get('file')}  "
            f"duration={video.get('duration_ms', 0) / 1000:.1f}s"
        )
    else:
        print("  video          : (none)")
    return 0
