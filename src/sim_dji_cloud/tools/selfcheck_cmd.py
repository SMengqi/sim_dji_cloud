from pathlib import Path
from sim_dji_cloud.selfcheck import SelfCheck


async def run_selfcheck(
    flight_dir: Path,
    tolerance_ms: int,
    report_dir: Path | None,
) -> int:
    sc = SelfCheck(
        flight_dir=Path(flight_dir),
        report_dir=report_dir,
        tolerance_ms=tolerance_ms,
    )
    result = await sc.run_with_inprocess_loopback()
    print(f"SelfCheck overall: {result.overall}")
    print(f"  report: {result.report_dir}")
    for r in result.topic_results:
        print(
            f"  [{r.status}] {r.topic}  "
            f"orig={r.orig_count} replay={r.replayed_count} drift={r.max_relative_drift_ms}ms"
        )
    return 0 if result.overall == "PASS" else 2
