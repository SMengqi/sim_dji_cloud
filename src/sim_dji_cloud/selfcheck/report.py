import json
from dataclasses import asdict
from pathlib import Path
from .comparator import TopicCompareResult


def write_reports(
    out_dir: Path,
    results: list[TopicCompareResult],
    tolerance_ms: int,
    flight_dir_name: str,
) -> None:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    overall = "PASS" if all(r.status == "PASS" for r in results) else "FAIL"

    summary = {
        "flight_dir": flight_dir_name,
        "tolerance_ms": tolerance_ms,
        "overall": overall,
        "topics": [asdict(r) for r in results],
    }
    (out_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2)
    )

    lines = []
    lines.append(f"# SelfCheck 报告：{flight_dir_name}")
    lines.append("")
    lines.append(f"- tolerance_ms = {tolerance_ms}")
    lines.append(f"- overall = **{overall}**")
    lines.append("")
    lines.append("| topic | device_sn | orig | replay | drift_ms | reorder | status |")
    lines.append("|---|---|---|---|---|---|---|")
    for r in results:
        lines.append(
            f"| {r.topic} | {r.device_sn} | {r.orig_count} | {r.replayed_count} | "
            f"{r.max_relative_drift_ms} | {r.out_of_order_count} | **{r.status}** |"
        )
        if r.status != "PASS":
            lines.append(
                f"  - first divergence: idx={r.first_divergence_index}, "
                f"field={r.first_divergence_field}"
            )
    (out_dir / "summary.md").write_text("\n".join(lines) + "\n")
