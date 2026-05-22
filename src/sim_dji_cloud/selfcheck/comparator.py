import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class TopicCompareResult:
    topic: str
    device_sn: str
    orig_count: int
    replayed_count: int
    first_divergence_index: Optional[int]
    first_divergence_field: Optional[str]
    max_relative_drift_ms: int
    out_of_order_count: int
    max_reorder_offset: int
    status: str  # PASS | FAIL


def _canonical_payload(p) -> str:
    return json.dumps(p, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _load_records(path: Path) -> list[dict]:
    if not path.exists():
        return []
    out: list[dict] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
    return out


def compare_topic(
    orig_path: Path,
    replay_path: Path,
    tolerance_ms: int = 50,
    topic_label: str = "",
    device_sn: str = "",
) -> TopicCompareResult:
    orig = _load_records(orig_path)
    replay = _load_records(replay_path)

    first_div_idx: Optional[int] = None
    first_div_field: Optional[str] = None
    max_drift = 0
    out_of_order = 0
    max_reorder = 0

    if len(orig) != len(replay):
        return TopicCompareResult(
            topic=topic_label, device_sn=device_sn,
            orig_count=len(orig), replayed_count=len(replay),
            first_divergence_index=None,
            first_divergence_field=f"count(orig={len(orig)}, replay={len(replay)})",
            max_relative_drift_ms=0,
            out_of_order_count=0, max_reorder_offset=0, status="FAIL",
        )

    for i, (a, b) in enumerate(zip(orig, replay)):
        if a.get("topic") != b.get("topic"):
            first_div_idx, first_div_field = i, "topic"
            break
        if a.get("direction") != b.get("direction"):
            first_div_idx, first_div_field = i, "direction"
            break
        if a.get("dji_ts_ms") != b.get("dji_ts_ms"):
            first_div_idx, first_div_field = i, "dji_ts_ms"
            break
        if _canonical_payload(a.get("payload")) != _canonical_payload(b.get("payload")):
            first_div_idx, first_div_field = i, "payload"
            break
        if i > 0:
            dt_o = a["recv_ts_ms"] - orig[i - 1]["recv_ts_ms"]
            dt_r = b["recv_ts_ms"] - replay[i - 1]["recv_ts_ms"]
            drift = abs(dt_o - dt_r)
            if drift > max_drift:
                max_drift = drift

    for i in range(1, len(replay)):
        if replay[i]["recv_ts_ms"] < replay[i - 1]["recv_ts_ms"]:
            out_of_order += 1
            max_reorder = max(max_reorder, 1)

    status = "PASS"
    if first_div_field is not None:
        status = "FAIL"
    if max_drift > tolerance_ms:
        status = "FAIL"
        if first_div_field is None:
            first_div_field = f"recv_interval_drift({max_drift}ms)"
    if out_of_order > 0:
        status = "FAIL"

    return TopicCompareResult(
        topic=topic_label, device_sn=device_sn,
        orig_count=len(orig), replayed_count=len(replay),
        first_divergence_index=first_div_idx,
        first_divergence_field=first_div_field,
        max_relative_drift_ms=max_drift,
        out_of_order_count=out_of_order, max_reorder_offset=max_reorder,
        status=status,
    )
