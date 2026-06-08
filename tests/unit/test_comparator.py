import json
from pathlib import Path
from sim_dji_cloud.selfcheck.comparator import compare_topic, TopicCompareResult


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


def test_identical_topics_pass(tmp_path: Path):
    orig = tmp_path / "orig.jsonl"
    replay = tmp_path / "replay.jsonl"
    recs = [
        {"recv_ts_ms": 0, "dji_ts_ms": 0, "direction": "up", "topic": "t", "payload": {"a": 1}},
        {"recv_ts_ms": 1000, "dji_ts_ms": 1000, "direction": "up", "topic": "t", "payload": {"a": 2}},
    ]
    _write_jsonl(orig, recs)
    _write_jsonl(replay, [dict(r, recv_ts_ms=r["recv_ts_ms"] + 50_000) for r in recs])

    result = compare_topic(orig, replay, tolerance_ms=50)
    assert result.status == "PASS"
    assert result.orig_count == 2
    assert result.replayed_count == 2


def test_payload_deep_equal_ignores_key_order(tmp_path: Path):
    orig = tmp_path / "orig.jsonl"
    replay = tmp_path / "replay.jsonl"
    _write_jsonl(orig, [
        {"recv_ts_ms": 0, "dji_ts_ms": 0, "direction": "up", "topic": "t", "payload": {"a": 1, "b": 2}},
    ])
    _write_jsonl(replay, [
        {"recv_ts_ms": 100, "dji_ts_ms": 0, "direction": "up", "topic": "t", "payload": {"b": 2, "a": 1}},
    ])
    result = compare_topic(orig, replay, tolerance_ms=50)
    assert result.status == "PASS"


def test_count_mismatch_fails(tmp_path: Path):
    orig = tmp_path / "orig.jsonl"
    replay = tmp_path / "replay.jsonl"
    _write_jsonl(orig, [
        {"recv_ts_ms": i, "dji_ts_ms": 0, "direction": "up", "topic": "t", "payload": {}}
        for i in range(3)
    ])
    _write_jsonl(replay, [
        {"recv_ts_ms": i, "dji_ts_ms": 0, "direction": "up", "topic": "t", "payload": {}}
        for i in range(2)
    ])
    result = compare_topic(orig, replay, tolerance_ms=50)
    assert result.status == "FAIL"
    assert "count" in (result.first_divergence_field or "")


def test_dji_ts_mismatch_fails(tmp_path: Path):
    orig = tmp_path / "orig.jsonl"
    replay = tmp_path / "replay.jsonl"
    _write_jsonl(orig, [{"recv_ts_ms": 0, "dji_ts_ms": 100,
                        "direction": "up", "topic": "t", "payload": {}}])
    _write_jsonl(replay, [{"recv_ts_ms": 0, "dji_ts_ms": 999,
                          "direction": "up", "topic": "t", "payload": {}}])
    result = compare_topic(orig, replay, tolerance_ms=50)
    assert result.status == "FAIL"
    assert result.first_divergence_field == "dji_ts_ms"


def test_relative_interval_within_tolerance(tmp_path: Path):
    orig = tmp_path / "orig.jsonl"
    replay = tmp_path / "replay.jsonl"
    _write_jsonl(orig, [
        {"recv_ts_ms": 0, "dji_ts_ms": 0, "direction": "up", "topic": "t", "payload": {}},
        {"recv_ts_ms": 1000, "dji_ts_ms": 0, "direction": "up", "topic": "t", "payload": {}},
    ])
    _write_jsonl(replay, [
        {"recv_ts_ms": 10000, "dji_ts_ms": 0, "direction": "up", "topic": "t", "payload": {}},
        {"recv_ts_ms": 11040, "dji_ts_ms": 0, "direction": "up", "topic": "t", "payload": {}},
    ])
    result = compare_topic(orig, replay, tolerance_ms=50)
    assert result.status == "PASS"
    assert result.max_relative_drift_ms <= 50


def test_relative_interval_exceeds_tolerance(tmp_path: Path):
    orig = tmp_path / "orig.jsonl"
    replay = tmp_path / "replay.jsonl"
    _write_jsonl(orig, [
        {"recv_ts_ms": 0, "dji_ts_ms": 0, "direction": "up", "topic": "t", "payload": {}},
        {"recv_ts_ms": 1000, "dji_ts_ms": 0, "direction": "up", "topic": "t", "payload": {}},
    ])
    _write_jsonl(replay, [
        {"recv_ts_ms": 0, "dji_ts_ms": 0, "direction": "up", "topic": "t", "payload": {}},
        {"recv_ts_ms": 1200, "dji_ts_ms": 0, "direction": "up", "topic": "t", "payload": {}},
    ])
    result = compare_topic(orig, replay, tolerance_ms=50)
    assert result.status == "FAIL"
    assert result.max_relative_drift_ms == 200


def test_drift_includes_records_after_divergence(tmp_path: Path):
    """drift 计算完整 scan 全部对齐记录，不在首次 divergence 时停。

    Regression (review MAJOR): 旧 break-on-divergence 让 divergence 之后的大
    drift 被丢，报告里 max_drift 严重偏低误导分析。
    """
    orig = tmp_path / "orig.jsonl"
    replay = tmp_path / "replay.jsonl"
    # index=1 payload 不同 → divergence；index=2 的 recv_ts gap 异常（500ms vs 100ms）
    _write_jsonl(orig, [
        {"recv_ts_ms": 0,    "dji_ts_ms": 0,   "direction": "up", "topic": "t",
         "payload": {"a": 1}},
        {"recv_ts_ms": 1000, "dji_ts_ms": 100, "direction": "up", "topic": "t",
         "payload": {"a": 2}},
        {"recv_ts_ms": 1100, "dji_ts_ms": 200, "direction": "up", "topic": "t",
         "payload": {"a": 3}},
    ])
    _write_jsonl(replay, [
        {"recv_ts_ms": 0,    "dji_ts_ms": 0,   "direction": "up", "topic": "t",
         "payload": {"a": 1}},
        {"recv_ts_ms": 1000, "dji_ts_ms": 100, "direction": "up", "topic": "t",
         "payload": {"a": 999}},     # divergence here
        {"recv_ts_ms": 1500, "dji_ts_ms": 200, "direction": "up", "topic": "t",
         "payload": {"a": 3}},
    ])
    result = compare_topic(orig, replay, tolerance_ms=50)
    # 旧实现：break 在 i=1 → max_drift=0（divergence 之后的 gap 不算）
    # 新实现：完整 pass → orig dt = 1100-1000 = 100; replay dt = 1500-1000 = 500
    #                     → drift = 400
    assert result.max_relative_drift_ms == 400, (
        f"divergence 之后的 drift 应被算入；实际 {result.max_relative_drift_ms}"
    )
    assert result.first_divergence_field == "payload"


def test_max_reorder_records_actual_backstep_ms(tmp_path: Path):
    """max_reorder_offset 应当是 recv_ts_ms 实际倒退最大毫秒数，不是硬封顶 1。

    Regression (review MAJOR): 旧 max(_, 1) 让字段永远 0 或 1，毫无诊断意义。
    """
    orig = tmp_path / "orig.jsonl"
    replay = tmp_path / "replay.jsonl"
    _write_jsonl(orig, [
        {"recv_ts_ms": 0,    "dji_ts_ms": 0, "direction": "up", "topic": "t", "payload": {}},
        {"recv_ts_ms": 1000, "dji_ts_ms": 0, "direction": "up", "topic": "t", "payload": {}},
        {"recv_ts_ms": 2000, "dji_ts_ms": 0, "direction": "up", "topic": "t", "payload": {}},
    ])
    # 故意造 250ms 倒退（replay 第三条 recv_ts < 第二条）
    _write_jsonl(replay, [
        {"recv_ts_ms": 0,    "dji_ts_ms": 0, "direction": "up", "topic": "t", "payload": {}},
        {"recv_ts_ms": 1500, "dji_ts_ms": 0, "direction": "up", "topic": "t", "payload": {}},
        {"recv_ts_ms": 1250, "dji_ts_ms": 0, "direction": "up", "topic": "t", "payload": {}},
    ])
    result = compare_topic(orig, replay, tolerance_ms=500)
    assert result.out_of_order_count == 1
    assert result.max_reorder_offset == 250, (
        f"应记 250ms 倒退，旧实现硬封顶 1；实际 {result.max_reorder_offset}"
    )
