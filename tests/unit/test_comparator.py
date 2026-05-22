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
