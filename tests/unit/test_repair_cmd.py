import json
from pathlib import Path
from sim_dji_cloud.tools.repair_cmd import repair_flight, _parse_dir_name


def test_repair_rebuilds_manifest_from_topics(tmp_path: Path):
    flight = tmp_path / "T-X__SN_DOCK__20260521-100000"
    (flight / "topics").mkdir(parents=True)
    (flight / "topics" / "thing__product__SN_DOCK__osd.0001.jsonl").write_text(
        '{"recv_ts_ms":1000,"dji_ts_ms":1000,"direction":"up","topic":"thing/product/SN_DOCK/osd","payload":{}}\n'
        '{"recv_ts_ms":2000,"dji_ts_ms":2000,"direction":"up","topic":"thing/product/SN_DOCK/osd","payload":{}}\n'
    )
    (flight / "topics" / "thing__product__SN_DRONE__osd.0001.jsonl").write_text(
        '{"recv_ts_ms":1500,"dji_ts_ms":1500,"direction":"up","topic":"thing/product/SN_DRONE/osd","payload":{}}\n'
    )

    code = repair_flight(flight)
    assert code == 0

    m = json.loads((flight / "manifest.json").read_text())
    assert m["task_id"] == "T-X"
    assert m["dock_sn"] == "SN_DOCK"
    assert m["status"] == "interrupted"
    assert m["finalize_reason"] == "repaired"
    assert m["started_at_recv_ms"] == 1000
    assert m["ended_at_recv_ms"] == 2000
    topics_map = {t["topic"]: t for t in m["topics"]}
    assert topics_map["thing/product/SN_DOCK/osd"]["count"] == 2
    assert topics_map["thing/product/SN_DRONE/osd"]["count"] == 1


def test_repair_skips_when_manifest_already_exists(tmp_path: Path, capsys):
    flight = tmp_path / "T-Y__SN_DOCK__20260521-100000"
    (flight / "topics").mkdir(parents=True)
    (flight / "manifest.json").write_text('{"schema_version":1}')
    code = repair_flight(flight)
    captured = capsys.readouterr()
    assert code != 0
    assert "exists" in captured.err.lower() or "exists" in captured.out.lower()


def test_repair_force_overwrites(tmp_path: Path):
    flight = tmp_path / "T-Z__SN_DOCK__20260521-100000"
    (flight / "topics").mkdir(parents=True)
    (flight / "manifest.json").write_text('{"schema_version":1,"junk":true}')
    (flight / "topics" / "thing__product__SN_DOCK__osd.0001.jsonl").write_text(
        '{"recv_ts_ms":5,"topic":"thing/product/SN_DOCK/osd","payload":{}}\n'
    )
    code = repair_flight(flight, force=True)
    assert code == 0
    m = json.loads((flight / "manifest.json").read_text())
    assert m["finalize_reason"] == "repaired"
    assert "junk" not in m


# --- dual-format parsing tests ---

def test_parse_legacy_format():
    tid, sn = _parse_dir_name("UUID-XYZ__SN_DOCK__20260521-153732")
    assert tid == "UUID-XYZ"
    assert sn == "SN_DOCK"


def test_parse_new_format():
    tid, sn = _parse_dir_name("SN_DOCK_20260521-153732")
    assert tid == "unknown"
    assert sn == "SN_DOCK"


def test_parse_new_format_with_ms3():
    tid, sn = _parse_dir_name("SN_DOCK_20260521-153732_456")
    assert tid == "unknown"
    assert sn == "SN_DOCK"


def test_parse_unrecognized():
    tid, sn = _parse_dir_name("totally_unrelated_name")
    assert tid == "unknown"
    assert sn == "unknown"


def test_repair_new_format_dir(tmp_path: Path):
    """repair_flight works on new-format <dock_sn>_<ts> directory names."""
    flight = tmp_path / "SN_DOCK_20260521-100000"
    (flight / "topics").mkdir(parents=True)
    (flight / "topics" / "thing__product__SN_DOCK__osd.0001.jsonl").write_text(
        '{"recv_ts_ms":1000,"dji_ts_ms":1000,"direction":"up","topic":"thing/product/SN_DOCK/osd","payload":{}}\n'
    )
    code = repair_flight(flight)
    assert code == 0
    m = json.loads((flight / "manifest.json").read_text())
    assert m["task_id"] == "unknown"
    assert m["dock_sn"] == "SN_DOCK"
    assert m["finalize_reason"] == "repaired"


def test_repair_merges_multi_volume_topics(tmp_path: Path):
    """同一 topic 的多卷 (.0001 / .0002 / ...) 必须合到一条 topic 下的 files[]。

    Regression (review MAJOR): 旧实现把每卷文件当独立 topic 加进 topics[]，
    repair 后的 manifest 不再多卷聚合，selfcheck/player 读到的 files 缺卷。
    """
    flight = tmp_path / "SN_DOCK_20260601-100000"
    (flight / "topics").mkdir(parents=True)
    (flight / "topics" / "thing__product__SN_DOCK__osd.0001.jsonl").write_text(
        '{"recv_ts_ms":1000,"topic":"thing/product/SN_DOCK/osd","payload":{}}\n'
        '{"recv_ts_ms":1500,"topic":"thing/product/SN_DOCK/osd","payload":{}}\n'
    )
    (flight / "topics" / "thing__product__SN_DOCK__osd.0002.jsonl").write_text(
        '{"recv_ts_ms":2000,"topic":"thing/product/SN_DOCK/osd","payload":{}}\n'
    )
    (flight / "topics" / "thing__product__SN_DOCK__osd.0003.jsonl").write_text(
        '{"recv_ts_ms":2800,"topic":"thing/product/SN_DOCK/osd","payload":{}}\n'
        '{"recv_ts_ms":3500,"topic":"thing/product/SN_DOCK/osd","payload":{}}\n'
    )

    code = repair_flight(flight)
    assert code == 0
    m = json.loads((flight / "manifest.json").read_text())

    osd_topics = [t for t in m["topics"]
                  if t["topic"] == "thing/product/SN_DOCK/osd"]
    assert len(osd_topics) == 1, (
        f"多卷必须合并到 1 条 topic entry，实际 {len(osd_topics)}"
    )
    entry = osd_topics[0]
    # 5 条记录跨 3 卷 (2 + 1 + 2)
    assert entry["count"] == 5
    assert entry["first_recv_ts_ms"] == 1000
    assert entry["last_recv_ts_ms"] == 3500
    file_names = sorted(f["name"] for f in entry["files"])
    assert file_names == [
        "topics/thing__product__SN_DOCK__osd.0001.jsonl",
        "topics/thing__product__SN_DOCK__osd.0002.jsonl",
        "topics/thing__product__SN_DOCK__osd.0003.jsonl",
    ]
    by_name = {f["name"]: f for f in entry["files"]}
    assert by_name["topics/thing__product__SN_DOCK__osd.0001.jsonl"]["count"] == 2
    assert by_name["topics/thing__product__SN_DOCK__osd.0002.jsonl"]["count"] == 1
    assert by_name["topics/thing__product__SN_DOCK__osd.0003.jsonl"]["count"] == 2
