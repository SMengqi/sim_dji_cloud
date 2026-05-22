import json
from pathlib import Path
from sim_dji_cloud.tools.repair_cmd import repair_flight


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
