import json
from pathlib import Path

import pytest

from sim_dji_cloud.storage.manifest import ManifestBuilder, validate_manifest

def test_manifest_builder_emits_required_fields(tmp_path: Path):
    mb = ManifestBuilder(
        flight_dir=tmp_path,
        task_id="T123",
        dock_sn="SN_DOCK_EXAMPLE",
        drone_sn="SN_DRONE_EXAMPLE",
        started_at_recv_ms=1715000000000,
    )
    mb.record_topic(
        topic="thing/product/SN_DOCK_EXAMPLE/osd",
        device_sn="SN_DOCK_EXAMPLE",
        direction="up",
        files=[{"name": "topics/thing__product__SN_DOCK_EXAMPLE__osd.jsonl",
                "count": 100, "first_ms": 1715000000000, "last_ms": 1715000100000}],
    )
    mb.set_takeoff_offset_ms(5000)
    mb.set_landing_offset_ms(95000)
    mb.add_gap(reason="mqtt_disconnect", start_ms=20000, end_ms=22000)
    mb.finalize(
        ended_at_recv_ms=1715000100000,
        finalize_reason="auto_idle",
        status="ok",
    )

    manifest_path = tmp_path / "manifest.json"
    assert manifest_path.exists()
    data = json.loads(manifest_path.read_text())

    assert data["schema_version"] == 1
    assert data["status"] == "ok"
    assert data["finalize_reason"] == "auto_idle"
    assert data["task_id"] == "T123"
    assert data["dock_sn"] == "SN_DOCK_EXAMPLE"
    assert data["drone_sn"] == "SN_DRONE_EXAMPLE"
    assert data["takeoff_offset_ms"] == 5000
    assert data["landing_offset_ms"] == 95000
    assert len(data["gaps"]) == 1
    assert len(data["topics"]) == 1
    assert data["topics"][0]["device_sn"] == "SN_DOCK_EXAMPLE"
    assert data["topics"][0]["count"] == 100

def test_manifest_aggregates_count_across_files(tmp_path: Path):
    mb = ManifestBuilder(
        flight_dir=tmp_path, task_id="T1",
        dock_sn="D1", drone_sn="A1", started_at_recv_ms=0,
    )
    mb.record_topic(
        topic="thing/product/D1/drc/up",
        device_sn="D1",
        direction="up",
        files=[
            {"name": "topics/thing__product__D1__drc__up.0001.jsonl",
             "count": 30000, "first_ms": 0, "last_ms": 1000000},
            {"name": "topics/thing__product__D1__drc__up.0002.jsonl",
             "count": 24000, "first_ms": 1000000, "last_ms": 1800000},
        ],
    )
    mb.finalize(ended_at_recv_ms=1800000, finalize_reason="auto_idle", status="ok")
    data = json.loads((tmp_path / "manifest.json").read_text())
    t = data["topics"][0]
    assert t["count"] == 54000
    assert t["first_recv_ts_ms"] == 0
    assert t["last_recv_ts_ms"] == 1800000

def test_validate_manifest_rejects_missing_required(tmp_path: Path):
    bad = {"schema_version": 1}
    (tmp_path / "manifest.json").write_text(json.dumps(bad))
    errors = validate_manifest(tmp_path / "manifest.json")
    assert any("task_id" in e for e in errors)
    assert any("dock_sn" in e for e in errors)

def test_validate_manifest_accepts_complete(tmp_path: Path):
    good = {
        "schema_version": 1,
        "status": "ok",
        "finalize_reason": "auto_idle",
        "task_id": "T1",
        "dock_sn": "D",
        "drone_sn": "A",
        "started_at_recv_ms": 0,
        "ended_at_recv_ms": 100,
        "takeoff_offset_ms": None,
        "landing_offset_ms": None,
        "gaps": [],
        "topics": [],
        "video": None,
    }
    (tmp_path / "manifest.json").write_text(json.dumps(good))
    errors = validate_manifest(tmp_path / "manifest.json")
    assert errors == []

def test_manifest_set_video_and_update_drone_sn(tmp_path: Path):
    mb = ManifestBuilder(
        flight_dir=tmp_path, task_id="T1",
        dock_sn="D1", drone_sn="unknown", started_at_recv_ms=0,
    )
    mb.update_drone_sn("A_REAL")
    mb.set_video({
        "file": "video/main.mp4",
        "source_url": "rtmp://x/live",
        "started_at_recv_ms": 500,
        "duration_ms": 1800000,
        "segments": [{"start_ms": 0, "end_ms": 1800000, "file": "video/main.mp4"}],
    })
    mb.finalize(ended_at_recv_ms=1800000, finalize_reason="auto_idle", status="ok")
    data = json.loads((tmp_path / "manifest.json").read_text())
    assert data["drone_sn"] == "A_REAL"
    assert data["video"]["file"] == "video/main.mp4"
    assert data["video"]["duration_ms"] == 1800000

def test_manifest_multiple_gaps_preserve_order(tmp_path: Path):
    mb = ManifestBuilder(
        flight_dir=tmp_path, task_id="T1",
        dock_sn="D1", drone_sn="A1", started_at_recv_ms=0,
    )
    mb.add_gap(reason="mqtt_disconnect", start_ms=100, end_ms=200)
    mb.add_gap(reason="mqtt_disconnect", start_ms=500, end_ms=600)
    mb.finalize(ended_at_recv_ms=1000, finalize_reason="auto_idle", status="ok")
    data = json.loads((tmp_path / "manifest.json").read_text())
    assert len(data["gaps"]) == 2
    assert data["gaps"][0]["start_ms"] == 100
    assert data["gaps"][1]["start_ms"] == 500

def test_manifest_post_finalize_mutations_raise(tmp_path: Path):
    mb = ManifestBuilder(
        flight_dir=tmp_path, task_id="T1",
        dock_sn="D1", drone_sn="A1", started_at_recv_ms=0,
    )
    mb.finalize(ended_at_recv_ms=100, finalize_reason="auto_idle", status="ok")

    with pytest.raises(RuntimeError, match="already finalized"):
        mb.record_topic(topic="x", device_sn="y", direction="up", files=[])
    with pytest.raises(RuntimeError, match="already finalized"):
        mb.add_gap(reason="r", start_ms=0, end_ms=1)
    with pytest.raises(RuntimeError, match="already finalized"):
        mb.set_video(None)
    with pytest.raises(RuntimeError, match="already finalized"):
        mb.set_takeoff_offset_ms(0)
    with pytest.raises(RuntimeError, match="already finalized"):
        mb.set_landing_offset_ms(0)
    with pytest.raises(RuntimeError, match="already finalized"):
        mb.update_drone_sn("X")

def test_manifest_finalize_rejects_bad_status(tmp_path: Path):
    mb = ManifestBuilder(
        flight_dir=tmp_path, task_id="T1",
        dock_sn="D1", drone_sn="A1", started_at_recv_ms=0,
    )
    with pytest.raises(ValueError, match="status must be"):
        mb.finalize(ended_at_recv_ms=100, finalize_reason="x", status="bogus")


def test_manifest_finalize_is_atomic_no_tmp_leak(tmp_path: Path):
    """finalize 写入后不应在飞行目录留下 manifest.json.tmp 残留。

    Regression for non-atomic write: 早期直接 write_text，崩溃中途留下截断 JSON
    打挂 inspect/list/play/selfcheck。修复改走 atomic_write_text。
    """
    mb = ManifestBuilder(
        flight_dir=tmp_path, task_id="T1",
        dock_sn="D1", drone_sn="A1", started_at_recv_ms=0,
    )
    mb.finalize(ended_at_recv_ms=100, finalize_reason="task_idle", status="ok")
    assert (tmp_path / "manifest.json").exists()
    assert not (tmp_path / "manifest.json.tmp").exists()
