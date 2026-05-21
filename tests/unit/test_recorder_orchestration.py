import json
import pytest
from pathlib import Path
from sim_dji_cloud.recorder import Recorder


@pytest.fixture
def minimal_config(tmp_path: Path):
    return {
        "mqtt": {
            "host": "localhost", "port": 1883, "tls": False,
            "client_id": "t", "username": None, "password": None,
            "ca_file": None, "cert_file": None, "key_file": None,
            "subscribe_patterns": [],
            "deny_topics": [],
        },
        "storage": {
            "root": str(tmp_path / "rec"),
            "enable_raw_firehose": False,
            "flush_max_records": 1,
            "flush_interval_ms": 50,
            "queue_max_size": 1000,
            "rotate_max_bytes": 10**9,
            "rotate_max_records": 10**6,
        },
        "video": {"enabled": False},
        "flight_detection": {
            "finalize_idle_seconds": 0,
            "rules": {
                "start": [{"source": "dock_services", "payload_match": {"method": "wayline_prepare"}}],
                "end": [{"source": "dock_osd", "field": "wayline_mission_state", "equals": "idle"}],
            },
        },
    }


@pytest.mark.asyncio
async def test_recorder_writes_jsonl_when_recording(minimal_config, tmp_path: Path):
    rec = Recorder(minimal_config, dock_sn="SN_DOCK", drone_sn=None)
    await rec.start_async_components()

    await rec.on_mqtt_message(
        topic="thing/product/SN_DOCK/services",
        payload=json.dumps({"method": "wayline_prepare", "data": {"flight_id": "T1"}}).encode(),
        recv_ts_ms=1000,
    )
    await rec.on_mqtt_message(
        topic="thing/product/SN_DOCK/osd",
        payload=json.dumps({"wayline_mission_state": "executing"}).encode(),
        recv_ts_ms=1500,
    )
    await rec.on_mqtt_message(
        topic="thing/product/SN_DRONE/osd",
        payload=json.dumps({"mode_code": "manual_flight"}).encode(),
        recv_ts_ms=2000,
    )
    await rec.on_mqtt_message(
        topic="thing/product/SN_DOCK/osd",
        payload=json.dumps({"wayline_mission_state": "idle"}).encode(),
        recv_ts_ms=10_000,
    )

    flight_dir = await rec.finalize_and_close(finalize_reason="auto_idle")
    assert flight_dir.exists()
    assert (flight_dir / "manifest.json").exists()

    manifest = json.loads((flight_dir / "manifest.json").read_text())
    assert manifest["task_id"] == "T1"
    assert manifest["dock_sn"] == "SN_DOCK"
    assert manifest["drone_sn"] == "SN_DRONE"
    topics = {t["topic"] for t in manifest["topics"]}
    assert "thing/product/SN_DOCK/services" in topics
    assert "thing/product/SN_DOCK/osd" in topics
    assert "thing/product/SN_DRONE/osd" in topics


@pytest.mark.asyncio
async def test_recorder_writes_pending_dir_before_task_id(minimal_config, tmp_path: Path):
    """task_id unknown → pending_<ts> dir; arrives → rename"""
    cfg = dict(minimal_config)
    cfg["flight_detection"]["rules"]["start"] = [
        {"source": "dock_osd", "field": "wayline_mission_state", "not_equals": "idle"}
    ]
    rec = Recorder(cfg, dock_sn="SN_DOCK", drone_sn=None)
    await rec.start_async_components()

    await rec.on_mqtt_message(
        "thing/product/SN_DOCK/osd",
        json.dumps({"wayline_mission_state": "executing"}).encode(),
        500,
    )
    assert rec.flight_dir is not None
    assert rec.flight_dir.name.startswith("pending_")

    await rec.on_mqtt_message(
        "thing/product/SN_DOCK/services",
        json.dumps({"method": "wayline_prepare", "data": {"flight_id": "T-RENAMED"}}).encode(),
        1000,
    )
    assert "T-RENAMED" in rec.flight_dir.name


@pytest.mark.asyncio
async def test_recorder_rename_preserves_pre_rename_records_in_manifest(
    minimal_config, tmp_path: Path,
):
    """Records written to pending dir before rename must appear in final manifest counts."""
    cfg = dict(minimal_config)
    cfg["flight_detection"]["rules"]["start"] = [
        {"source": "dock_osd", "field": "wayline_mission_state", "not_equals": "idle"}
    ]
    rec = Recorder(cfg, dock_sn="SN_DOCK", drone_sn=None)
    await rec.start_async_components()

    # 2 records to dock_osd pre-rename
    await rec.on_mqtt_message(
        "thing/product/SN_DOCK/osd",
        json.dumps({"wayline_mission_state": "executing", "n": 1}).encode(),
        500,
    )
    await rec.on_mqtt_message(
        "thing/product/SN_DOCK/osd",
        json.dumps({"wayline_mission_state": "executing", "n": 2}).encode(),
        600,
    )

    # rename trigger
    await rec.on_mqtt_message(
        "thing/product/SN_DOCK/services",
        json.dumps({"method": "wayline_prepare", "data": {"flight_id": "T-MERGE"}}).encode(),
        1000,
    )

    # 1 more record post-rename
    await rec.on_mqtt_message(
        "thing/product/SN_DOCK/osd",
        json.dumps({"wayline_mission_state": "executing", "n": 3}).encode(),
        1500,
    )

    flight_dir = await rec.finalize_and_close(finalize_reason="auto_idle")
    manifest = json.loads((flight_dir / "manifest.json").read_text())
    assert "T-MERGE" in flight_dir.name

    # dock_osd entry should reflect ALL 3 records (2 pre + 1 post)
    dock_osd = next(t for t in manifest["topics"] if t["topic"] == "thing/product/SN_DOCK/osd")
    assert dock_osd["count"] == 3
    assert dock_osd["first_recv_ts_ms"] == 500
    assert dock_osd["last_recv_ts_ms"] == 1500
