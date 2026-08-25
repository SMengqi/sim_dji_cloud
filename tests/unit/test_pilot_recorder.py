import json
import pytest
from pathlib import Path

from sim_dji_cloud.recorder.pilot_recorder import PilotRecorder
from sim_dji_cloud.recorder.pilot_flight_detector import PilotFlightState


def _cfg(tmp_path: Path) -> dict:
    return {
        "mqtt": {
            "host": "localhost", "port": 1883, "tls": False,
            "client_id": "t", "username": None, "password": None,
            "ca_file": None, "cert_file": None, "key_file": None,
            "subscribe_patterns": [],
            "deny_topics": [],
        },
        "storage": {
            "root": str(tmp_path / "rec_pilot"),
            "enable_raw_firehose": False,
            "flush_max_records": 1,
            "flush_interval_ms": 50,
            "queue_max_size": 1000,
            "rotate_max_bytes": 10**9,
            "rotate_max_records": 10**6,
        },
        "pilot_flight_detection": {
            "idle_debounce_seconds": 0,
        },
    }


async def _topo(rec, sns, ts):
    await rec.on_mqtt_message(
        "sys/product/SN_RC/status",
        json.dumps({"method": "update_topo",
                    "data": {"sub_devices": [{"sn": s} for s in sns]}}).encode(),
        ts,
    )


@pytest.mark.asyncio
async def test_writes_jsonl_and_manifest_when_aircraft_online(tmp_path):
    rec = PilotRecorder(_cfg(tmp_path), rc_sn="SN_RC", aircraft_sn=None)
    await rec.start_async_components()

    await _topo(rec, ["SN_AIRCRAFT"], 1000)
    await rec.on_mqtt_message(
        "thing/product/SN_RC/osd",
        json.dumps({"data": {"capacity_percent": 80}}).encode(),
        1500,
    )
    await rec.on_mqtt_message(
        "thing/product/SN_AIRCRAFT/osd",
        json.dumps({"data": {"mode_code": 3}}).encode(),
        2000,
    )

    flight_dir = await rec.finalize_and_close(finalize_reason="aircraft_offline")
    assert flight_dir.exists()
    assert (flight_dir / "manifest.json").exists()

    manifest = json.loads((flight_dir / "manifest.json").read_text())
    assert manifest["dock_sn"] == "SN_RC"
    assert manifest["drone_sn"] == "SN_AIRCRAFT"
    topics = {t["topic"] for t in manifest["topics"]}
    assert "thing/product/SN_RC/osd" in topics
    assert "thing/product/SN_AIRCRAFT/osd" in topics


@pytest.mark.asyncio
async def test_finalize_writes_extra_gaps_into_manifest(tmp_path):
    rec = PilotRecorder(_cfg(tmp_path), rc_sn="SN_RC", aircraft_sn=None)
    await rec.start_async_components()
    await _topo(rec, ["SN_AIRCRAFT"], 500)

    fake_gaps = [
        {"reason": "mqtt_disconnect", "start_ms": 600, "end_ms": 700},
        {"reason": "mqtt_disconnect", "start_ms": 1200, "end_ms": 1250},
    ]
    flight_dir = await rec.finalize_and_close(
        finalize_reason="aircraft_offline", extra_gaps=fake_gaps,
    )
    manifest = json.loads((flight_dir / "manifest.json").read_text())
    gaps = manifest["gaps"]
    assert len(gaps) == 2
    assert gaps[0]["reason"] == "mqtt_disconnect"
    assert gaps[0]["start_ms"] == 600
    assert gaps[1]["end_ms"] == 1250


@pytest.mark.asyncio
async def test_two_online_offline_cycles_make_two_flight_dirs(tmp_path):
    rec = PilotRecorder(_cfg(tmp_path), rc_sn="SN_RC", aircraft_sn=None)
    await rec.start_async_components()

    await _topo(rec, ["SN_AIRCRAFT"], 1000)
    await _topo(rec, [], 2000)   # idle_debounce_seconds=0 → 立刻 FINALIZING
    assert rec._detector.state == PilotFlightState.FINALIZING
    dir1 = await rec.finalize_and_close("aircraft_offline")
    await rec.reset_for_next_flight()
    assert rec.flight_dir is None
    assert rec._detector.state == PilotFlightState.WAITING_AIRCRAFT
    assert rec.aircraft_sn == "SN_AIRCRAFT"   # 跨段保留已学到的 SN

    await _topo(rec, ["SN_AIRCRAFT"], 5000)
    await _topo(rec, [], 6000)
    dir2 = await rec.finalize_and_close("aircraft_offline")

    assert dir1.exists() and dir2.exists()
    assert dir1 != dir2
    assert (dir1 / "manifest.json").exists()
    assert (dir2 / "manifest.json").exists()


@pytest.mark.asyncio
async def test_pending_dir_renamed_once_track_id_learned(tmp_path):
    rec = PilotRecorder(_cfg(tmp_path), rc_sn="SN_RC", aircraft_sn=None)
    await rec.start_async_components()

    await _topo(rec, ["SN_AIRCRAFT"], 1000)
    assert rec.flight_dir is not None
    assert rec.flight_dir.name.startswith("pending_")

    await rec.on_mqtt_message(
        "thing/product/SN_AIRCRAFT/osd",
        json.dumps({"data": {"track_id": "TRK-42"}}).encode(),
        1500,
    )
    assert rec.flight_dir is not None
    assert not rec.flight_dir.name.startswith("pending_")
    assert rec.flight_dir.name.startswith("SN_RC_")

    flight_dir = await rec.finalize_and_close("aircraft_offline")
    manifest = json.loads((flight_dir / "manifest.json").read_text())
    assert manifest["task_id"] == "TRK-42"


@pytest.mark.asyncio
async def test_on_mqtt_message_accepts_str_payload(tmp_path):
    """跟 dock 版一样，gmqtt 某些版本传 str 而不是 bytes；不应抛 AttributeError。"""
    rec = PilotRecorder(_cfg(tmp_path), rc_sn="SN_RC", aircraft_sn=None)
    await rec.start_async_components()
    await rec.on_mqtt_message(
        "sys/product/SN_RC/status",
        '{"data": {"sub_devices": [{"sn": "SN_AIRCRAFT"}]}}',   # type: ignore[arg-type]
        500,
    )
    assert rec._detector.state == PilotFlightState.RECORDING
