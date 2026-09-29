import json
from pathlib import Path

import pytest

from sim_dji_cloud.recorder import Recorder
from sim_dji_cloud.recorder.flight_detector import FlightState


def _cfg(tmp_path: Path) -> dict:
    return {
        "mqtt": {"host": "localhost", "port": 1883, "tls": False, "client_id": "t",
                 "username": None, "password": None, "ca_file": None, "cert_file": None,
                 "key_file": None, "subscribe_patterns": [], "deny_topics": []},
        "storage": {"root": str(tmp_path / "rec"), "enable_raw_firehose": False,
                    "flush_max_records": 1, "flush_interval_ms": 50, "queue_max_size": 1000,
                    "rotate_max_bytes": 10**9, "rotate_max_records": 10**6},
        "video": {"enabled": False},
        "flight_detection": {"record_steps": [0, 1, 2], "idle_debounce_seconds": 0},
    }


async def _step(rec, step, ts, extra=None):
    data = {"flighttask_step_code": step}
    if extra:
        data.update(extra)
    await rec.on_mqtt_message("thing/product/SN_DOCK/osd",
                              json.dumps({"data": data}).encode(), ts)


@pytest.mark.asyncio
async def test_two_task_cycles_make_two_flight_dirs(tmp_path):
    rec = Recorder(_cfg(tmp_path), dock_sn="SN_DOCK", drone_sn=None)
    await rec.start_async_components()

    # 第一段
    await _step(rec, 1, 1000, extra={"sub_device": {"device_sn": "SN_DRONE"}})
    await _step(rec, 1, 1500)
    await _step(rec, 5, 2000)                       # debounce=0 → FINALIZING
    assert rec._detector.state == FlightState.FINALIZING
    dir1 = await rec.finalize_and_close("task_idle")
    await rec.reset_for_next_flight()
    assert rec.flight_dir is None
    assert rec._detector.state == FlightState.WAITING_TASK

    # 第二段
    await _step(rec, 1, 5000)
    await _step(rec, 1, 5500)
    await _step(rec, 5, 6000)
    dir2 = await rec.finalize_and_close("task_idle")

    assert dir1.exists() and dir2.exists()
    assert dir1 != dir2
    assert (dir1 / "manifest.json").exists()
    assert (dir2 / "manifest.json").exists()


@pytest.mark.asyncio
async def test_reset_clears_state(tmp_path):
    rec = Recorder(_cfg(tmp_path), dock_sn="SN_DOCK", drone_sn=None)
    await rec.start_async_components()
    await _step(rec, 1, 1000, extra={"sub_device": {"device_sn": "SN_DRONE"}})
    await _step(rec, 5, 2000)
    await rec.finalize_and_close("task_idle")
    await rec.reset_for_next_flight()
    assert rec.flight_dir is None
    assert rec._manifest is None
    assert rec._queues == {}
    assert rec._writers == {}


async def _dock_osd(rec, ts, **data):
    await rec.on_mqtt_message("thing/product/SN_DOCK/osd",
                              json.dumps({"data": data}).encode(), ts)


@pytest.mark.asyncio
async def test_manual_flight_drone_out_of_dock_makes_flight_dir(tmp_path):
    rec = Recorder(_cfg(tmp_path), dock_sn="SN_DOCK", drone_sn=None)
    await rec.start_async_components()
    await _dock_osd(rec, 1000, flighttask_step_code=5, drone_in_dock=1,
                    sub_device={"device_sn": "SN_DRONE"})
    assert rec.flight_dir is None
    await _dock_osd(rec, 2000, drone_in_dock=0)
    assert rec._detector.state == FlightState.RECORDING
    await _dock_osd(rec, 3000, drone_in_dock=1)       # debounce=0 → FINALIZING
    assert rec._detector.state == FlightState.FINALIZING
    d = await rec.finalize_and_close("task_idle")
    assert (d / "manifest.json").exists()


@pytest.mark.asyncio
async def test_record_when_drone_out_of_dock_config_passthrough(tmp_path):
    cfg = _cfg(tmp_path)
    cfg["flight_detection"]["record_when_drone_out_of_dock"] = False
    rec = Recorder(cfg, dock_sn="SN_DOCK", drone_sn=None)
    assert rec._detector.record_when_drone_out_of_dock is False
    assert Recorder(_cfg(tmp_path), dock_sn="SN_DOCK",
                    drone_sn=None)._detector.record_when_drone_out_of_dock is True


def _read_topic_records(flight_dir, suffix):
    out = []
    for f in sorted((flight_dir / "topics").glob(f"*__{suffix}.*.jsonl")):
        out += [json.loads(l) for l in f.read_text().splitlines() if l.strip()]
    return out


@pytest.mark.asyncio
async def test_non_json_payload_recorded_as_b64_while_recording(tmp_path):
    # 2026-09-28 生产：drc/up / 飞行器 state 有 non-JSON 负载，旧实现直接丢弃。
    import base64
    rec = Recorder(_cfg(tmp_path), dock_sn="SN_DOCK", drone_sn=None)
    await rec.start_async_components()
    raw = b"\x08\x01\xffnot-json"
    # 未录制时 non-JSON 不落盘也不崩
    await rec.on_mqtt_message("thing/product/SN_DOCK/drc/up", raw, 500)
    await _step(rec, 1, 1000, extra={"sub_device": {"device_sn": "SN_DRONE"}})
    await rec.on_mqtt_message("thing/product/SN_DOCK/drc/up", raw, 1100)
    await rec.on_mqtt_message("thing/product/SN_DRONE/state", raw, 1200)
    assert rec._detector.state == FlightState.RECORDING   # non-JSON 不影响起停
    await _step(rec, 5, 2000)
    d = await rec.finalize_and_close("task_idle")
    for suffix in ("drc__up", "state"):
        recs = _read_topic_records(d, suffix)
        assert len(recs) == 1, suffix
        assert recs[0]["payload"] is None
        assert recs[0]["dji_ts_ms"] is None
        assert base64.b64decode(recs[0]["payload_b64"]) == raw
