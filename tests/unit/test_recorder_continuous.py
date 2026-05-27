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
