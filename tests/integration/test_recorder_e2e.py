import asyncio
import json
from pathlib import Path
import pytest
import gmqtt
from sim_dji_cloud.config import load_config
from sim_dji_cloud.recorder import Recorder
from sim_dji_cloud.recorder.mqtt_client import MqttRecorderClient, MqttConfig


@pytest.mark.asyncio
async def test_record_one_flight_end_to_end(mosquitto_broker, tmp_path: Path):
    fixture = Path(__file__).parent.parent / "fixtures" / "recorder_minimal.yaml"
    cfg = load_config(fixture)
    cfg["storage"]["root"] = str(tmp_path / "rec")
    cfg["mqtt"]["port"] = mosquitto_broker

    rec = Recorder(cfg, dock_sn="SN_DOCK_TEST", drone_sn=None)
    await rec.start_async_components()

    async def on_msg(topic: str, payload: bytes, recv_ts_ms: int) -> None:
        await rec.on_mqtt_message(topic, payload, recv_ts_ms)

    m = cfg["mqtt"]
    rcli = MqttRecorderClient(
        MqttConfig(
            host=m["host"], port=m["port"], tls=False,
            client_id="rec-test",
            username=None, password=None,
            ca_file=None, cert_file=None, key_file=None,
            subscribe_patterns=m["subscribe_patterns"],
        ),
        on_message=on_msg,
    )
    loop_task = asyncio.create_task(rcli.run_forever())
    await asyncio.sleep(0.5)

    pub = gmqtt.Client("fake-cloud")
    await pub.connect(m["host"], m["port"])

    pub.publish("thing/product/SN_DOCK_TEST/services",
                json.dumps({"method": "wayline_prepare", "data": {"flight_id": "T-E2E-1"}}))
    await asyncio.sleep(0.1)
    pub.publish("thing/product/SN_DOCK_TEST/osd",
                json.dumps({"wayline_mission_state": "executing", "timestamp": 1000}))
    await asyncio.sleep(0.1)
    pub.publish("thing/product/SN_DRONE_TEST/osd",
                json.dumps({"mode_code": "manual_flight", "timestamp": 1001}))
    await asyncio.sleep(0.1)
    for i in range(20):
        pub.publish("thing/product/SN_DOCK_TEST/drc/up",
                    json.dumps({"hsi": i, "timestamp": 2000 + i}))
    await asyncio.sleep(0.5)

    pub.publish("thing/product/SN_DOCK_TEST/osd",
                json.dumps({"wayline_mission_state": "idle", "timestamp": 3000}))
    await asyncio.sleep(2.0)

    await rcli.stop()
    loop_task.cancel()
    await pub.disconnect()

    assert rec.flight_dir is not None
    flight_dir = await rec.finalize_and_close(finalize_reason="auto_idle")

    manifest = json.loads((flight_dir / "manifest.json").read_text())
    assert manifest["task_id"] == "T-E2E-1"
    assert manifest["dock_sn"] == "SN_DOCK_TEST"
    assert manifest["drone_sn"] == "SN_DRONE_TEST"
    topic_set = {t["topic"] for t in manifest["topics"]}
    assert "thing/product/SN_DOCK_TEST/services" in topic_set
    assert "thing/product/SN_DOCK_TEST/osd" in topic_set
    assert "thing/product/SN_DRONE_TEST/osd" in topic_set
    assert "thing/product/SN_DOCK_TEST/drc/up" in topic_set

    drone_osd = flight_dir / "topics" / "thing__product__SN_DRONE_TEST__osd.0001.jsonl"
    assert drone_osd.exists()
    lines = drone_osd.read_text().strip().splitlines()
    assert len(lines) >= 1
    row = json.loads(lines[0])
    assert row["topic"] == "thing/product/SN_DRONE_TEST/osd"
    assert row["payload"]["mode_code"] == "manual_flight"
    assert row["dji_ts_ms"] == 1001
