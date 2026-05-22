import asyncio
import json
import pytest
import gmqtt
from pathlib import Path
from sim_dji_cloud.player import Player
from sim_dji_cloud.player.mqtt_publisher import MqttPublisher


@pytest.mark.asyncio
async def test_player_publishes_to_real_broker(mosquitto_broker, tmp_path: Path):
    flight = tmp_path / "T-E2E__SN_DOCK_E2E__20260521-000000"
    (flight / "topics").mkdir(parents=True)
    (flight / "topics" / "thing__product__SN_DOCK_E2E__osd.0001.jsonl").write_text(
        '{"recv_ts_ms":0,"dji_ts_ms":0,"direction":"up","topic":"thing/product/SN_DOCK_E2E/osd","payload":{"a":1}}\n'
        '{"recv_ts_ms":500,"dji_ts_ms":500,"direction":"up","topic":"thing/product/SN_DOCK_E2E/osd","payload":{"a":2}}\n'
    )
    manifest = {
        "schema_version": 1, "status": "ok", "finalize_reason": "auto_idle",
        "task_id": "T-E2E", "dock_sn": "SN_DOCK_E2E", "drone_sn": "unknown",
        "started_at_recv_ms": 0, "ended_at_recv_ms": 500,
        "gaps": [], "video": None,
        "topics": [{
            "topic": "thing/product/SN_DOCK_E2E/osd",
            "device_sn": "SN_DOCK_E2E", "direction": "up",
            "count": 2, "first_recv_ts_ms": 0, "last_recv_ts_ms": 500,
            "files": [{"name": "topics/thing__product__SN_DOCK_E2E__osd.0001.jsonl",
                       "count": 2, "first_ms": 0, "last_ms": 500}],
        }],
    }
    (flight / "manifest.json").write_text(json.dumps(manifest))

    received: list = []
    sub = gmqtt.Client("e2e-sub")

    def on_msg(client, topic, payload, qos, properties):
        received.append((topic, payload))
        return 0
    sub.on_message = on_msg
    await sub.connect("127.0.0.1", mosquitto_broker)
    sub.subscribe("thing/product/+/osd", qos=1)
    await asyncio.sleep(0.3)

    publisher = MqttPublisher(
        host="127.0.0.1", port=mosquitto_broker, client_id="e2e-player",
    )
    player = Player(flight_dir=flight, publisher=publisher, speed=10.0)
    await player.start()
    await player.wait_until_done()

    await asyncio.sleep(0.3)
    await sub.disconnect()

    assert len(received) == 2
    topics = {t for t, _ in received}
    assert "thing/product/SN_DOCK_E2E/osd" in topics
