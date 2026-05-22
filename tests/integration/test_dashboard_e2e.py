import asyncio
import json
import pytest
import gmqtt
from fastapi.testclient import TestClient

from sim_dji_cloud.dashboard import LiveState, MqttSubscriber, create_app


@pytest.mark.asyncio
async def test_dashboard_receives_real_broker_messages(mosquitto_broker):
    """起 mosquitto + 起 dashboard subscriber + 发几条假 osd + 验证 snapshot。"""
    state = LiveState()
    sub = MqttSubscriber(state, host="127.0.0.1", port=mosquitto_broker,
                         client_id="dash-e2e-sub")
    await sub.connect()
    await asyncio.sleep(0.3)

    pub = gmqtt.Client("dash-e2e-pub")
    await pub.connect("127.0.0.1", mosquitto_broker)

    pub.publish(
        "thing/product/SN_DRONE_E2E/osd",
        json.dumps({
            "data": {
                "mode_code": 5,
                "battery": {"capacity_percent": 75},
                "latitude": 30.5, "longitude": 121.5,
                "horizontal_speed": 2.0,
            },
        }),
    )
    pub.publish(
        "thing/product/SN_DOCK_E2E/osd",
        json.dumps({
            "data": {
                "mode_code": 4,
                "environment_temperature": 25.0,
                "humidity": 80,
                "sub_device": {"device_sn": "SN_DRONE_E2E"},
            },
        }),
    )
    pub.publish(
        "thing/product/SN_DOCK_E2E/events",
        json.dumps({"method": "return_home_info", "data": {"flight_id": "T-E2E"}}),
    )

    await asyncio.sleep(0.5)
    await pub.disconnect()
    await sub.disconnect()

    app = create_app(state)
    client = TestClient(app)
    r = client.get("/api/snapshot")
    body = r.json()

    assert body["drone"]["sn"] == "SN_DRONE_E2E"
    assert body["drone"]["mode_code"] == 5
    assert body["drone"]["battery_pct"] == 75
    assert body["dock"]["sn"] == "SN_DOCK_E2E"
    assert body["dock"]["temperature"] == 25.0
    assert body["dock"]["paired_drone_sn"] == "SN_DRONE_E2E"
    assert len(body["events"]) >= 1
    assert any(e["method"] == "return_home_info" for e in body["events"])
