import asyncio
import json
import pytest
import gmqtt
from fastapi.testclient import TestClient

from sim_dji_cloud.dashboard import LiveState, MqttSubscriber, create_app
from sim_dji_cloud.dashboard.events_archive import EventsArchive


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
    assert body["dock"]["environment_temperature"] == 25.0
    assert body["dock"]["paired_drone_sn"] == "SN_DRONE_E2E"
    assert len(body["events"]) >= 1
    assert any(e["method"] == "return_home_info" for e in body["events"])


@pytest.mark.asyncio
async def test_dashboard_archive_clears_on_real_idle_marker(
    mosquitto_broker, tmp_path,
):
    """端到端：真 mosquitto + 真 dashboard app；
    publish events → /api/timeline 有数据 → publish dock idle → /api/timeline 空。
    复用现有 mosquitto_broker fixture（conftest.py）。"""
    archive = EventsArchive()
    state = LiveState(on_flight_idle=[archive.reset])
    sub = MqttSubscriber(
        state,
        host="127.0.0.1",
        port=mosquitto_broker,
        client_id="arch-e2e-sub",
        archive=archive,
    )
    await sub.connect()
    await asyncio.sleep(0.3)

    pub = gmqtt.Client("arch-e2e-pub")
    await pub.connect("127.0.0.1", mosquitto_broker)

    # Warmup: publish a dock OSD with sub_device to establish _known_dock_sn
    # (otherwise the later flighttask_step_code=5 OSD will be routed to _update_drone
    # instead of _update_dock, and the idle listener won't fire)
    pub.publish(
        "thing/product/SN_DOCK_ARCH/osd",
        json.dumps({"data": {"flighttask_step_code": 1,
                              "sub_device": {"device_sn": "SN_DRONE_ARCH"}}}),
    )
    await asyncio.sleep(0.3)

    # Publish an event — should land in archive
    pub.publish(
        "thing/product/SN_DOCK_ARCH/events",
        json.dumps({"method": "alarm"}),
    )
    await asyncio.sleep(0.5)

    client = TestClient(
        create_app(state=state, archive=archive, recordings_root=tmp_path)
    )
    r = client.get("/api/timeline")
    assert r.status_code == 200
    assert len(r.json()["entries"]) >= 1, "archive should have the alarm event"

    # Publish dock OSD with flighttask_step_code=5 (idle) → triggers archive.reset()
    pub.publish(
        "thing/product/SN_DOCK_ARCH/osd",
        json.dumps({"data": {"flighttask_step_code": 5}}),
    )
    await asyncio.sleep(0.5)

    r = client.get("/api/timeline")
    assert r.status_code == 200
    assert r.json()["entries"] == [], "archive should be cleared after idle marker"

    await pub.disconnect()
    await sub.disconnect()


@pytest.mark.asyncio
async def test_play_start_stop_status_e2e(mosquitto_broker, tmp_path, monkeypatch):
    """端到端：起 mosquitto + dashboard app；POST /api/play/start 起 play
    子进程；GET /api/play/status 看到 running；POST /api/play/stop 收尾；
    GET 再看到 stopped。

    复用 mosquitto_broker fixture (tests/integration/conftest.py)。
    play 子进程会真起一个 sim-dji play，需要 fixture flight_dir。
    用 tests/fixtures/real_flight_basic/ (220s 真机 fixture)。
    """
    from pathlib import Path
    from sim_dji_cloud.dashboard.play_controller import PlayController

    monkeypatch.setenv("DASHBOARD_TOKEN", "test_token_e2e")

    fixture_path = Path(__file__).parent.parent / "fixtures" / "real_flight_basic"
    if not fixture_path.exists():
        pytest.skip("real_flight_basic fixture missing")
    recordings_root = fixture_path.parent

    pc = PlayController(recordings_root=recordings_root, log_dir=tmp_path / "logs")
    state = LiveState()
    app = create_app(state=state, play_controller=pc, recordings_root=recordings_root)
    client = TestClient(app)

    # 初始：stopped
    r = client.get("/api/play/status")
    assert r.status_code == 200
    assert r.json()["state"] == "stopped"

    # POST start
    r = client.post(
        "/api/play/start",
        json={
            "flight_dir": "real_flight_basic",
            "mqtt_url": f"tcp://127.0.0.1:{mosquitto_broker}",
            "speed": 100.0,
        },
        headers={"Authorization": "Bearer test_token_e2e"},
    )
    assert r.status_code == 201, r.text
    pid = r.json()["pid"]

    await asyncio.sleep(0.5)

    # GET status: running
    r = client.get("/api/play/status")
    assert r.json()["state"] == "running"
    assert r.json()["pid"] == pid

    # POST stop
    r = client.post(
        "/api/play/stop",
        headers={"Authorization": "Bearer test_token_e2e"},
    )
    assert r.status_code == 200
    assert r.json()["state"] == "stopped"

    # GET status: stopped
    r = client.get("/api/play/status")
    assert r.json()["state"] == "stopped"
