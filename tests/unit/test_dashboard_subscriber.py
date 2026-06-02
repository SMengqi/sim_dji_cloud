import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from sim_dji_cloud.dashboard.live_state import LiveState
from sim_dji_cloud.dashboard.mqtt_subscriber import MqttSubscriber


@pytest.mark.asyncio
async def test_subscriber_connects_and_subscribes():
    state = LiveState()
    fake_client = MagicMock()
    fake_client.connect = AsyncMock()
    fake_client.disconnect = AsyncMock()
    fake_client.subscribe = MagicMock()

    with patch("sim_dji_cloud.dashboard.mqtt_subscriber.gmqtt.Client",
               return_value=fake_client):
        sub = MqttSubscriber(state, host="localhost", port=1883,
                             client_id="dashboard-test")
        await sub.connect()
        # 模拟 on_connect 触发
        sub._on_connect(fake_client, flags=0, rc=0, properties={})

    patterns = [call.args[0] for call in fake_client.subscribe.call_args_list]
    assert "thing/product/+/osd" in patterns
    assert "thing/product/+/events" in patterns


@pytest.mark.asyncio
async def test_subscriber_routes_message_to_live_state():
    import json
    state = LiveState()
    fake_client = MagicMock()
    fake_client.connect = AsyncMock()
    fake_client.disconnect = AsyncMock()
    fake_client.subscribe = MagicMock()

    with patch("sim_dji_cloud.dashboard.mqtt_subscriber.gmqtt.Client",
               return_value=fake_client):
        sub = MqttSubscriber(state, host="localhost", port=1883, client_id="t")
        payload = json.dumps({
            "data": {"mode_code": 5, "latitude": 30.0, "longitude": 121.0,
                     "battery": {"capacity_percent": 60}},
        }).encode("utf-8")
        await sub._on_message_internal(None, "thing/product/SN_DRONE/osd",
                                       payload, 0, {})

    snap = state.snapshot()
    assert snap["drone"]["mode_code"] == 5
    assert snap["drone"]["battery_pct"] == 60
    assert len(snap["drone_trail"]) == 1


@pytest.mark.asyncio
async def test_subscriber_ignores_non_json_payload():
    state = LiveState()
    fake_client = MagicMock()
    fake_client.connect = AsyncMock()
    fake_client.disconnect = AsyncMock()
    fake_client.subscribe = MagicMock()

    with patch("sim_dji_cloud.dashboard.mqtt_subscriber.gmqtt.Client",
               return_value=fake_client):
        sub = MqttSubscriber(state, host="localhost", port=1883, client_id="t")
        await sub._on_message_internal(None, "thing/product/X/osd",
                                       b"not-json", 0, {})
    snap = state.snapshot()
    # 计数仍累加（payload 不可解码就走空 dict）
    assert snap["topic_counts"]["thing/product/X/osd"] == 1


@pytest.mark.asyncio
async def test_subscriber_forwards_to_archive_after_state():
    """收到 MQTT 消息时 state.update 和 archive.append 都被调用，且先 state 后 archive。"""
    import json as _json
    from sim_dji_cloud.dashboard.events_archive import EventsArchive

    call_order: list[str] = []

    class TrackedState(LiveState):
        def update(self, topic, payload, recv_ts_ms):
            call_order.append("state")
            super().update(topic, payload, recv_ts_ms)

    class TrackedArchive(EventsArchive):
        def append(self, topic, payload, recv_ts_ms):
            call_order.append("archive")
            super().append(topic, payload, recv_ts_ms)

    fake_client = MagicMock()
    fake_client.connect = AsyncMock()
    fake_client.disconnect = AsyncMock()
    fake_client.subscribe = MagicMock()

    with patch("sim_dji_cloud.dashboard.mqtt_subscriber.gmqtt.Client",
               return_value=fake_client):
        sub = MqttSubscriber(
            state=TrackedState(),
            archive=TrackedArchive(),
            host="localhost", port=1883,
            client_id="test", subscribe_patterns=["thing/+/+/+"],
        )
        await sub._on_message_internal(
            None,
            topic="thing/product/SN/events",
            payload=_json.dumps({"method": "x"}).encode(),
            qos=0,
            properties={},
        )
    assert call_order == ["state", "archive"]


@pytest.mark.asyncio
async def test_subscriber_with_no_archive_still_works():
    """archive 参数不传时不抛错（向后兼容），消息照常走 state。"""
    import json as _json

    state = LiveState()
    fake_client = MagicMock()
    fake_client.connect = AsyncMock()
    fake_client.disconnect = AsyncMock()
    fake_client.subscribe = MagicMock()

    with patch("sim_dji_cloud.dashboard.mqtt_subscriber.gmqtt.Client",
               return_value=fake_client):
        sub = MqttSubscriber(
            state=state,
            host="localhost", port=1883,
            client_id="test", subscribe_patterns=["thing/+/+/+"],
        )
        await sub._on_message_internal(
            None,
            topic="thing/product/SN_DOCK/osd",
            payload=_json.dumps({"data": {"sub_device": {"device_sn": "SN_DRONE"}}}).encode(),
            qos=0,
            properties={},
        )
    snap = state.snapshot()
    assert snap["dock"].get("sn") == "SN_DOCK"
