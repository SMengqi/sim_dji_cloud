import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from sim_dji_cloud.recorder.mqtt_client import MqttRecorderClient, MqttConfig

@pytest.mark.asyncio
async def test_mqtt_client_subscribes_to_all_patterns_on_connect():
    cfg = MqttConfig(
        host="example.com", port=8883, tls=True,
        client_id="test", username="u", password="p",
        ca_file=None, cert_file=None, key_file=None,
        subscribe_patterns=["thing/product/+/+", "thing/product/+/drc/+"],
    )
    async def on_message(topic, payload, recv_ts_ms):
        pass

    fake_client = MagicMock()
    fake_client.connect = AsyncMock()
    fake_client.disconnect = AsyncMock()
    fake_client.subscribe = MagicMock()
    fake_client.set_auth_credentials = MagicMock()

    with patch("sim_dji_cloud.recorder.mqtt_client.gmqtt.Client", return_value=fake_client):
        c = MqttRecorderClient(cfg, on_message=on_message)
        # Simulate on_connect callback firing
        c._on_connect(fake_client, flags=0, rc=0, properties={})
        topics = [call.args[0] for call in fake_client.subscribe.call_args_list]
        assert "thing/product/+/+" in topics
        assert "thing/product/+/drc/+" in topics

@pytest.mark.asyncio
async def test_mqtt_client_dispatches_message_to_callback():
    cfg = MqttConfig(
        host="example.com", port=1883, tls=False,
        client_id="t", username=None, password=None,
        ca_file=None, cert_file=None, key_file=None,
        subscribe_patterns=["thing/product/+/+"],
    )
    received: list = []
    async def on_message(topic, payload, recv_ts_ms):
        received.append((topic, payload, recv_ts_ms))

    fake_client = MagicMock()
    fake_client.connect = AsyncMock()
    fake_client.disconnect = AsyncMock()
    fake_client.subscribe = MagicMock()
    fake_client.set_auth_credentials = MagicMock()

    with patch("sim_dji_cloud.recorder.mqtt_client.gmqtt.Client", return_value=fake_client):
        c = MqttRecorderClient(cfg, on_message=on_message)
        await c._on_message_internal(None, "thing/product/D1/osd", b'{"x":1}', 0, {})

    assert received[0][0] == "thing/product/D1/osd"
    assert received[0][1] == b'{"x":1}'
    assert isinstance(received[0][2], int)


@pytest.mark.asyncio
async def test_connecting_flag_prevents_double_connect():
    """_connecting 标志阻止 broker 慢响应（CONNACK 未到）时重复 connect。

    Regression: 旧代码无 _connecting 标志，broker 卡时 1s tick 会触发多次
    connect，每次 _on_connect 都 subscribe → QoS=1 重复收消息 → JSONL 双倍。
    """
    cfg = MqttConfig(
        host="slow.example.com", port=1883, tls=False,
        client_id="t", username=None, password=None,
        ca_file=None, cert_file=None, key_file=None,
        subscribe_patterns=["thing/product/+/+"],
    )
    connect_calls = 0

    async def fake_connect(*args, **kwargs):
        nonlocal connect_calls
        connect_calls += 1
        return  # CONNACK 不来 → _on_connect 永远不触发

    fake_client = MagicMock()
    fake_client.connect = fake_connect
    fake_client.disconnect = AsyncMock()
    fake_client.subscribe = MagicMock()
    fake_client.set_auth_credentials = MagicMock()

    async def on_message(topic, payload, recv_ts_ms):
        pass

    with patch("sim_dji_cloud.recorder.mqtt_client.gmqtt.Client",
               return_value=fake_client):
        c = MqttRecorderClient(cfg, on_message=on_message)
        # 直接模拟 run_forever 的 connect 触发逻辑两遍：第二遍若没标志会再调
        for _ in range(3):
            if not c._connected.is_set() and not c._connecting:
                c._connecting = True
                await c.connect()

    assert connect_calls == 1, (
        f"_connecting 标志应阻止重复 connect，实际 {connect_calls} 次"
    )
    # 确认 _on_disconnect 会重置标志，允许真正断线后重连
    c._on_disconnect(fake_client, packet=None, exc=None)
    assert c._connecting is False
    for _ in range(2):
        if not c._connected.is_set() and not c._connecting:
            c._connecting = True
            await c.connect()
    assert connect_calls == 2, "断线后应允许重连一次"
