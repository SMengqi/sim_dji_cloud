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
