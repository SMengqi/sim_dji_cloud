import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from sim_dji_cloud.player.mqtt_publisher import MqttPublisher


@pytest.mark.asyncio
async def test_publish_calls_gmqtt_client():
    fake_client = MagicMock()
    fake_client.connect = AsyncMock()
    fake_client.disconnect = AsyncMock()
    fake_client.publish = MagicMock()

    with patch("sim_dji_cloud.player.mqtt_publisher.gmqtt.Client", return_value=fake_client):
        p = MqttPublisher(host="127.0.0.1", port=11883, client_id="player-test")
        await p.connect()
        await p.publish("test/topic", b'{"x":1}')
        await p.disconnect()

    args = fake_client.publish.call_args
    assert args.args[0] == "test/topic"
    assert args.args[1] == b'{"x":1}'


@pytest.mark.asyncio
async def test_publish_accepts_string_payload():
    fake_client = MagicMock()
    fake_client.connect = AsyncMock()
    fake_client.disconnect = AsyncMock()
    fake_client.publish = MagicMock()

    with patch("sim_dji_cloud.player.mqtt_publisher.gmqtt.Client", return_value=fake_client):
        p = MqttPublisher(host="127.0.0.1", port=11883, client_id="t")
        await p.connect()
        await p.publish("test/topic", '{"x":1}')
        await p.disconnect()

    args = fake_client.publish.call_args
    assert isinstance(args.args[1], bytes)
    assert args.args[1] == b'{"x":1}'
