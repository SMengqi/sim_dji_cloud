from typing import Union
import gmqtt


class MqttPublisher:
    """gmqtt 客户端的极简发布端封装。阶段二回放只需 publish，不订阅。"""

    def __init__(self, host: str, port: int, client_id: str):
        self.host = host
        self.port = port
        self._client = gmqtt.Client(client_id)

    async def connect(self) -> None:
        await self._client.connect(self.host, self.port)

    async def publish(self, topic: str, payload: Union[bytes, str], qos: int = 0) -> None:
        if isinstance(payload, str):
            payload = payload.encode("utf-8")
        self._client.publish(topic, payload, qos=qos)

    async def disconnect(self) -> None:
        try:
            await self._client.disconnect()
        except Exception:
            pass
