import json
from typing import Optional
import gmqtt
from loguru import logger

from sim_dji_cloud.dashboard.live_state import LiveState
from sim_dji_cloud.utils.time_ms import now_ms


# Level B：只订 osd 与 events，避开 DRC 高频流和 services 噪音
DEFAULT_PATTERNS = [
    "thing/product/+/osd",
    "thing/product/+/events",
]


class MqttSubscriber:
    """gmqtt 客户端 → 解码 JSON → 喂给 LiveState。"""

    def __init__(
        self,
        state: LiveState,
        host: str,
        port: int,
        client_id: str,
        subscribe_patterns: Optional[list[str]] = None,
    ):
        self._state = state
        self._host = host
        self._port = port
        self._patterns = subscribe_patterns or DEFAULT_PATTERNS
        self._client = gmqtt.Client(client_id)
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message_internal
        self._client.on_disconnect = self._on_disconnect

    def _on_connect(self, client, flags, rc, properties):
        logger.info("dashboard subscriber connected rc={}", rc)
        for pattern in self._patterns:
            self._client.subscribe(pattern, qos=0)

    def _on_disconnect(self, client, packet, exc=None):
        logger.warning("dashboard subscriber disconnected exc={}", exc)

    async def _on_message_internal(self, client, topic, payload, qos, properties):
        try:
            payload_obj = json.loads(payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            payload_obj = {}
        try:
            self._state.update(topic, payload_obj, now_ms())
        except Exception:
            logger.exception("dashboard state update failed for {}", topic)
        return 0

    async def connect(self) -> None:
        await self._client.connect(self._host, self._port)

    async def disconnect(self) -> None:
        try:
            await self._client.disconnect()
        except Exception:
            pass
