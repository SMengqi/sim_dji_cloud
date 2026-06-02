import json
from typing import Optional
import gmqtt
from loguru import logger

from sim_dji_cloud.dashboard.live_state import LiveState
from sim_dji_cloud.utils.time_ms import now_ms


# Dashboard 订阅集合：
#   osd       —— dock/drone 周期上报，driver of the state panels & trail
#   events    —— dock 异步事件，左侧"最近事件"面板
#   drc/down  —— 飞控下行（如 stick_control，30Hz 上限），右侧"控制消息"面板
#   services —— 航线/任务等服务下行（如 takeoff、return_home），同右侧面板
# 注意 drc/down 在真实飞行中是高频流，LiveState 用独立 controls_ring 限长。
DEFAULT_PATTERNS = [
    "thing/product/+/osd",
    "thing/product/+/events",
    "thing/product/+/drc/down",
    "thing/product/+/services",
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
        archive=None,
    ):
        self._state = state
        self._host = host
        self._port = port
        self._patterns = subscribe_patterns or DEFAULT_PATTERNS
        self._archive = archive
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
            recv_ts_ms = now_ms()
            self._state.update(topic, payload_obj, recv_ts_ms)
            if self._archive is not None:
                self._archive.append(topic, payload_obj, recv_ts_ms)
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
