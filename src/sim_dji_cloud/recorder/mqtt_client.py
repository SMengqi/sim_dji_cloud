import asyncio
import ssl
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional

import gmqtt
from loguru import logger

from sim_dji_cloud.utils.time_ms import now_ms

OnMessage = Callable[[str, bytes, int], Awaitable[None]]


@dataclass
class MqttConfig:
    host: str
    port: int
    tls: bool
    client_id: str
    username: Optional[str]
    password: Optional[str]
    ca_file: Optional[str]
    cert_file: Optional[str]
    key_file: Optional[str]
    subscribe_patterns: list[str] = field(default_factory=list)


class MqttRecorderClient:
    """gmqtt 封装：连接 → 订阅 → on_message 派发；带指数退避重连。"""

    def __init__(self, cfg: MqttConfig, on_message: OnMessage):
        self.cfg = cfg
        self._on_message = on_message
        self._client = gmqtt.Client(cfg.client_id)
        if cfg.username:
            self._client.set_auth_credentials(cfg.username, cfg.password)
        self._client.on_message = self._on_message_internal
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._connected = asyncio.Event()
        self._stop = asyncio.Event()
        self._disconnect_at_ms: Optional[int] = None
        # _connecting：connect 已发起但 _on_connect 尚未回调；防止 run_forever
        # 1s tick 期间重复 connect → 双倍 subscribe → QoS=1 重复消息 → JSONL 双倍。
        self._connecting: bool = False
        self.gaps: list[dict] = []

    async def _on_message_internal(self, client, topic, payload, qos, properties):
        try:
            await self._on_message(topic, payload, now_ms())
        except Exception:
            logger.exception("on_message handler failed for topic {}", topic)
        return 0

    def _on_connect(self, client, flags, rc, properties):
        logger.info("mqtt connected rc={}", rc)
        if self._disconnect_at_ms is not None:
            self.gaps.append({
                "reason": "mqtt_disconnect",
                "start_ms": self._disconnect_at_ms,
                "end_ms": now_ms(),
            })
            self._disconnect_at_ms = None
        for pattern in self.cfg.subscribe_patterns:
            self._client.subscribe(pattern, qos=1)
        self._connected.set()
        self._connecting = False

    def _on_disconnect(self, client, packet, exc=None):
        logger.warning("mqtt disconnected exc={}", exc)
        self._disconnect_at_ms = now_ms()
        self._connected.clear()
        self._connecting = False

    async def connect(self) -> None:
        ssl_ctx: Optional[ssl.SSLContext] = None
        if self.cfg.tls:
            ssl_ctx = ssl.create_default_context()
            if self.cfg.ca_file:
                ssl_ctx.load_verify_locations(self.cfg.ca_file)
            if self.cfg.cert_file and self.cfg.key_file:
                ssl_ctx.load_cert_chain(self.cfg.cert_file, self.cfg.key_file)
        await self._client.connect(
            self.cfg.host, self.cfg.port,
            ssl=ssl_ctx, version=gmqtt.constants.MQTTv50,
        )

    async def run_forever(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                # 仅当未连接且无 in-flight connect 时才发起；防止 broker 慢响应
                # 时 1s tick 重复 connect → _on_connect 多次触发 → 重复 subscribe
                # → QoS=1 收到重复消息 → JSONL 双倍。
                # Regression: test_run_forever_does_not_double_connect.
                if not self._connected.is_set() and not self._connecting:
                    self._connecting = True
                    try:
                        await self.connect()
                    except Exception:
                        self._connecting = False
                        raise
                    backoff = 1.0
                await asyncio.wait_for(self._stop.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except Exception:
                logger.exception("mqtt loop error; retrying in {}s", backoff)
                await asyncio.sleep(backoff)
                backoff = min(60.0, backoff * 2)

    async def stop(self) -> None:
        self._stop.set()
        try:
            await self._client.disconnect()
        except Exception:
            pass
