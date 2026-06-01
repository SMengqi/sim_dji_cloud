import asyncio
import json
from pathlib import Path
from typing import Optional, Protocol, Callable
from loguru import logger

from sim_dji_cloud.player.jsonl_iterator import JsonlIterator
from sim_dji_cloud.player.scheduler import VirtTimeScheduler
from sim_dji_cloud.player.video_pusher import VideoPusher, plan_video_push


class _PublisherProto(Protocol):
    async def connect(self) -> None: ...
    async def publish(self, topic: str, payload: bytes, qos: int = 0) -> None: ...
    async def disconnect(self) -> None: ...


class Player:
    """飞行目录回放编排：
    - 加载 manifest，每个 topic 一个 asyncio publisher 协程
    - 协程读 jsonl，按 recv_ts_ms - started_at_recv_ms 等到虚拟时间到再 publish
    - wait_until_done 等所有协程结束
    """

    def __init__(
        self,
        flight_dir: Path,
        publisher: _PublisherProto,
        speed: float = 1.0,
        start_offset_ms: int = 0,
        video_push_url: str | None = None,
        video_pusher_factory: Callable[..., VideoPusher] = VideoPusher,
        video_anchor_offset_ms: int = 0,
    ):
        self.flight_dir = Path(flight_dir)
        self._publisher = publisher
        self._speed = speed
        self._start_offset_ms = start_offset_ms
        self._scheduler = VirtTimeScheduler()
        self._manifest: dict = {}
        self._tasks: list[asyncio.Task] = []
        self._video_push_url = video_push_url
        self._video_pusher_factory = video_pusher_factory
        self._video_anchor_offset_ms = video_anchor_offset_ms
        self._video_pusher: Optional[VideoPusher] = None
        self._video_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self._manifest = json.loads((self.flight_dir / "manifest.json").read_text())
        await self._publisher.connect()

        started_at = self._manifest.get("started_at_recv_ms", 0)
        self._scheduler.start(
            virt_zero_ms=self._start_offset_ms,
            speed=self._speed,
        )

        for topic_entry in self._manifest.get("topics", []):
            files = [self.flight_dir / f["name"] for f in topic_entry.get("files", [])]
            files = [f for f in files if f.exists()]
            if not files:
                logger.warning("topic {} has no readable files, skipping",
                               topic_entry["topic"])
                continue
            task = asyncio.create_task(
                self._replay_topic(topic_entry, files, started_at)
            )
            self._tasks.append(task)

        video_meta = self._manifest.get("video") or {}
        video_rel = video_meta.get("file")  # e.g. "video/main_<ms>.mp4" or "video/main.mp4"
        video_file = (self.flight_dir / video_rel) if video_rel else None
        video_exists = bool(video_file and video_file.exists())
        plan = plan_video_push(
            self._manifest, self._video_push_url, self._speed,
            video_exists,
            anchor_offset_ms=self._video_anchor_offset_ms,
        )
        if plan is not None:
            self._video_task = asyncio.create_task(self._run_video_push(plan, video_file))

    async def _run_video_push(self, plan: dict, video_file: "Path | None") -> None:
        try:
            await self._scheduler.wait_until_virt(plan["wait_virt_ms"])
            pusher = self._video_pusher_factory(
                video_file, self._video_push_url, [])
            pusher.start(plan["ss_seconds"])
            self._video_pusher = pusher
            logger.info("video push started -> {}", self._video_push_url)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("视频推流启动失败；MQTT 回放不受影响")

    async def _replay_topic(
        self,
        topic_entry: dict,
        files: list[Path],
        started_at_ms: int,
    ) -> None:
        topic = topic_entry["topic"]
        for record in JsonlIterator(files):
            recv_ts = record.get("recv_ts_ms")
            if not isinstance(recv_ts, int):
                continue
            virt_target = recv_ts - started_at_ms + self._start_offset_ms
            await self._scheduler.wait_until_virt(virt_target)
            payload = record.get("payload", {})
            payload_bytes = json.dumps(payload, ensure_ascii=False,
                                       separators=(",", ":")).encode("utf-8")
            try:
                await self._publisher.publish(topic, payload_bytes)
            except Exception:
                logger.exception("publish failed for topic {}", topic)

    async def wait_until_done(self) -> None:
        try:
            if self._tasks:
                await asyncio.gather(*self._tasks, return_exceptions=True)
        finally:
            if self._video_task is not None:
                self._video_task.cancel()
                try:
                    await self._video_task
                except (asyncio.CancelledError, Exception):
                    pass
            if self._video_pusher is not None:
                try:
                    self._video_pusher.stop()
                except Exception:
                    logger.exception("停止视频推流失败")
            await self._publisher.disconnect()
