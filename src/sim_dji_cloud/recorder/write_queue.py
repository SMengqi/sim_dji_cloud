import asyncio
from enum import Enum
from typing import Any

from loguru import logger

from sim_dji_cloud.storage.rotation import RotatingJsonlWriter


class QueueFullPolicy(str, Enum):
    DROP_NEW = "drop_new"
    BLOCK = "block"


class TopicWriteQueue:
    """单 topic 异步写入队列；consumer 协程批量 flush 到 RotatingJsonlWriter。"""

    def __init__(
        self,
        writer: RotatingJsonlWriter,
        flush_max_records: int,
        flush_interval_ms: int,
        queue_max_size: int,
        full_policy: QueueFullPolicy = QueueFullPolicy.DROP_NEW,
    ):
        self._writer = writer
        self._flush_max_records = flush_max_records
        self._flush_interval = flush_interval_ms / 1000.0
        self._queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=queue_max_size)
        self._full_policy = full_policy
        self._consumer_task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._paused = False
        self.dropped_count = 0

    async def start(self) -> None:
        self._consumer_task = asyncio.create_task(self._consume())

    async def put(self, record: dict[str, Any]) -> bool:
        """返回 True 表示被丢弃。"""
        if self._full_policy == QueueFullPolicy.BLOCK:
            await self._queue.put(record)
            return False
        try:
            self._queue.put_nowait(record)
            return False
        except asyncio.QueueFull:
            self.dropped_count += 1
            logger.warning(
                "write queue full, dropping (total dropped: {})", self.dropped_count
            )
            return True

    async def _consume(self) -> None:
        buffer: list[dict[str, Any]] = []
        loop = asyncio.get_running_loop()
        last_flush = loop.time()
        try:
            while not self._stop.is_set():
                if self._paused:
                    await asyncio.sleep(0.01)
                    continue
                try:
                    timeout = max(0.001, self._flush_interval - (loop.time() - last_flush))
                    rec = await asyncio.wait_for(self._queue.get(), timeout=timeout)
                    buffer.append(rec)
                except asyncio.TimeoutError:
                    pass

                should_flush = (
                    len(buffer) >= self._flush_max_records
                    or (loop.time() - last_flush) >= self._flush_interval
                )
                if should_flush and buffer:
                    for r in buffer:
                        self._writer.write(r)
                    buffer.clear()
                    last_flush = loop.time()

            # post-loop drain: handles items enqueued after stop was set but
            # before the consumer woke up from wait_for
            while not self._queue.empty():
                buffer.append(self._queue.get_nowait())
            for r in buffer:
                self._writer.write(r)
        except Exception:
            logger.exception("TopicWriteQueue consumer crashed; remaining items may be lost")
            raise

    async def drain_and_close(self) -> None:
        self._stop.set()
        try:
            if self._consumer_task is not None:
                await self._consumer_task
        finally:
            self._writer.close()
