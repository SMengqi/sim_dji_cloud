import asyncio
from pathlib import Path

import pytest

from sim_dji_cloud.recorder.write_queue import QueueFullPolicy, TopicWriteQueue
from sim_dji_cloud.storage.rotation import RotatingJsonlWriter


@pytest.mark.asyncio
async def test_write_queue_drains_to_rotating_writer(tmp_path: Path):
    writer = RotatingJsonlWriter(
        base_path=tmp_path / "topic",
        rotate_max_records=10**6,
        rotate_max_bytes=10**9,
        flush_max_records=1,
    )
    q = TopicWriteQueue(
        writer=writer,
        flush_max_records=100,
        flush_interval_ms=50,
        queue_max_size=1000,
    )
    await q.start()
    for i in range(5):
        await q.put({"recv_ts_ms": i, "i": i})
    await q.drain_and_close()

    files = sorted(tmp_path.glob("topic.*.jsonl"))
    assert len(files) == 1
    lines = files[0].read_text().splitlines()
    assert len(lines) == 5


@pytest.mark.asyncio
async def test_write_queue_periodic_flush(tmp_path: Path):
    """即使没到 flush_max_records，flush_interval_ms 到也应刷盘"""
    writer = RotatingJsonlWriter(
        base_path=tmp_path / "topic",
        rotate_max_records=10**6,
        rotate_max_bytes=10**9,
        flush_max_records=1,
    )
    q = TopicWriteQueue(
        writer=writer,
        flush_max_records=10000,
        flush_interval_ms=50,
        queue_max_size=1000,
    )
    await q.start()
    await q.put({"recv_ts_ms": 1, "i": 1})
    await asyncio.sleep(0.2)
    files = sorted(tmp_path.glob("topic.*.jsonl"))
    assert files and files[0].read_text().count("\n") >= 1
    await q.drain_and_close()


@pytest.mark.asyncio
async def test_write_queue_full_drops_with_warn(tmp_path: Path):
    """queue 满时按 DROP_NEW 策略丢弃 + 计数"""
    writer = RotatingJsonlWriter(
        base_path=tmp_path / "topic",
        rotate_max_records=10**6,
        rotate_max_bytes=10**9,
        flush_max_records=10000,
    )
    q = TopicWriteQueue(
        writer=writer,
        flush_max_records=10000,
        flush_interval_ms=10000,
        queue_max_size=2,
        full_policy=QueueFullPolicy.DROP_NEW,
    )
    await q.start()
    q._paused = True
    await q.put({"i": 1})
    await q.put({"i": 2})
    dropped = await q.put({"i": 3})
    assert dropped is True
    assert q.dropped_count == 1
    q._paused = False
    await q.drain_and_close()
