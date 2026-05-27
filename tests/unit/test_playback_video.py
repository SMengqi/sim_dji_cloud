import json
from pathlib import Path

import pytest

from sim_dji_cloud.player import Player


class FakePublisher:
    async def connect(self): pass
    async def disconnect(self): pass
    async def publish(self, topic, payload, qos=0): pass


class FakeVideoPusher:
    def __init__(self, source_file, push_url, extra_args=None):
        self.source_file = Path(source_file)
        self.push_url = push_url
        self.started_ss = None
        self.stopped = False

    def start(self, ss_seconds=0.0):
        self.started_ss = ss_seconds

    def stop(self, timeout_s=10.0):
        self.stopped = True

    def is_alive(self):
        return self.started_ss is not None and not self.stopped


def _flight(tmp_path: Path, with_video: bool) -> Path:
    """所有记录 recv_ts=0（回放瞬间完成）；视频 offset=0（wait_until_virt(0) 立即返回）。"""
    flight = tmp_path / "T-T__SN_DOCK__20260526-000000"
    (flight / "topics").mkdir(parents=True)
    (flight / "topics" / "thing__product__SN_DOCK__osd.0001.jsonl").write_text(
        '{"recv_ts_ms":0,"dji_ts_ms":0,"direction":"up",'
        '"topic":"thing/product/SN_DOCK/osd","payload":{"a":1}}\n'
    )
    if with_video:
        (flight / "video").mkdir()
        (flight / "video" / "main.mp4").write_bytes(b"x")
    manifest = {
        "schema_version": 1, "status": "ok", "finalize_reason": "auto_idle",
        "task_id": "T-T", "dock_sn": "SN_DOCK", "drone_sn": "SN_DRONE",
        "started_at_recv_ms": 0, "ended_at_recv_ms": 0,
        "takeoff_offset_ms": None, "landing_offset_ms": None, "gaps": [],
        "video": ({"file": "video/main.mp4", "source_url": "rtmp://orig/live/x",
                   "started_at_recv_ms": 0, "duration_ms": 1000,
                   "segments": []} if with_video else None),
        "topics": [
            {"topic": "thing/product/SN_DOCK/osd", "device_sn": "SN_DOCK",
             "direction": "up", "count": 1, "first_recv_ts_ms": 0, "last_recv_ts_ms": 0,
             "files": [{"name": "topics/thing__product__SN_DOCK__osd.0001.jsonl",
                        "count": 1, "first_ms": 0, "last_ms": 0}]},
        ],
    }
    (flight / "manifest.json").write_text(json.dumps(manifest))
    return flight


@pytest.mark.asyncio
async def test_push_started_and_stopped(tmp_path):
    created = []

    def factory(src, url, extra):
        vp = FakeVideoPusher(src, url, extra)
        created.append(vp)
        return vp

    flight = _flight(tmp_path, with_video=True)
    p = Player(flight_dir=flight, publisher=FakePublisher(), speed=1.0,
               video_push_url="rtmp://srs/live/x", video_pusher_factory=factory)
    await p.start()
    await p.wait_until_done()

    assert len(created) == 1
    assert created[0].push_url == "rtmp://srs/live/x"
    assert created[0].source_file == flight / "video" / "main.mp4"
    assert created[0].started_ss == 0.0
    assert created[0].stopped is True


@pytest.mark.asyncio
async def test_no_push_url_no_pusher(tmp_path):
    created = []
    flight = _flight(tmp_path, with_video=True)
    p = Player(flight_dir=flight, publisher=FakePublisher(), speed=1.0,
               video_pusher_factory=lambda *a: created.append(a))
    await p.start()
    await p.wait_until_done()
    assert created == []


@pytest.mark.asyncio
async def test_no_video_no_pusher(tmp_path):
    created = []
    flight = _flight(tmp_path, with_video=False)
    p = Player(flight_dir=flight, publisher=FakePublisher(), speed=1.0,
               video_push_url="rtmp://srs/live/x",
               video_pusher_factory=lambda *a: created.append(a))
    await p.start()
    await p.wait_until_done()
    assert created == []


@pytest.mark.asyncio
async def test_speed_not_1_no_pusher(tmp_path):
    created = []
    flight = _flight(tmp_path, with_video=True)
    p = Player(flight_dir=flight, publisher=FakePublisher(), speed=10.0,
               video_push_url="rtmp://srs/live/x",
               video_pusher_factory=lambda *a: created.append(a))
    await p.start()
    await p.wait_until_done()
    assert created == []


@pytest.mark.asyncio
async def test_push_start_failure_is_non_fatal(tmp_path):
    # pusher.start() 抛异常不能炸：MQTT 重发仍完成，wait_until_done 正常返回
    class BoomPusher(FakeVideoPusher):
        def start(self, ss_seconds=0.0):
            raise RuntimeError("ffmpeg push failed")

    published = []

    class RecordingPublisher:
        async def connect(self): pass
        async def disconnect(self): pass
        async def publish(self, topic, payload, qos=0):
            published.append((topic, payload))

    flight = _flight(tmp_path, with_video=True)
    p = Player(flight_dir=flight, publisher=RecordingPublisher(), speed=1.0,
               video_push_url="rtmp://srs/live/x",
               video_pusher_factory=lambda s, u, e: BoomPusher(s, u, e))
    await p.start()
    await p.wait_until_done()  # 不抛
    assert len(published) == 1  # MQTT 照常重发
