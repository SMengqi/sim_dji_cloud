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


# ---------------------------------------------------------------------------
# Regression: ./run.sh stop play-video 发 SIGTERM，play_flight 必须把 video
# pusher 也停掉，否则 ffmpeg 子进程会被孤儿化继续推流。
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_play_flight_sigterm_stops_video_pusher(tmp_path, monkeypatch):
    """复现并固化 fix：上游发 SIGTERM 时，play_flight 走 wait_task.cancel()
    路径，让 Player.wait_until_done() 的 finally 跑到，
    VideoPusher.stop() 被调用（→ 真实场景里给 ffmpeg 发 SIGINT 收尾），
    不会留下孤儿 ffmpeg 继续推 RTMP。
    """
    import asyncio
    import signal as _signal
    import os
    from sim_dji_cloud.tools import play_cmd
    from sim_dji_cloud.player import Player as RealPlayer

    # 1. publisher 的 publish 永远 block：让"飞行"一直进行中、不会自然结束，
    #    从而把测试的退出路径强制走"收到信号 → cancel"那条分支。
    class BlockingPublisher:
        def __init__(self, **kw): pass
        async def connect(self): pass
        async def disconnect(self): self.disconnected = True
        async def publish(self, topic, payload, qos=0):
            await asyncio.Event().wait()   # 永久阻塞，直到被 cancel

    monkeypatch.setattr(play_cmd, "MqttPublisher", BlockingPublisher)

    # 2. 注入 fake VideoPusher，记录 stop() 是否被调到。
    fake_pusher = FakeVideoPusher(
        source_file=tmp_path / "video" / "main.mp4",
        push_url="rtmp://srs/live/x",
    )
    def factory(src, url, extra=None):
        return fake_pusher

    # play_cmd 内部用默认 VideoPusher 构造 Player，没有暴露 factory，
    # 这里 patch Player.__init__ 把 factory 注进去。
    orig_init = RealPlayer.__init__
    def patched_init(self, *args, **kwargs):
        kwargs.setdefault("video_pusher_factory", factory)
        orig_init(self, *args, **kwargs)
    monkeypatch.setattr(RealPlayer, "__init__", patched_init)

    # 3. 准备一个含视频的飞行目录。
    flight = _flight(tmp_path, with_video=True)

    # 4. 起 play_flight；它内部会 add_signal_handler(SIGTERM, stop_event.set)。
    play_task = asyncio.create_task(play_cmd.play_flight(
        flight_dir=flight, mqtt_url="tcp://localhost:1883",
        speed=1.0, start_offset_ms=0, video_push_url="rtmp://srs/live/x",
    ))

    # 给 play_flight 一点时间起 publish 任务 + video push 任务（让 pusher
    # 真的被 start 了，否则下面 stopped 断言意义就弱了）。
    await asyncio.sleep(0.1)
    assert fake_pusher.is_alive(), "前置条件：pusher 已 start，正模拟运行中"

    # 5. 模拟 ./run.sh stop play-video → SIGTERM。
    os.kill(os.getpid(), _signal.SIGTERM)

    # 6. play_flight 必须在合理时间里返回 0 且 pusher.stop() 已调到。
    result = await asyncio.wait_for(play_task, timeout=5.0)
    assert result == 0
    assert fake_pusher.stopped, (
        "SIGTERM 收到后必须把 VideoPusher.stop() 调到，"
        "否则真实场景里 ffmpeg 子进程会被孤儿化继续推流"
    )
