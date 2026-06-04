import asyncio
import json
import time
from pathlib import Path

import pytest

from sim_dji_cloud.player import Player


class _FakePublisher:
    def __init__(self):
        self.published = []
        self.connected = False
        self.disconnected = False

    async def connect(self) -> None:
        self.connected = True

    async def publish(self, topic: str, payload: bytes, qos: int = 0) -> None:
        self.published.append((topic, int(time.monotonic() * 1000),
                               payload.decode("utf-8")))

    async def disconnect(self) -> None:
        self.disconnected = True


def _make_flight(tmp_path: Path, recv_ts_list: list[int]) -> Path:
    flight = tmp_path / "flight_test"
    flight.mkdir()
    topics = flight / "topics"
    topics.mkdir()
    jsonl_path = topics / "thing__product__SN_DOCK__osd.0000.jsonl"
    lines = []
    for ts in recv_ts_list:
        lines.append(json.dumps({
            "recv_ts_ms": ts, "dji_ts_ms": ts, "direction": "in",
            "topic": "thing/product/SN_DOCK/osd",
            "payload": {"data": {"seq": ts}},
        }))
    jsonl_path.write_text("\n".join(lines) + "\n")
    manifest = {
        "schema_version": 1, "status": "ok", "task_id": "T",
        "dock_sn": "SN_DOCK", "drone_sn": "SN_DRONE",
        "started_at_recv_ms": recv_ts_list[0],
        "ended_at_recv_ms": recv_ts_list[-1],
        "gaps": [],
        "topics": [{
            "topic": "thing/product/SN_DOCK/osd",
            "files": [{"name": "topics/thing__product__SN_DOCK__osd.0000.jsonl"}],
        }],
    }
    (flight / "manifest.json").write_text(json.dumps(manifest))
    return flight


@pytest.mark.asyncio
async def test_player_pause_then_resume_publishes_in_order(tmp_path):
    flight = _make_flight(tmp_path, [1000, 1100, 1200, 1300])
    pub = _FakePublisher()
    p = Player(flight_dir=flight, publisher=pub, speed=10.0)
    await p.start()
    await asyncio.sleep(0.05)
    res = await p.pause()
    assert res["state"] == "paused"
    count_at_pause = len(pub.published)
    await asyncio.sleep(0.2)
    assert len(pub.published) == count_at_pause, (
        "pause 后又发了新消息: "
        f"{[m[0] for m in pub.published[count_at_pause:]]}"
    )
    res = await p.resume()
    assert res["state"] == "running"
    for t in p._tasks:
        try:
            await asyncio.wait_for(t, timeout=2.0)
        except asyncio.TimeoutError:
            t.cancel()
    assert len(pub.published) == 4
    await p.wait_until_done()


@pytest.mark.asyncio
async def test_player_seek_skips_to_target_virt(tmp_path):
    started = 1000
    flight = _make_flight(tmp_path, [started, started + 100, started + 200,
                                     started + 300])
    pub = _FakePublisher()
    p = Player(flight_dir=flight, publisher=pub, speed=10.0)
    await p.start()
    res = await p.seek(200)
    assert res["virt_ms"] == 200
    for t in p._tasks:
        try:
            await asyncio.wait_for(t, timeout=2.0)
        except asyncio.TimeoutError:
            t.cancel()
    seqs = [json.loads(m[2])["data"]["seq"] for m in pub.published]
    assert all(s >= 1200 or s == 1000 for s in seqs), seqs
    assert started + 200 in seqs
    assert started + 300 in seqs
    await p.wait_until_done()


@pytest.mark.asyncio
async def test_player_seek_paused_does_not_restart_video(tmp_path, monkeypatch):
    """Seek-while-paused should NOT restart ffmpeg video task; resume does.
    Uses monkeypatch on plan_video_push to make video plan always non-None,
    so the guard branch is genuinely exercised."""
    flight = _make_flight(tmp_path, [1000, 1100, 1200])

    # Force plan_video_push to return a non-None plan so the guard is exercised
    import sim_dji_cloud.player as player_mod
    monkeypatch.setattr(
        player_mod, "plan_video_push",
        lambda *a, **kw: {"wait_virt_ms": 0, "ss_seconds": 0.0},
    )
    # Also stub _run_video_push to avoid actually invoking ffmpeg / VideoPusher
    async def _noop_video(self, plan, video_file):
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            raise
    monkeypatch.setattr(Player, "_run_video_push", _noop_video)

    pub = _FakePublisher()
    p = Player(flight_dir=flight, publisher=pub, speed=10.0,
               video_push_url="rtmp://fake/test")
    await p.start()
    # Video task should have spawned (plan returned non-None)
    assert p._video_task is not None
    initial_video_task = p._video_task

    await p.pause()
    # pause should have cancelled video task
    assert p._video_task is None

    await p.seek(50)
    # paused seek MUST NOT restart video task
    assert p._video_task is None, (
        "seek while paused should NOT restart video task"
    )

    await p.resume()
    # resume SHOULD restart video task
    assert p._video_task is not None
    assert p._video_task is not initial_video_task

    # cleanup: cancel everything
    for t in p._tasks:
        t.cancel()
    if p._video_task:
        p._video_task.cancel()
    try:
        await asyncio.gather(*p._tasks, p._video_task, return_exceptions=True)
    except Exception:
        pass
    await p.wait_until_done()


@pytest.mark.asyncio
async def test_player_progress_reflects_state(tmp_path):
    # Use a 1000ms flight so seek(500) lands in the middle, not beyond end.
    flight = _make_flight(tmp_path, [1000, 1100, 1500, 2000])
    pub = _FakePublisher()
    p = Player(flight_dir=flight, publisher=pub, speed=10.0)
    await p.start()
    await asyncio.sleep(0.02)
    prog0 = p.progress()
    assert prog0["paused"] is False
    assert prog0["total_ms"] == 1000   # ended (2000) - started (1000)
    await p.pause()
    prog1 = p.progress()
    assert prog1["paused"] is True
    await asyncio.sleep(0.05)
    prog2 = p.progress()
    assert prog2["virt_ms"] == prog1["virt_ms"], "paused virt drifted"
    await p.resume()
    await p.seek(500)
    prog3 = p.progress()
    assert 490 <= prog3["virt_ms"] <= 510
    for t in p._tasks:
        try:
            await asyncio.wait_for(t, timeout=2.0)
        except asyncio.TimeoutError:
            t.cancel()
    await p.wait_until_done()


@pytest.mark.asyncio
async def test_player_seek_beyond_end_clamps_to_total_minus_one(tmp_path):
    """seek(virt_ms > total_ms) should clamp + warn + return clamped value.
    Design §2 D11 + §4.2 + §7."""
    flight = _make_flight(tmp_path, [1000, 1100, 1200])   # total = 200
    pub = _FakePublisher()
    p = Player(flight_dir=flight, publisher=pub, speed=10.0)
    await p.start()
    res = await p.seek(5000)   # way past end
    assert res["virt_ms"] == 199, (
        f"seek beyond end should clamp to total-1=199; got {res['virt_ms']}"
    )
    for t in p._tasks:
        try:
            await asyncio.wait_for(t, timeout=2.0)
        except asyncio.TimeoutError:
            t.cancel()
    await p.wait_until_done()


@pytest.mark.asyncio
async def test_player_seek_records_fire_at_expected_wall_time(tmp_path):
    """Seek correctness: after seek(S), record at absolute T should publish
    approximately (T - S) / speed wall ms later. Regression test for the
    formula bug where _replay_topic_from_virt double-counted _start_offset_ms,
    making records fire S/speed ms too late at real speed."""
    started = 1000
    # Records at 100ms / 200ms / 300ms offsets from start.
    flight = _make_flight(tmp_path, [started, started + 100,
                                     started + 200, started + 300])
    pub = _FakePublisher()
    # speed=2.0 keeps the test fast but still timing-sensitive
    p = Player(flight_dir=flight, publisher=pub, speed=2.0)
    await p.start()
    # Seek to 200ms in: records at offsets 200, 300 should fire.
    seek_wall_ms = int(time.monotonic() * 1000)
    await p.seek(200)
    # Wait for all post-seek records to flush
    for t in p._tasks:
        try:
            await asyncio.wait_for(t, timeout=2.0)
        except asyncio.TimeoutError:
            t.cancel()

    # _FakePublisher records (topic, wall_ms_when_published, payload_str)
    seqs = [(int(json.loads(m[2])["data"]["seq"]) - started, m[1] - seek_wall_ms)
            for m in pub.published]
    # Filter to post-seek records only (pre-seek 0 / 100 may or may not have
    # already published depending on race; assert only on >= 200 records)
    post_seek = [(t, wall_delay) for t, wall_delay in seqs if t >= 200]
    assert len(post_seek) == 2, f"Expected 2 post-seek records, got {post_seek}"

    # Record at absolute T = 200 should fire ~0 ms after seek
    # Record at absolute T = 300 should fire ~(300-200)/2 = 50 ms after seek
    # Allow generous tolerance for asyncio scheduling jitter.
    for t, wall_delay in post_seek:
        expected_wall = (t - 200) / 2.0
        # The buggy formula would give expected_wall = t / 2.0 (i.e. 100ms / 150ms
        # instead of 0ms / 50ms). 50ms+ extra delay is well outside tolerance.
        assert wall_delay <= expected_wall + 50, (
            f"Record T={t}ms fired {wall_delay}ms after seek; "
            f"expected <= {expected_wall + 50}ms (correct ~{expected_wall}ms, "
            f"buggy ~{t / 2.0}ms)"
        )
    await p.wait_until_done()


@pytest.mark.asyncio
async def test_player_seek_does_not_publish_idle_marker(tmp_path):
    """Regression test for race where Player.seek cancelling tasks made
    Player.wait_until_done's gather return prematurely, triggering its
    finally block which publishes the dashboard-clear synthetic idle OSD
    (flighttask_step_code=5). Symptom (real-machine 2026-06-04): seek
    clicks wiped dashboard state but video kept showing because new ffmpeg
    subprocess was independent of the dead Python player.

    Verify: after start + seek mid-flight, the publisher must NOT have
    received any synthetic idle marker. Only normal record payloads.
    """
    flight = _make_flight(tmp_path, [1000, 1100, 1500, 2000])
    pub = _FakePublisher()
    p = Player(flight_dir=flight, publisher=pub, speed=10.0)
    await p.start()
    await asyncio.sleep(0.02)
    # Seek mid-flight
    await p.seek(500)
    # Let new tasks publish a few records
    await asyncio.sleep(0.1)

    # Check no synthetic idle marker was published
    idle_published = [
        m for m in pub.published
        if '"flighttask_step_code":5' in m[2]
        or '"flighttask_step_code": 5' in m[2]
    ]
    assert not idle_published, (
        f"seek triggered premature wait_until_done finally; idle marker "
        f"published: {idle_published}"
    )

    # Cleanup — let player finish naturally
    for t in p._tasks:
        try:
            await asyncio.wait_for(t, timeout=2.0)
        except asyncio.TimeoutError:
            t.cancel()
    await p.wait_until_done()
    # After wait_until_done, idle marker IS expected (natural finish)
    idle_after_finish = [
        m for m in pub.published
        if '"flighttask_step_code":5' in m[2]
        or '"flighttask_step_code": 5' in m[2]
    ]
    assert idle_after_finish, (
        "wait_until_done finishing naturally should publish idle marker"
    )
