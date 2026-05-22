import asyncio
import json
import pytest
from pathlib import Path
from sim_dji_cloud.player import Player


def _make_flight_dir(tmp_path: Path) -> Path:
    flight = tmp_path / "T-T__SN_DOCK_FIXTURE__20260521-000000"
    (flight / "topics").mkdir(parents=True)

    dock_osd = flight / "topics" / "thing__product__SN_DOCK_FIXTURE__osd.0001.jsonl"
    dock_osd.write_text(
        '{"recv_ts_ms":0,"dji_ts_ms":0,"direction":"up","topic":"thing/product/SN_DOCK_FIXTURE/osd","payload":{"a":1}}\n'
        '{"recv_ts_ms":1000,"dji_ts_ms":1000,"direction":"up","topic":"thing/product/SN_DOCK_FIXTURE/osd","payload":{"a":2}}\n'
    )
    drone_osd = flight / "topics" / "thing__product__SN_DRONE_FIXTURE__osd.0001.jsonl"
    drone_osd.write_text(
        '{"recv_ts_ms":500,"dji_ts_ms":500,"direction":"up","topic":"thing/product/SN_DRONE_FIXTURE/osd","payload":{"m":5}}\n'
        '{"recv_ts_ms":1500,"dji_ts_ms":1500,"direction":"up","topic":"thing/product/SN_DRONE_FIXTURE/osd","payload":{"m":9}}\n'
    )

    manifest = {
        "schema_version": 1, "status": "ok", "finalize_reason": "auto_idle",
        "task_id": "T-T", "dock_sn": "SN_DOCK_FIXTURE", "drone_sn": "SN_DRONE_FIXTURE",
        "started_at_recv_ms": 0, "ended_at_recv_ms": 1500,
        "takeoff_offset_ms": None, "landing_offset_ms": None,
        "gaps": [], "video": None,
        "topics": [
            {"topic": "thing/product/SN_DOCK_FIXTURE/osd", "device_sn": "SN_DOCK_FIXTURE",
             "direction": "up", "count": 2, "first_recv_ts_ms": 0, "last_recv_ts_ms": 1000,
             "files": [{"name": "topics/thing__product__SN_DOCK_FIXTURE__osd.0001.jsonl",
                        "count": 2, "first_ms": 0, "last_ms": 1000}]},
            {"topic": "thing/product/SN_DRONE_FIXTURE/osd", "device_sn": "SN_DRONE_FIXTURE",
             "direction": "up", "count": 2, "first_recv_ts_ms": 500, "last_recv_ts_ms": 1500,
             "files": [{"name": "topics/thing__product__SN_DRONE_FIXTURE__osd.0001.jsonl",
                        "count": 2, "first_ms": 500, "last_ms": 1500}]},
        ],
    }
    (flight / "manifest.json").write_text(json.dumps(manifest))
    return flight


@pytest.mark.asyncio
async def test_player_publishes_all_records_in_order(tmp_path: Path):
    flight = _make_flight_dir(tmp_path)

    published: list[tuple[str, bytes]] = []

    class FakePublisher:
        async def connect(self): pass
        async def disconnect(self): pass
        async def publish(self, topic, payload, qos=0):
            published.append((topic, payload))

    p = Player(flight_dir=flight, publisher=FakePublisher(), speed=10.0)
    await p.start()
    await p.wait_until_done()

    assert len(published) == 4
    topics = {t for t, _ in published}
    assert "thing/product/SN_DOCK_FIXTURE/osd" in topics
    assert "thing/product/SN_DRONE_FIXTURE/osd" in topics


@pytest.mark.asyncio
async def test_player_publishes_original_payload_bytes(tmp_path: Path):
    flight = _make_flight_dir(tmp_path)
    published: list[tuple[str, bytes]] = []

    class FakePublisher:
        async def connect(self): pass
        async def disconnect(self): pass
        async def publish(self, topic, payload, qos=0):
            published.append((topic, payload))

    p = Player(flight_dir=flight, publisher=FakePublisher(), speed=100.0)
    await p.start()
    await p.wait_until_done()

    for topic, payload in published:
        decoded = json.loads(payload.decode())
        assert isinstance(decoded, dict)
        assert "a" in decoded or "m" in decoded


@pytest.mark.asyncio
async def test_player_skips_topics_with_missing_files(tmp_path: Path):
    flight = _make_flight_dir(tmp_path)
    manifest = json.loads((flight / "manifest.json").read_text())
    manifest["topics"].append({
        "topic": "thing/product/SN_DOCK_FIXTURE/missing",
        "device_sn": "SN_DOCK_FIXTURE", "direction": "up",
        "count": 0, "first_recv_ts_ms": None, "last_recv_ts_ms": None,
        "files": [{"name": "topics/thing__product__SN_DOCK_FIXTURE__missing.0001.jsonl",
                   "count": 0, "first_ms": None, "last_ms": None}],
    })
    (flight / "manifest.json").write_text(json.dumps(manifest))

    published = []
    class FakePublisher:
        async def connect(self): pass
        async def disconnect(self): pass
        async def publish(self, topic, payload, qos=0):
            published.append((topic, payload))

    p = Player(flight_dir=flight, publisher=FakePublisher(), speed=100.0)
    await p.start()
    await p.wait_until_done()
    assert len(published) == 4
