import json
from pathlib import Path

from fastapi.testclient import TestClient

from sim_dji_cloud.dashboard.api import create_app
from sim_dji_cloud.dashboard.live_state import LiveState


def _make_flight(root: Path, name: str, *, started_ms=None, ended_ms=None,
                 has_video=False, dock_sn="SN_TEST",
                 missing_manifest=False, corrupt_manifest=False):
    """构造一个 flight_dir + manifest（可控字段）。"""
    flight = root / name
    flight.mkdir()
    if missing_manifest:
        return flight
    m = {
        "schema_version": 1,
        "status": "ok",
        "task_id": "T-T",
        "dock_sn": dock_sn,
        "drone_sn": "SN_DRONE",
        "started_at_recv_ms": started_ms,
        "ended_at_recv_ms": ended_ms,
        "gaps": [], "topics": [],
    }
    if has_video:
        m["video"] = {"file": "video/main_xxx.mp4"}
    if corrupt_manifest:
        (flight / "manifest.json").write_text("{not json")
    else:
        (flight / "manifest.json").write_text(json.dumps(m))
    return flight


def _client(tmp_path):
    state = LiveState()
    app = create_app(state=state, recordings_root=tmp_path)
    return TestClient(app)


def test_get_flights_returns_empty_when_root_missing(tmp_path):
    nonexistent = tmp_path / "nope"
    state = LiveState()
    app = create_app(state=state, recordings_root=nonexistent)
    client = TestClient(app)
    r = client.get("/api/flights")
    assert r.status_code == 200
    assert r.json() == {"flights": []}


def test_get_flights_returns_empty_when_root_empty(tmp_path):
    client = _client(tmp_path)
    r = client.get("/api/flights")
    assert r.status_code == 200
    assert r.json() == {"flights": []}


def test_get_flights_returns_standard_fields(tmp_path):
    _make_flight(tmp_path, "flight_A",
                 started_ms=1780455600000, ended_ms=1780456330000,
                 has_video=True, dock_sn="DOCK_A")
    client = _client(tmp_path)
    r = client.get("/api/flights")
    assert r.status_code == 200
    flights = r.json()["flights"]
    assert len(flights) == 1
    f = flights[0]
    assert f["id"] == "flight_A"
    assert f["started_at_ms"] == 1780455600000
    assert f["duration_ms"] == 730000
    assert f["has_video"] is True
    assert f["dock_sn"] == "DOCK_A"


def test_get_flights_sorted_by_started_at_desc(tmp_path):
    _make_flight(tmp_path, "flight_OLD",
                 started_ms=1780440000000, ended_ms=1780440100000)
    _make_flight(tmp_path, "flight_NEW",
                 started_ms=1780466400000, ended_ms=1780466500000)
    _make_flight(tmp_path, "flight_MID",
                 started_ms=1780455600000, ended_ms=1780455700000)
    client = _client(tmp_path)
    r = client.get("/api/flights")
    ids = [f["id"] for f in r.json()["flights"]]
    assert ids == ["flight_NEW", "flight_MID", "flight_OLD"]


def test_get_flights_skips_missing_manifest(tmp_path):
    _make_flight(tmp_path, "flight_A",
                 started_ms=100, ended_ms=200)
    _make_flight(tmp_path, "flight_BAD", missing_manifest=True)
    client = _client(tmp_path)
    r = client.get("/api/flights")
    ids = [f["id"] for f in r.json()["flights"]]
    assert ids == ["flight_A"]


def test_get_flights_skips_corrupt_manifest(tmp_path):
    _make_flight(tmp_path, "flight_A",
                 started_ms=100, ended_ms=200)
    _make_flight(tmp_path, "flight_BAD", corrupt_manifest=True)
    client = _client(tmp_path)
    r = client.get("/api/flights")
    ids = [f["id"] for f in r.json()["flights"]]
    assert ids == ["flight_A"]


def test_get_flights_skips_hidden_and_non_dir(tmp_path):
    _make_flight(tmp_path, "flight_A",
                 started_ms=100, ended_ms=200)
    (tmp_path / ".git").mkdir()
    (tmp_path / "README.txt").write_text("not a flight")
    client = _client(tmp_path)
    r = client.get("/api/flights")
    ids = [f["id"] for f in r.json()["flights"]]
    assert ids == ["flight_A"]


def test_get_flights_cache_hits_within_ttl(tmp_path):
    """缓存有效期内第二次 GET 不应触发 disk scan（结果跟首次一致即使盘上新增）。

    Regression (review MAJOR): _scan_flights 每次 GET 都全盘扫；大目录 / NFS 慢盘下
    占资源。加 TTL 缓存后短窗口内重复 GET 返同一份快照。
    """
    _make_flight(tmp_path, "flight_A",
                 started_ms=100, ended_ms=200)
    state = LiveState()
    app = create_app(state=state, recordings_root=tmp_path,
                     flights_cache_ttl_s=60.0)    # 长 TTL 让测试看到缓存
    client = TestClient(app)

    r1 = client.get("/api/flights")
    ids1 = [f["id"] for f in r1.json()["flights"]]
    assert ids1 == ["flight_A"]

    # 在缓存有效期内新增飞行 — 不应进结果
    _make_flight(tmp_path, "flight_B_added_late",
                 started_ms=300, ended_ms=400)
    r2 = client.get("/api/flights")
    ids2 = [f["id"] for f in r2.json()["flights"]]
    assert ids2 == ["flight_A"], (
        f"缓存有效期内新增飞行不应可见，实际看到 {ids2}"
    )


def test_get_flights_cache_expires_after_ttl(tmp_path):
    """TTL 过后下一次 GET 重新扫盘，能看到新增的飞行。"""
    _make_flight(tmp_path, "flight_A",
                 started_ms=100, ended_ms=200)
    state = LiveState()
    app = create_app(state=state, recordings_root=tmp_path,
                     flights_cache_ttl_s=0.0)    # TTL=0 每次都过期
    client = TestClient(app)

    r1 = client.get("/api/flights")
    assert [f["id"] for f in r1.json()["flights"]] == ["flight_A"]

    _make_flight(tmp_path, "flight_B",
                 started_ms=300, ended_ms=400)
    r2 = client.get("/api/flights")
    ids2 = {f["id"] for f in r2.json()["flights"]}
    assert ids2 == {"flight_A", "flight_B"}, (
        f"TTL 过后新飞行应当出现，实际 {ids2}"
    )
