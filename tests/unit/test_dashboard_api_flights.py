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
