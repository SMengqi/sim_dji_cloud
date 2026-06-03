from fastapi.testclient import TestClient

from sim_dji_cloud.dashboard.api import create_app
from sim_dji_cloud.dashboard.events_archive import EventsArchive
from sim_dji_cloud.dashboard.live_state import LiveState


def _client_with_state_and_archive(tmp_path):
    archive = EventsArchive(soft_cap=100)
    state = LiveState(on_flight_idle=[archive.reset])
    app = create_app(state=state, archive=archive, recordings_root=tmp_path)
    return TestClient(app), state, archive


def test_state_reset_503_no_env(tmp_path, monkeypatch):
    monkeypatch.delenv("DASHBOARD_TOKEN", raising=False)
    client, _, _ = _client_with_state_and_archive(tmp_path)
    r = client.post("/api/state/reset", json={})
    assert r.status_code == 503
    assert "DASHBOARD_TOKEN" in r.json()["detail"]


def test_state_reset_401_no_token(tmp_path, monkeypatch):
    monkeypatch.setenv("DASHBOARD_TOKEN", "secret123")
    client, _, _ = _client_with_state_and_archive(tmp_path)
    r = client.post("/api/state/reset", json={})
    assert r.status_code == 401


def test_state_reset_200_clears_state_and_archive(tmp_path, monkeypatch):
    monkeypatch.setenv("DASHBOARD_TOKEN", "secret123")
    client, state, archive = _client_with_state_and_archive(tmp_path)

    state.update(
        topic="thing/product/SN_DOCK/osd",
        payload={"data": {"flighttask_step_code": 1,
                          "sub_device": {"device_sn": "SN_DRONE"}}},
        recv_ts_ms=1000,
    )
    archive.append("thing/product/SN_DOCK/events",
                   {"method": "alarm"}, recv_ts_ms=2000)
    assert state.snapshot()["dock"]
    assert archive.query()[0]

    r = client.post(
        "/api/state/reset",
        json={},
        headers={"Authorization": "Bearer secret123"},
    )
    assert r.status_code == 200
    assert r.json() == {"state": "reset"}

    assert state.snapshot()["dock"] == {}
    assert state.snapshot()["drone_trail"] == []
    assert archive.query()[0] == []
