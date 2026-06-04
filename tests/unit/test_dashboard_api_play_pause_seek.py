from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from sim_dji_cloud.dashboard.api import create_app
from sim_dji_cloud.dashboard.live_state import LiveState
from sim_dji_cloud.dashboard.play_controller import (
    PlayController, NotRunning, ControlUnavailable,
)


def _client(tmp_path, pc=None):
    state = LiveState()
    pc = pc or PlayController(recordings_root=tmp_path, log_dir=tmp_path)
    app = create_app(state=state, play_controller=pc, recordings_root=tmp_path)
    return TestClient(app), pc


def test_pause_503_no_env(tmp_path, monkeypatch):
    monkeypatch.delenv("DASHBOARD_TOKEN", raising=False)
    client, _ = _client(tmp_path)
    r = client.post("/api/play/pause", json={})
    assert r.status_code == 503
    assert "DASHBOARD_TOKEN" in r.json()["detail"]


def test_pause_401_no_token(tmp_path, monkeypatch):
    monkeypatch.setenv("DASHBOARD_TOKEN", "secret")
    client, _ = _client(tmp_path)
    r = client.post("/api/play/pause", json={})
    assert r.status_code == 401


def test_pause_404_when_play_not_running(tmp_path, monkeypatch):
    monkeypatch.setenv("DASHBOARD_TOKEN", "secret")
    pc = MagicMock(spec=PlayController)
    pc.pause.side_effect = NotRunning()
    client, _ = _client(tmp_path, pc=pc)
    r = client.post(
        "/api/play/pause", json={},
        headers={"Authorization": "Bearer secret"},
    )
    assert r.status_code == 404
    assert "play not running" in r.json()["detail"]


def test_pause_503_when_control_unavailable(tmp_path, monkeypatch):
    monkeypatch.setenv("DASHBOARD_TOKEN", "secret")
    pc = MagicMock(spec=PlayController)
    pc.pause.side_effect = ControlUnavailable("sidecar missing")
    client, _ = _client(tmp_path, pc=pc)
    r = client.post(
        "/api/play/pause", json={},
        headers={"Authorization": "Bearer secret"},
    )
    assert r.status_code == 503


def test_seek_400_negative_virt_ms(tmp_path, monkeypatch):
    monkeypatch.setenv("DASHBOARD_TOKEN", "secret")
    pc = MagicMock(spec=PlayController)
    pc.seek.side_effect = ValueError("virt_ms must be int >= 0")
    client, _ = _client(tmp_path, pc=pc)
    r = client.post(
        "/api/play/seek", json={"virt_ms": -1},
        headers={"Authorization": "Bearer secret"},
    )
    assert r.status_code == 400


def test_pause_409_already_paused(tmp_path, monkeypatch):
    """RuntimeError('already paused') from PlayController.pause -> 409."""
    monkeypatch.setenv("DASHBOARD_TOKEN", "secret")
    pc = MagicMock(spec=PlayController)
    pc.pause.side_effect = RuntimeError("already paused")
    client, _ = _client(tmp_path, pc=pc)
    r = client.post(
        "/api/play/pause", json={},
        headers={"Authorization": "Bearer secret"},
    )
    assert r.status_code == 409
    assert "already paused" in r.json()["detail"]


def test_resume_409_not_paused(tmp_path, monkeypatch):
    """RuntimeError('not paused') from PlayController.resume -> 409."""
    monkeypatch.setenv("DASHBOARD_TOKEN", "secret")
    pc = MagicMock(spec=PlayController)
    pc.resume.side_effect = RuntimeError("not paused")
    client, _ = _client(tmp_path, pc=pc)
    r = client.post(
        "/api/play/resume", json={},
        headers={"Authorization": "Bearer secret"},
    )
    assert r.status_code == 409
    assert "not paused" in r.json()["detail"]
