import json
import socket
from pathlib import Path

import httpx
import pytest

from sim_dji_cloud.player.control_server import ControlServer


class _FakePlayer:
    def __init__(self):
        self.pause_calls = 0
        self.resume_calls = 0
        self.seek_calls = []
        self.pause_raises: Exception | None = None
        self.resume_raises: Exception | None = None
        self.seek_response = {"state": "running", "virt_ms": 0}
        self.progress_response = {
            "virt_ms": 0, "total_ms": 100, "paused": False, "speed": 1.0,
        }

    async def pause(self) -> dict:
        self.pause_calls += 1
        if self.pause_raises:
            raise self.pause_raises
        return {"state": "paused", "virt_ms": 42}

    async def resume(self) -> dict:
        self.resume_calls += 1
        if self.resume_raises:
            raise self.resume_raises
        return {"state": "running", "virt_ms": 42}

    async def seek(self, virt_ms: int) -> dict:
        self.seek_calls.append(virt_ms)
        return {**self.seek_response, "virt_ms": virt_ms}

    def progress(self) -> dict:
        return self.progress_response


@pytest.mark.asyncio
async def test_control_server_starts_and_writes_sidecar(tmp_path):
    sidecar = tmp_path / "play.control.json"
    s = ControlServer(_FakePlayer(), sidecar, pid=999, started_at_ms=1780000000000)
    await s.start()
    try:
        assert sidecar.exists()
        data = json.loads(sidecar.read_text())
        assert data["control_port"] == s.port
        assert data["pid"] == 999
        assert data["started_at_ms"] == 1780000000000
        with socket.create_connection(("127.0.0.1", s.port), timeout=1.0):
            pass
    finally:
        await s.stop()


@pytest.mark.asyncio
async def test_control_server_pause_calls_player_pause(tmp_path):
    fake = _FakePlayer()
    s = ControlServer(fake, tmp_path / "x.json", pid=1, started_at_ms=0)
    await s.start()
    try:
        async with httpx.AsyncClient() as c:
            r = await c.post(f"http://127.0.0.1:{s.port}/control/pause", json={})
        assert r.status_code == 200
        assert r.json() == {"state": "paused", "virt_ms": 42}
        assert fake.pause_calls == 1

        fake.pause_raises = RuntimeError("already paused")
        async with httpx.AsyncClient() as c:
            r = await c.post(f"http://127.0.0.1:{s.port}/control/pause", json={})
        assert r.status_code == 409
        assert "already paused" in r.json()["detail"]
    finally:
        await s.stop()


@pytest.mark.asyncio
async def test_control_server_seek_validates_virt_ms(tmp_path):
    fake = _FakePlayer()
    s = ControlServer(fake, tmp_path / "x.json", pid=1, started_at_ms=0)
    await s.start()
    try:
        async with httpx.AsyncClient() as c:
            r = await c.post(f"http://127.0.0.1:{s.port}/control/seek",
                             json={"virt_ms": -1})
        assert r.status_code == 400

        async with httpx.AsyncClient() as c:
            r = await c.post(f"http://127.0.0.1:{s.port}/control/seek",
                             json={"virt_ms": "foo"})
        assert r.status_code == 400

        async with httpx.AsyncClient() as c:
            r = await c.post(f"http://127.0.0.1:{s.port}/control/seek",
                             content="not json",
                             headers={"Content-Type": "application/json"})
        assert r.status_code == 400

        async with httpx.AsyncClient() as c:
            r = await c.post(f"http://127.0.0.1:{s.port}/control/seek",
                             json={"virt_ms": 1000})
        assert r.status_code == 200
        assert fake.seek_calls == [1000]
    finally:
        await s.stop()


@pytest.mark.asyncio
async def test_control_server_progress_returns_player_progress(tmp_path):
    fake = _FakePlayer()
    fake.progress_response = {
        "virt_ms": 425000, "total_ms": 730000,
        "paused": False, "speed": 1.5,
    }
    s = ControlServer(fake, tmp_path / "x.json", pid=1, started_at_ms=0)
    await s.start()
    try:
        async with httpx.AsyncClient() as c:
            r = await c.get(f"http://127.0.0.1:{s.port}/control/progress")
        assert r.status_code == 200
        assert r.json() == fake.progress_response
    finally:
        await s.stop()


@pytest.mark.asyncio
async def test_control_server_stop_cleans_sidecar(tmp_path):
    sidecar = tmp_path / "play.control.json"
    s = ControlServer(_FakePlayer(), sidecar, pid=1, started_at_ms=0)
    await s.start()
    port = s.port
    assert sidecar.exists()
    await s.stop()
    assert not sidecar.exists()
    with pytest.raises((ConnectionRefusedError, OSError)):
        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
            pass
