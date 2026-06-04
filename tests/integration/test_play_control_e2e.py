import asyncio
import json
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest


@pytest.mark.asyncio
async def test_play_pause_resume_seek_e2e(mosquitto_broker, tmp_path):
    """真起 mosquitto + sim-dji play + control sidecar；通过 HTTP 调
    pause → status freeze → resume → status advance → seek → status jump。"""
    recordings = tmp_path / "recordings"
    flight = recordings / "flight_T"
    (flight / "topics").mkdir(parents=True)
    started = int(time.time() * 1000)
    ended = started + 5000
    jsonl = flight / "topics" / "thing__product__SN_DOCK__osd.0000.jsonl"
    lines = []
    for i in range(50):
        ts = started + i * 100
        lines.append(json.dumps({
            "recv_ts_ms": ts, "dji_ts_ms": ts, "direction": "in",
            "topic": "thing/product/SN_DOCK/osd",
            "payload": {"data": {"seq": i}},
        }))
    jsonl.write_text("\n".join(lines) + "\n")
    (flight / "manifest.json").write_text(json.dumps({
        "schema_version": 1, "status": "ok", "task_id": "T",
        "dock_sn": "SN_DOCK", "drone_sn": "SN_DRONE",
        "started_at_recv_ms": started, "ended_at_recv_ms": ended,
        "gaps": [],
        "topics": [{
            "topic": "thing/product/SN_DOCK/osd",
            "files": [{"name": "topics/thing__product__SN_DOCK__osd.0000.jsonl"}],
        }],
    }))

    sidecar = tmp_path / "play.control.json"
    log_path = tmp_path / "play.log"
    proc = subprocess.Popen([
        sys.executable, "-m", "sim_dji_cloud.cli", "play",
        str(flight),
        "--mqtt-url", f"tcp://localhost:{mosquitto_broker}",
        "--speed", "1.0",
        "--control-sidecar-path", str(sidecar),
    ], stdout=open(log_path, "ab"),
       stderr=subprocess.STDOUT, start_new_session=True)

    try:
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            if sidecar.exists():
                break
            await asyncio.sleep(0.1)
        else:
            raise RuntimeError(f"control sidecar never appeared; "
                               f"log={log_path.read_text()}")

        port = json.loads(sidecar.read_text())["control_port"]
        base = f"http://127.0.0.1:{port}"

        async with httpx.AsyncClient(base_url=base, timeout=5.0) as c:
            await asyncio.sleep(0.5)
            p1 = (await c.get("/control/progress")).json()
            assert p1["paused"] is False
            assert p1["virt_ms"] > 0
            assert p1["total_ms"] == 5000

            r = await c.post("/control/pause", json={})
            assert r.status_code == 200
            p2 = (await c.get("/control/progress")).json()
            assert p2["paused"] is True
            await asyncio.sleep(0.3)
            p3 = (await c.get("/control/progress")).json()
            assert p3["virt_ms"] == p2["virt_ms"], "paused virt drifted"

            r = await c.post("/control/resume", json={})
            assert r.status_code == 200
            await asyncio.sleep(0.2)
            p4 = (await c.get("/control/progress")).json()
            assert p4["paused"] is False
            assert p4["virt_ms"] > p3["virt_ms"]

            r = await c.post("/control/seek", json={"virt_ms": 3000})
            assert r.status_code == 200
            p5 = (await c.get("/control/progress")).json()
            assert 2900 <= p5["virt_ms"] <= 3200
    finally:
        proc.send_signal(15)   # SIGTERM
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
