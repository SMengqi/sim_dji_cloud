from fastapi.testclient import TestClient
from sim_dji_cloud.dashboard.live_state import LiveState
from sim_dji_cloud.dashboard.api import create_app


def test_health_endpoint():
    state = LiveState()
    app = create_app(state)
    client = TestClient(app)
    r = client.get("/api/health")
    assert r.status_code == 200
    assert r.json() == {"ok": True}


def test_snapshot_returns_live_state():
    state = LiveState()
    state.update("thing/product/SN_DRONE/osd", {
        "data": {"mode_code": 5, "battery": {"capacity_percent": 60},
                 "latitude": 30.0, "longitude": 121.0},
    }, recv_ts_ms=1000)

    app = create_app(state)
    client = TestClient(app)
    r = client.get("/api/snapshot")
    assert r.status_code == 200
    body = r.json()
    assert body["drone"]["mode_code"] == 5
    assert body["drone"]["battery_pct"] == 60
    assert body["drone_trail"] == [[30.0, 121.0]]
    assert body["topic_counts"]["thing/product/SN_DRONE/osd"] == 1


def test_root_serves_html():
    """GET / 应返回单文件 HTML（content-type text/html）。
    static/index.html 当前不存在，应返回占位页（仍然是 HTML）。"""
    state = LiveState()
    app = create_app(state)
    client = TestClient(app)
    r = client.get("/")
    assert r.status_code == 200
    assert "text/html" in r.headers["content-type"].lower()
    assert "<html" in r.text.lower() or "<!doctype" in r.text.lower()


def test_websocket_pushes_snapshot():
    """WS 连上后应至少收到一次 snapshot 帧。"""
    state = LiveState()
    state.update("thing/product/SN_DRONE/osd",
                 {"data": {"mode_code": 7, "battery": {"capacity_percent": 80}}},
                 recv_ts_ms=1000)
    app = create_app(state, ws_push_interval_ms=50)
    client = TestClient(app)
    with client.websocket_connect("/ws/stream") as ws:
        frame = ws.receive_json()
        assert "drone" in frame
        assert frame["drone"]["mode_code"] == 7
