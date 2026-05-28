import base64
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


# 1x1 透明 PNG，用于测试 PNG 端点
_PNG_1X1 = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="
)


def _sample_flight_area():
    return {
        "png_bounds": [[0.0, 123.0], [0.001, 123.001]],
        "areas": {"type": "FeatureCollection", "features": [
            {"type": "Feature",
             "geometry": {"type": "Polygon",
                          "coordinates": [[[123.0, 0.0], [123.001, 0.0], [123.0, 0.001], [123.0, 0.0]]]},
             "properties": {"id": 1000, "kind": "restriction", "height_limit": 120, "name": "禁飞区A"}},
        ]},
    }


def test_flight_area_unconfigured():
    app = create_app(LiveState())
    client = TestClient(app)
    r = client.get("/api/flight-area")
    assert r.status_code == 200
    assert r.json() == {"configured": False}
    # PNG 未配置 -> 404
    assert client.get("/api/flight-area/background.png").status_code == 404


def test_flight_area_configured(tmp_path):
    png = tmp_path / "bg.png"
    png.write_bytes(_PNG_1X1)
    app = create_app(LiveState(), flight_area=_sample_flight_area(), flight_area_png=png)
    client = TestClient(app)

    r = client.get("/api/flight-area")
    assert r.status_code == 200
    body = r.json()
    assert body["configured"] is True
    assert body["png_url"] == "/api/flight-area/background.png"
    assert body["png_bounds"] == [[0.0, 123.0], [0.001, 123.001]]
    assert body["areas"]["features"][0]["properties"]["kind"] == "restriction"

    img = client.get("/api/flight-area/background.png")
    assert img.status_code == 200
    assert "image/png" in img.headers["content-type"]
    assert img.headers["cache-control"] == "public, max-age=86400"
    assert img.content == _PNG_1X1


def test_flight_area_configured_without_png():
    # 提供了区域但没给 PNG：不应广告 png_url，PNG 端点仍 404
    app = create_app(LiveState(), flight_area=_sample_flight_area(), flight_area_png=None)
    client = TestClient(app)
    body = client.get("/api/flight-area").json()
    assert body["configured"] is True
    assert "png_url" not in body
    assert client.get("/api/flight-area/background.png").status_code == 404


def test_video_unconfigured():
    app = create_app(LiveState())
    client = TestClient(app)
    r = client.get("/api/video")
    assert r.status_code == 200
    assert r.json() == {"configured": False}


def test_video_configured():
    url = "http://10.0.0.5:8080/live/livestream.flv"
    app = create_app(LiveState(), video_url=url)
    client = TestClient(app)
    r = client.get("/api/video")
    assert r.status_code == 200
    assert r.json() == {"configured": True, "url": url, "type": "flv"}


def test_static_serves_mpegts():
    app = create_app(LiveState())
    client = TestClient(app)
    r = client.get("/static/mpegts.min.js")
    assert r.status_code == 200
    assert "javascript" in r.headers["content-type"].lower()
    assert "createPlayer" in r.text


def test_dashboard_help_shows_video_url():
    from click.testing import CliRunner
    from sim_dji_cloud.cli import main
    res = CliRunner().invoke(main, ["dashboard", "--help"])
    assert res.exit_code == 0
    assert "--video-url" in res.output


def test_index_html_embeds_video_player():
    app = create_app(LiveState())
    client = TestClient(app)
    html = client.get("/").text
    assert "/static/mpegts.min.js" in html
    assert "mpegts.createPlayer" in html
    assert 'class="video-panel"' in html
    assert "/api/video" in html
    assert "loadVideo" in html
