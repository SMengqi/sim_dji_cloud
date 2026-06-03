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


def test_index_html_has_pip_toggle():
    """Dashboard exposes a Picture-in-Picture toggle: detect support via
    document.pictureInPictureEnabled, provide togglePip()/requestPictureInPicture
    glue, and bind a button gated on video.pipSupported.
    """
    app = create_app(LiveState())
    client = TestClient(app)
    html = client.get("/").text
    assert "pictureInPictureEnabled" in html
    assert "togglePip" in html
    assert "requestPictureInPicture" in html
    assert "video.pipSupported" in html
    # enter/leave event listeners must be wired so OS-level close syncs UI state.
    assert "enterpictureinpicture" in html
    assert "leavepictureinpicture" in html


def test_index_html_drone_card_has_zoom_and_gimbal_rows():
    """Drone card must render zoom_factor + gimbal_pitch + gimbal_yaw rows,
    bound to state.drone.* fields that LiveState populates from
    OSD cameras[].zoom_factor + data.<payload_index>.gimbal_*.
    """
    app = create_app(LiveState())
    client = TestClient(app)
    html = client.get("/").text
    assert "变焦倍数" in html
    assert "云台俯仰角" in html
    assert "云台偏航角" in html
    assert "state.drone.zoom_factor" in html
    assert "state.drone.gimbal_pitch" in html
    assert "state.drone.gimbal_yaw" in html


def test_index_html_handles_empty_trail_by_removing_markers():
    """When backend pushes an empty drone_trail (task went idle), the frontend
    must remove the start + drone markers and clear the polyline, otherwise
    the previous flight's markers stay stuck on the map.
    """
    app = create_app(LiveState())
    client = TestClient(app)
    html = client.get("/").text
    assert "trail.length === 0" in html
    assert "removeLayer(this.startMarker)" in html
    assert "removeLayer(this.droneMarker)" in html


def test_index_html_events_panel_is_split_and_renders_controls():
    """Events strip is split: left = events, right = control messages (drc/down
    + services). Both render from state.controls / state.events.
    """
    app = create_app(LiveState())
    client = TestClient(app)
    html = client.get("/").text
    # Two panes
    assert html.count('class="events-pane"') >= 2
    # Pane titles
    assert "最近事件" in html
    assert "控制消息" in html
    # Right pane iterates state.controls (the new ring)
    assert "state.controls" in html
    # Each row is clickable -> openModal
    assert "openModal('event'" in html
    assert "openModal('control'" in html


def test_index_html_has_payload_modal():
    """Click on an event/control row pops a modal showing the full payload
    + a copy button. Backdrop click, ESC, and X close it.
    """
    app = create_app(LiveState())
    client = TestClient(app)
    html = client.get("/").text
    assert "modal-backdrop" in html
    assert "modal.payloadJson" in html
    assert "copyModalToClipboard" in html
    assert "closeModal" in html
    # ESC key must close
    assert "keydown.escape.window" in html
    # Click on backdrop closes (but click inside card does not)
    assert "click.self=\"closeModal()\"" in html


def _client_with_archive(tmp_path):
    """构造带 archive + recordings_root 的 TestClient。"""
    from fastapi.testclient import TestClient
    from sim_dji_cloud.dashboard.api import create_app
    from sim_dji_cloud.dashboard.live_state import LiveState
    from sim_dji_cloud.dashboard.events_archive import EventsArchive

    archive = EventsArchive(soft_cap=10**6)
    state = LiveState()
    app = create_app(state=state, archive=archive, recordings_root=tmp_path)
    return TestClient(app), archive


def test_get_timeline_live_returns_session_and_entries(tmp_path):
    client, archive = _client_with_archive(tmp_path)
    archive.append("thing/product/SN/events",
                   {"method": "alarm"}, recv_ts_ms=1000)
    archive.append("thing/product/SN/drc/down",
                   {"method": "stick"}, recv_ts_ms=1500)
    r = client.get("/api/timeline")
    assert r.status_code == 200
    data = r.json()
    assert data["source"] == "live"
    assert data["session_started_at_ms"] == 1000
    assert [e["method"] for e in data["entries"]] == ["alarm", "stick"]
    assert data["truncated"] is False


def test_get_timeline_kinds_filter_events_only(tmp_path):
    client, archive = _client_with_archive(tmp_path)
    archive.append("thing/product/SN/events", {"method": "e"}, recv_ts_ms=1)
    archive.append("thing/product/SN/drc/down", {"method": "c"}, recv_ts_ms=2)
    r = client.get("/api/timeline?kinds=event")
    assert [e["method"] for e in r.json()["entries"]] == ["e"]


def test_get_timeline_since_ms_filter(tmp_path):
    client, archive = _client_with_archive(tmp_path)
    for ts in [10, 20, 30, 40]:
        archive.append("thing/product/SN/events",
                       {"method": str(ts)}, recv_ts_ms=ts)
    r = client.get("/api/timeline?since_ms=25")
    assert [e["method"] for e in r.json()["entries"]] == ["30", "40"]


def test_get_timeline_limit_truncates_with_flag(tmp_path):
    client, archive = _client_with_archive(tmp_path)
    for ts in range(20):
        archive.append("thing/product/SN/events",
                       {"method": str(ts)}, recv_ts_ms=ts)
    r = client.get("/api/timeline?limit=3")
    data = r.json()
    assert data["truncated"] is True
    assert len(data["entries"]) == 3


def test_get_timeline_offline_source_reads_flight_dir(tmp_path):
    from tests.unit.test_events_archive_from_flight_dir import _build_minimal_flight
    flight = _build_minimal_flight(tmp_path)
    client, _ = _client_with_archive(tmp_path)
    rel = flight.relative_to(tmp_path)
    r = client.get(f"/api/timeline?source={rel}")
    assert r.status_code == 200
    data = r.json()
    assert data["session_started_at_ms"] == 50   # manifest started_at
    methods = [e["method"] for e in data["entries"]]
    assert methods == ["alarm_1", "stick_control", "alarm_2", "wayline_create"]


def test_get_timeline_offline_source_not_found(tmp_path):
    client, _ = _client_with_archive(tmp_path)
    r = client.get("/api/timeline?source=no_such_dir")
    assert r.status_code == 404


def test_get_timeline_offline_source_path_traversal_rejected(tmp_path):
    client, _ = _client_with_archive(tmp_path)
    r = client.get("/api/timeline?source=../etc")
    assert r.status_code == 400


def test_get_timeline_offline_source_outside_root_rejected(tmp_path):
    client, _ = _client_with_archive(tmp_path)
    r = client.get(f"/api/timeline?source=/tmp/anywhere_else_{id(tmp_path)}")
    assert r.status_code == 400


def test_export_csv_header_and_basic_row(tmp_path):
    client, archive = _client_with_archive(tmp_path)
    archive.append("thing/product/SN/events",
                   {"method": "alarm", "data": {"x": 1}},
                   recv_ts_ms=1780297017006)
    r = client.get("/api/timeline/export.csv")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/csv")
    assert "attachment" in r.headers["content-disposition"]
    lines = r.text.strip().split("\n")
    assert lines[0] == "recv_ts_ms,recv_ts_iso,virt_offset_ms,kind,topic,method,payload_json"
    assert "1780297017006" in lines[1]
    assert "event" in lines[1]
    assert "alarm" in lines[1]


def test_export_csv_payload_json_quoted_safely(tmp_path):
    import csv as _csv
    import io
    import json as _json
    client, archive = _client_with_archive(tmp_path)
    archive.append("thing/product/SN/events",
                   {"method": "x", "data": {"note": 'hello, "world"\nline2'}},
                   recv_ts_ms=1000)
    r = client.get("/api/timeline/export.csv")
    reader = _csv.reader(io.StringIO(r.text))
    rows = list(reader)
    assert rows[0][0] == "recv_ts_ms"
    payload_json = rows[1][6]
    # csv.reader properly unquotes the cell; json.loads recovers the original data
    decoded = _json.loads(payload_json)
    assert decoded["data"]["note"] == 'hello, "world"\nline2'


def test_export_csv_filename_uses_flight_basename_for_offline(tmp_path):
    from tests.unit.test_events_archive_from_flight_dir import _build_minimal_flight
    flight = _build_minimal_flight(tmp_path)
    client, _ = _client_with_archive(tmp_path)
    rel = flight.relative_to(tmp_path)
    r = client.get(f"/api/timeline/export.csv?source={rel}")
    assert flight.name in r.headers["content-disposition"]


def test_export_csv_filename_uses_live_for_live(tmp_path):
    client, archive = _client_with_archive(tmp_path)
    archive.append("thing/product/SN/events", {"method": "x"}, recv_ts_ms=1)
    r = client.get("/api/timeline/export.csv")
    assert "timeline_live_" in r.headers["content-disposition"]


def test_timeline_router_not_mounted_when_archive_none(tmp_path):
    """create_app(archive=None) → /api/timeline 不挂，FastAPI 自然 404。"""
    from fastapi.testclient import TestClient
    from sim_dji_cloud.dashboard.api import create_app
    from sim_dji_cloud.dashboard.live_state import LiveState

    app = create_app(state=LiveState(), archive=None, recordings_root=tmp_path)
    c = TestClient(app)
    assert c.get("/api/timeline").status_code == 404


def _client_with_play(tmp_path):
    """构造带 play_controller 的 TestClient。PlayController 用真类，
    但 _pid_alive 和 subprocess.Popen 在测试里 mock。"""
    from fastapi.testclient import TestClient
    from sim_dji_cloud.dashboard.api import create_app
    from sim_dji_cloud.dashboard.live_state import LiveState
    from sim_dji_cloud.dashboard.play_controller import PlayController

    recordings = tmp_path / "recordings"
    recordings.mkdir()
    (recordings / "flight_A").mkdir()
    logs = tmp_path / "logs"

    pc = PlayController(recordings_root=recordings, log_dir=logs)
    state = LiveState()
    app = create_app(state=state, play_controller=pc, recordings_root=recordings)
    return TestClient(app), pc, recordings


def test_play_start_503_when_no_token_env(tmp_path, monkeypatch):
    monkeypatch.delenv("DASHBOARD_TOKEN", raising=False)
    client, _, _ = _client_with_play(tmp_path)
    r = client.post("/api/play/start", json={"flight_dir": "flight_A"})
    assert r.status_code == 503
    assert "DASHBOARD_TOKEN" in r.json()["detail"]


def test_play_start_401_when_wrong_token(tmp_path, monkeypatch):
    monkeypatch.setenv("DASHBOARD_TOKEN", "secret123")
    client, _, _ = _client_with_play(tmp_path)
    r = client.post(
        "/api/play/start",
        json={"flight_dir": "flight_A"},
        headers={"Authorization": "Bearer wrong"},
    )
    assert r.status_code == 401


def test_play_start_201_when_correct_token(tmp_path, monkeypatch):
    from unittest.mock import MagicMock, patch
    monkeypatch.setenv("DASHBOARD_TOKEN", "secret123")
    client, pc, _ = _client_with_play(tmp_path)
    fake_proc = MagicMock()
    fake_proc.poll.return_value = None
    fake_proc.pid = 54321
    with patch("sim_dji_cloud.dashboard.play_controller.subprocess.Popen",
               return_value=fake_proc), \
         patch("sim_dji_cloud.dashboard.play_controller.PlayController._pid_alive",
               return_value=True):
        r = client.post(
            "/api/play/start",
            json={"flight_dir": "flight_A"},
            headers={"Authorization": "Bearer secret123"},
        )
    assert r.status_code == 201
    data = r.json()
    assert data["state"] == "running"
    assert data["pid"] == 54321


def test_play_start_400_when_path_traversal(tmp_path, monkeypatch):
    monkeypatch.setenv("DASHBOARD_TOKEN", "secret123")
    client, _, _ = _client_with_play(tmp_path)
    r = client.post(
        "/api/play/start",
        json={"flight_dir": "../etc"},
        headers={"Authorization": "Bearer secret123"},
    )
    assert r.status_code == 400


def test_play_start_409_when_already_running(tmp_path, monkeypatch):
    from unittest.mock import patch
    monkeypatch.setenv("DASHBOARD_TOKEN", "secret123")
    client, pc, _ = _client_with_play(tmp_path)
    pc.log_dir.mkdir(parents=True, exist_ok=True)
    (pc.log_dir / "play.pid").write_text("99999\n")
    with patch("sim_dji_cloud.dashboard.play_controller.PlayController._pid_alive",
               return_value=True):
        r = client.post(
            "/api/play/start",
            json={"flight_dir": "flight_A"},
            headers={"Authorization": "Bearer secret123"},
        )
    assert r.status_code == 409
    assert "99999" in r.json()["detail"]


def test_play_stop_200_when_running(tmp_path, monkeypatch):
    import signal
    from unittest.mock import patch
    monkeypatch.setenv("DASHBOARD_TOKEN", "secret123")
    client, pc, _ = _client_with_play(tmp_path)
    pc.log_dir.mkdir(parents=True, exist_ok=True)
    (pc.log_dir / "play.pid").write_text("12345\n")

    alive_state = {"v": True}

    def fake_kill(pid, sig):
        if sig == signal.SIGTERM:
            alive_state["v"] = False

    with patch("sim_dji_cloud.dashboard.play_controller.os.kill",
               side_effect=fake_kill), \
         patch("sim_dji_cloud.dashboard.play_controller.PlayController._pid_alive",
               side_effect=lambda pid: alive_state["v"]):
        r = client.post(
            "/api/play/stop",
            headers={"Authorization": "Bearer secret123"},
        )
    assert r.status_code == 200
    assert r.json()["state"] == "stopped"


def test_play_stop_404_when_not_running(tmp_path, monkeypatch):
    monkeypatch.setenv("DASHBOARD_TOKEN", "secret123")
    client, _, _ = _client_with_play(tmp_path)
    r = client.post(
        "/api/play/stop",
        headers={"Authorization": "Bearer secret123"},
    )
    assert r.status_code == 404


def test_play_status_no_token_required(tmp_path, monkeypatch):
    """GET status 公开，不带 Authorization 也 200。"""
    monkeypatch.delenv("DASHBOARD_TOKEN", raising=False)
    client, _, _ = _client_with_play(tmp_path)
    r = client.get("/api/play/status")
    assert r.status_code == 200
    assert r.json()["state"] == "stopped"


def test_play_router_not_mounted_without_controller(tmp_path):
    """create_app(play_controller=None) → /api/play/* 不挂，FastAPI 自然 404。"""
    from fastapi.testclient import TestClient
    from sim_dji_cloud.dashboard.api import create_app
    from sim_dji_cloud.dashboard.live_state import LiveState

    app = create_app(state=LiveState(), play_controller=None, recordings_root=tmp_path)
    c = TestClient(app)
    assert c.get("/api/play/status").status_code == 404
