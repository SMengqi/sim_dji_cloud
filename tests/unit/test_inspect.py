import json
from pathlib import Path
from sim_dji_cloud.tools.inspect_cmd import inspect_flight


def test_inspect_outputs_summary(tmp_path: Path, capsys):
    flight = tmp_path / "T1__SN_DOCK__20260519-100000"
    flight.mkdir(parents=True)
    manifest = {
        "schema_version": 1,
        "status": "ok",
        "finalize_reason": "auto_idle",
        "task_id": "T1",
        "dock_sn": "SN_DOCK",
        "drone_sn": "SN_DRONE",
        "started_at_recv_ms": 0,
        "ended_at_recv_ms": 1_800_000,
        "takeoff_offset_ms": 5000,
        "landing_offset_ms": 1_795_000,
        "gaps": [{"reason": "mqtt_disconnect", "start_ms": 100, "end_ms": 200}],
        "topics": [
            {"topic": "thing/product/SN_DOCK/osd", "device_sn": "SN_DOCK",
             "direction": "up", "count": 1800,
             "first_recv_ts_ms": 0, "last_recv_ts_ms": 1_800_000, "files": []},
            {"topic": "thing/product/SN_DRONE/osd", "device_sn": "SN_DRONE",
             "direction": "up", "count": 1800,
             "first_recv_ts_ms": 0, "last_recv_ts_ms": 1_800_000, "files": []},
        ],
        "video": {"file": "video/main.mp4", "duration_ms": 1_799_500},
    }
    (flight / "manifest.json").write_text(json.dumps(manifest))

    code = inspect_flight(flight)
    captured = capsys.readouterr()
    assert code == 0
    assert "T1" in captured.out
    assert "SN_DOCK" in captured.out
    assert "SN_DRONE" in captured.out
    assert "thing/product/SN_DOCK/osd" in captured.out
    assert "thing/product/SN_DRONE/osd" in captured.out
    assert "1 gap" in captured.out
    assert "video" in captured.out.lower()


def test_inspect_missing_manifest_returns_nonzero(tmp_path: Path, capsys):
    flight = tmp_path / "broken"
    flight.mkdir()
    code = inspect_flight(flight)
    assert code != 0
    captured = capsys.readouterr()
    assert "manifest" in captured.err.lower() or "missing" in captured.err.lower()
