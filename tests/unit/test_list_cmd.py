import json
from pathlib import Path
from sim_dji_cloud.tools.list_cmd import list_flights


def _make_flight(root: Path, name: str, **overrides) -> Path:
    flight = root / name
    flight.mkdir(parents=True)
    manifest = {
        "schema_version": 1, "status": "ok", "finalize_reason": "auto_idle",
        "task_id": name.split("__")[0], "dock_sn": "SN_DOCK", "drone_sn": "SN_DRONE",
        "started_at_recv_ms": 0, "ended_at_recv_ms": 60_000,
        "gaps": [], "topics": [], "video": None,
    }
    manifest.update(overrides)
    (flight / "manifest.json").write_text(json.dumps(manifest))
    return flight


def test_list_outputs_each_flight(tmp_path: Path, capsys):
    _make_flight(tmp_path, "T1__SN_DOCK__20260521-100000",
                 ended_at_recv_ms=120_000)
    _make_flight(tmp_path, "T2__SN_DOCK__20260521-110000",
                 ended_at_recv_ms=180_000, status="interrupted")

    code = list_flights(tmp_path)
    captured = capsys.readouterr()
    assert code == 0
    assert "T1" in captured.out
    assert "T2" in captured.out
    assert "120.0s" in captured.out
    assert "interrupted" in captured.out


def test_list_skips_pending_dirs(tmp_path: Path, capsys):
    _make_flight(tmp_path, "T1__SN_DOCK__20260521-100000")
    (tmp_path / "pending_12345").mkdir()
    code = list_flights(tmp_path)
    captured = capsys.readouterr()
    assert code == 0
    assert "T1" in captured.out
    assert "pending_12345" not in captured.out


def test_list_handles_empty_root(tmp_path: Path, capsys):
    code = list_flights(tmp_path)
    captured = capsys.readouterr()
    assert code == 0
    assert "no flights" in captured.out.lower()
