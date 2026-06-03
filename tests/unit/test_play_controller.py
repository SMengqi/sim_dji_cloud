import json
import signal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from sim_dji_cloud.dashboard.play_controller import (
    PlayController, PlayAlreadyRunning, NotRunning,
)


def _make_pc(tmp_path: Path) -> tuple[PlayController, Path]:
    recordings = tmp_path / "recordings"
    recordings.mkdir()
    (recordings / "flight_A").mkdir()
    log_dir = tmp_path / "logs"
    return PlayController(recordings_root=recordings, log_dir=log_dir), recordings


def _running_popen() -> MagicMock:
    """Mock subprocess.Popen 返一个'活着'的进程对象。"""
    p = MagicMock()
    p.poll.return_value = None
    p.pid = 12345
    return p


def test_start_writes_pid_and_meta_files(tmp_path):
    pc, _ = _make_pc(tmp_path)
    with patch("sim_dji_cloud.dashboard.play_controller.subprocess.Popen",
               return_value=_running_popen()), \
         patch.object(PlayController, "_pid_alive", return_value=True):
        pc.start("flight_A")
    assert (tmp_path / "logs" / "play.pid").exists()
    assert (tmp_path / "logs" / "play.pid").read_text().strip() == "12345"
    assert (tmp_path / "logs" / "play.meta.json").exists()
    meta = json.loads((tmp_path / "logs" / "play.meta.json").read_text())
    assert meta["flight_dir"].endswith("flight_A")
    assert meta["speed"] == 1.0


def test_start_when_already_running_raises_PlayAlreadyRunning(tmp_path):
    pc, _ = _make_pc(tmp_path)
    (tmp_path / "logs").mkdir(exist_ok=True)
    (tmp_path / "logs" / "play.pid").write_text("99999\n")
    with patch.object(PlayController, "_pid_alive", return_value=True):
        with pytest.raises(PlayAlreadyRunning) as exc_info:
            pc.start("flight_A")
    assert exc_info.value.pid == 99999


def test_start_rejects_path_traversal(tmp_path):
    pc, _ = _make_pc(tmp_path)
    with pytest.raises(ValueError, match="must not contain"):
        pc.start("../etc/passwd")


def test_start_rejects_outside_recordings_root(tmp_path):
    pc, _ = _make_pc(tmp_path)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    with pytest.raises(ValueError, match="must be under"):
        pc.start(str(elsewhere))


def test_start_rejects_nonexistent_flight_dir(tmp_path):
    pc, _ = _make_pc(tmp_path)
    with pytest.raises(ValueError, match="not found"):
        pc.start("no_such_dir")


def test_stop_sends_sigterm_and_clears_files(tmp_path):
    pc, _ = _make_pc(tmp_path)
    (tmp_path / "logs").mkdir(exist_ok=True)
    (tmp_path / "logs" / "play.pid").write_text("12345\n")
    (tmp_path / "logs" / "play.meta.json").write_text("{}")

    killed: list[tuple[int, int]] = []
    alive_state = {"v": True}

    def kill_then_dead(pid, sig):
        killed.append((pid, sig))
        if sig == signal.SIGTERM:
            alive_state["v"] = False

    def fake_alive(pid):
        return alive_state["v"]

    with patch("sim_dji_cloud.dashboard.play_controller.os.kill",
               side_effect=kill_then_dead), \
         patch.object(PlayController, "_pid_alive", side_effect=fake_alive):
        result = pc.stop()

    assert (12345, signal.SIGTERM) in killed
    assert not (tmp_path / "logs" / "play.pid").exists()
    assert not (tmp_path / "logs" / "play.meta.json").exists()
    assert result["state"] == "stopped"


def test_stop_when_not_running_raises_NotRunning(tmp_path):
    pc, _ = _make_pc(tmp_path)
    with pytest.raises(NotRunning):
        pc.stop()


def test_status_running_returns_meta_fields(tmp_path):
    pc, _ = _make_pc(tmp_path)
    (tmp_path / "logs").mkdir(exist_ok=True)
    (tmp_path / "logs" / "play.pid").write_text("12345\n")
    (tmp_path / "logs" / "play.meta.json").write_text(json.dumps({
        "flight_dir": "/abs/path/flight_A",
        "speed": 2.0,
        "started_at_ms": 1780000000000,
    }))
    with patch.object(PlayController, "_pid_alive", return_value=True):
        s = pc.status()
    assert s["state"] == "running"
    assert s["pid"] == 12345
    assert s["speed"] == 2.0
    assert s["started_at_ms"] == 1780000000000


def test_status_cleans_stale_pid_file(tmp_path):
    pc, _ = _make_pc(tmp_path)
    (tmp_path / "logs").mkdir(exist_ok=True)
    (tmp_path / "logs" / "play.pid").write_text("99999\n")
    with patch.object(PlayController, "_pid_alive", return_value=False):
        s = pc.status()
    assert s["state"] == "stopped"
    assert not (tmp_path / "logs" / "play.pid").exists()


def test_status_log_tail_returns_last_n_lines(tmp_path):
    pc, _ = _make_pc(tmp_path)
    (tmp_path / "logs").mkdir(exist_ok=True)
    (tmp_path / "logs" / "play.pid").write_text("12345\n")
    log_file = tmp_path / "logs" / "play-20260602-120000.log"
    log_file.write_text("\n".join(f"line {i}" for i in range(50)) + "\n")
    (tmp_path / "logs" / "play-latest.log").symlink_to("play-20260602-120000.log")
    with patch.object(PlayController, "_pid_alive", return_value=True):
        s = pc.status()
    tail_lines = s["log_tail"].splitlines()
    assert len(tail_lines) == 20
    assert tail_lines[-1] == "line 49"


def test_pid_alive_returns_false_for_zombie(monkeypatch):
    """Child process is dead but not reaped (zombie) → _pid_alive returns False.

    Regression test for 2026-06-02 production bug: os.kill(pid, 0) succeeds
    for zombie processes, causing status() to falsely report state="running".
    """
    # Mock os.kill(pid, 0) to succeed (simulate pid existing)
    monkeypatch.setattr(
        "sim_dji_cloud.dashboard.play_controller.os.kill",
        lambda pid, sig: None
    )

    # Mock open to return /proc/<pid>/status with State: Z (zombie)
    fake_status = (
        "Name:\tsim-dji\n"
        "Umask:\t0022\n"
        "State:\tZ (zombie)\n"
        "Tgid:\t99999\n"
    )
    import builtins
    real_open = builtins.open

    def fake_open(path, *args, **kwargs):
        if str(path).startswith("/proc/") and str(path).endswith("/status"):
            from io import StringIO
            return StringIO(fake_status)
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", fake_open)

    # Mock waitpid to succeed (simulate successful reap)
    monkeypatch.setattr(
        "sim_dji_cloud.dashboard.play_controller.os.waitpid",
        lambda pid, flags: (pid, 0)
    )

    assert PlayController._pid_alive(99999) is False


def test_pid_alive_returns_true_for_running_non_zombie(monkeypatch):
    """Running process (State: R/S) → _pid_alive returns True.

    Regression protection: ensure zombie detection doesn't break normal
    process detection.
    """
    # Mock os.kill(pid, 0) to succeed (simulate pid existing)
    monkeypatch.setattr(
        "sim_dji_cloud.dashboard.play_controller.os.kill",
        lambda pid, sig: None
    )

    # Mock open to return /proc/<pid>/status with State: S (sleeping, normal)
    fake_status = "Name:\tsim-dji\nState:\tS (sleeping)\n"
    import builtins
    real_open = builtins.open

    def fake_open(path, *args, **kwargs):
        if str(path).startswith("/proc/") and str(path).endswith("/status"):
            from io import StringIO
            return StringIO(fake_status)
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", fake_open)

    assert PlayController._pid_alive(99999) is True
