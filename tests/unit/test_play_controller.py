import asyncio
import json
import signal
import time
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


def test_start_rejects_symlink_pointing_outside_recordings(tmp_path):
    """C-9 验证: recordings_root 内的 symlink 指向外部目录 → 必须被拒绝。

    .resolve() + relative_to() 已正确处理；本测试 pin 住该保护，避免
    重构时退化。
    """
    pc, recordings = _make_pc(tmp_path)
    outside = tmp_path / "outside_target"
    outside.mkdir()
    (outside / "secret.txt").write_text("PWNED")
    symlink = recordings / "sneak"
    symlink.symlink_to(outside)
    with pytest.raises(ValueError, match="must be under"):
        pc.start("sneak")


def test_start_closes_log_fp_when_popen_fails(tmp_path):
    """Popen 抛错时 log_fp 必须被关掉 —— FD 泄漏回归。

    旧实现 `log_fp = open(...); proc = Popen(...); log_fp.close()` 中间无
    try/finally；Popen 抛 FileNotFoundError（sim-dji 不在 PATH）时 close
    永不执行，每次失败启动泄漏一个 FD。修复改用 with open(...) 包 Popen。
    """
    pc, _ = _make_pc(tmp_path)
    mock_fp = MagicMock()
    mock_fp.__enter__ = MagicMock(return_value=mock_fp)
    mock_fp.__exit__ = MagicMock(return_value=False)
    with patch("sim_dji_cloud.dashboard.play_controller.open",
               create=True, return_value=mock_fp), \
         patch("sim_dji_cloud.dashboard.play_controller.subprocess.Popen",
               side_effect=FileNotFoundError("sim-dji not in PATH")):
        with pytest.raises(FileNotFoundError):
            pc.start("flight_A")
    assert mock_fp.close.called or mock_fp.__exit__.called


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


# ---------------------------------------------------------------------------
# astart / astop async regression suite (review MAJOR: PlayController async 化)
#
# Background: 旧 start() / stop() 内 sidecar 等待 / SIGTERM 等待用 time.sleep
# 阻塞最长 5s / timeout_s 秒。dashboard FastAPI sync handler 跑在 anyio
# threadpool 里，并发请求把池吃光后新请求排队。astart/astop 内部用
# asyncio.sleep，handler 改 async def → 整条链路不再阻塞任何线程。
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_astart_returns_same_dict_as_start(tmp_path):
    """astart 跟 start 的返回 dict 形状一致（state/pid/flight_dir/...）。"""
    pc, _ = _make_pc(tmp_path)
    with patch("sim_dji_cloud.dashboard.play_controller.subprocess.Popen",
               return_value=_running_popen()), \
         patch.object(PlayController, "_pid_alive", return_value=True):
        result = await pc.astart("flight_A")
    assert result["state"] == "running"
    assert result["pid"] == 12345
    assert result["flight_dir"].endswith("flight_A")


@pytest.mark.asyncio
async def test_astart_does_not_block_event_loop(tmp_path):
    """sidecar 没出现时 astart 等待阶段不冻 asyncio loop。

    Regression: 旧 start() 内 time.sleep 循环最长 5s，async handler 转为 sync
    handler 后 anyio 池被霸占。astart 用 await asyncio.sleep 真正 yield。
    """
    pc, _ = _make_pc(tmp_path)
    ticks = {"n": 0}

    async def ticker():
        for _ in range(40):
            await asyncio.sleep(0.01)
            ticks["n"] += 1

    with patch("sim_dji_cloud.dashboard.play_controller.subprocess.Popen",
               return_value=_running_popen()), \
         patch.object(PlayController, "_pid_alive", return_value=True):
        t0 = time.monotonic()
        # ticker 跑 ~400ms；同期 astart 因 sidecar 永远不出现走完 deadline
        await asyncio.gather(pc.astart("flight_A"), ticker())
        elapsed = time.monotonic() - t0

    assert ticks["n"] >= 30, (
        f"astart 等 sidecar 期间 asyncio loop 被冻 — ticker 只 tick "
        f"{ticks['n']} 次（应 ≥30），elapsed={elapsed:.2f}s"
    )


@pytest.mark.asyncio
async def test_astop_returns_same_dict_as_stop(tmp_path):
    """astop 跟 stop 返回形状一致；进程被 SIGTERM 优雅退出。"""
    pc, _ = _make_pc(tmp_path)
    (tmp_path / "logs").mkdir(exist_ok=True)
    (tmp_path / "logs" / "play.pid").write_text("12345\n")
    (tmp_path / "logs" / "play.meta.json").write_text("{}")

    alive_state = {"v": True}

    def kill_then_dead(pid, sig):
        if sig == signal.SIGTERM:
            alive_state["v"] = False

    def fake_alive(pid):
        return alive_state["v"]

    with patch("sim_dji_cloud.dashboard.play_controller.os.kill",
               side_effect=kill_then_dead), \
         patch.object(PlayController, "_pid_alive", side_effect=fake_alive):
        result = await pc.astop()

    assert result["state"] == "stopped"
    assert not (tmp_path / "logs" / "play.pid").exists()


@pytest.mark.asyncio
async def test_astop_does_not_block_event_loop(tmp_path):
    """SIGTERM 后 pid 一直 alive 时 astop 等待 deadline 不冻 asyncio loop。"""
    pc, _ = _make_pc(tmp_path)
    (tmp_path / "logs").mkdir(exist_ok=True)
    (tmp_path / "logs" / "play.pid").write_text("12345\n")
    (tmp_path / "logs" / "play.meta.json").write_text("{}")

    ticks = {"n": 0}

    async def ticker():
        for _ in range(40):
            await asyncio.sleep(0.01)
            ticks["n"] += 1

    # _pid_alive 永远 True → 走 deadline + SIGKILL 兜底分支；timeout_s=0.4 让
    # 测试快。期间 ticker 应正常 tick。
    with patch("sim_dji_cloud.dashboard.play_controller.os.kill") as kill_mock, \
         patch.object(PlayController, "_pid_alive", return_value=True):
        await asyncio.gather(pc.astop(timeout_s=0.4), ticker())

    assert ticks["n"] >= 30, (
        f"astop 等 SIGTERM 期间 asyncio loop 被冻 — ticker 只 tick "
        f"{ticks['n']} 次（应 ≥30）"
    )
    # SIGTERM + SIGKILL 都该发了
    sigs = [c.args[1] for c in kill_mock.call_args_list]
    assert signal.SIGTERM in sigs
    assert signal.SIGKILL in sigs


@pytest.mark.asyncio
async def test_astop_when_not_running_raises_NotRunning(tmp_path):
    pc, _ = _make_pc(tmp_path)
    with pytest.raises(NotRunning):
        await pc.astop()
