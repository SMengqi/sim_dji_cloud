import threading
import time
from unittest.mock import MagicMock, patch
from pathlib import Path

import pytest

from sim_dji_cloud.player.video_pusher import VideoPusher


def test_start_builds_ffmpeg_push_cmd(tmp_path):
    src = tmp_path / "main.mp4"
    src.write_bytes(b"x")
    with patch("sim_dji_cloud.player.video_pusher.subprocess.Popen") as popen:
        popen.return_value.poll.return_value = None
        vp = VideoPusher(src, "rtmp://srs/live/x")
        vp.start(ss_seconds=0.0)
        cmd = popen.call_args[0][0]
        assert cmd[0] == "ffmpeg"
        assert "-re" in cmd
        assert cmd[cmd.index("-c") + 1] == "copy"
        assert cmd[cmd.index("-f") + 1] == "flv"
        assert cmd[cmd.index("-i") + 1] == str(src)
        assert cmd[-1] == "rtmp://srs/live/x"
        assert "-ss" not in cmd  # ss=0 不加 -ss


def test_start_with_ss_before_input(tmp_path):
    src = tmp_path / "main.mp4"
    src.write_bytes(b"x")
    with patch("sim_dji_cloud.player.video_pusher.subprocess.Popen") as popen:
        popen.return_value.poll.return_value = None
        vp = VideoPusher(src, "rtmp://srs/live/x")
        vp.start(ss_seconds=12.5)
        cmd = popen.call_args[0][0]
        assert cmd[cmd.index("-ss") + 1] == "12.5"
        assert cmd.index("-ss") < cmd.index("-i")  # 快速 seek：-ss 在 -i 前


def test_stop_sigint_then_kill_on_timeout(tmp_path):
    src = tmp_path / "main.mp4"
    src.write_bytes(b"x")
    import subprocess as _sp
    exit_evt = threading.Event()

    # supervisor 线程跟主线程会并发调 wait()；用 callable side_effect
    # 区分两条路径（无 timeout = supervisor 阻塞等；有 timeout = 主线程 stop）。
    def fake_wait(timeout=None):
        if timeout is None:
            exit_evt.wait()         # 等 kill 触发
            return -9
        raise _sp.TimeoutExpired(cmd="ffmpeg", timeout=timeout)

    def fake_kill():
        exit_evt.set()

    with patch("sim_dji_cloud.player.video_pusher.subprocess.Popen") as popen:
        proc = MagicMock()
        proc.poll.return_value = None
        proc.wait.side_effect = fake_wait
        proc.kill.side_effect = fake_kill
        popen.return_value = proc
        vp = VideoPusher(src, "rtmp://srs/live/x")
        vp.start()
        vp.stop(timeout_s=0.01)
        proc.send_signal.assert_called_once()   # 先 SIGINT
        proc.kill.assert_called_once()          # 超时后 SIGKILL


def test_stop_noop_when_already_exited(tmp_path):
    src = tmp_path / "main.mp4"
    src.write_bytes(b"x")
    with patch("sim_dji_cloud.player.video_pusher.subprocess.Popen") as popen:
        proc = MagicMock()
        proc.poll.return_value = 0  # 已退出
        popen.return_value = proc
        vp = VideoPusher(src, "rtmp://srs/live/x")
        vp.start()
        vp.stop()
        proc.send_signal.assert_not_called()


# ---------------------------------------------------------------------------
# Supervisor regression suite (review C-7 / MAJOR: ffmpeg 退出无人检测)
# ---------------------------------------------------------------------------

def _proc_with_blocking_wait(exit_code: int, exit_after_event: threading.Event) -> MagicMock:
    """造一个 mock proc，wait() 会阻塞到 exit_after_event.set() 才返 exit_code。

    模拟真实 ffmpeg 子进程：start 后 supervisor 线程调 wait() 阻塞；外部触发
    退出（kill / RTMP 断 / 自然结束）时 wait 返 exit_code。
    """
    proc = MagicMock()
    proc.pid = 4242
    state = {"alive": True, "rc": None}

    def fake_wait(timeout=None):
        if not state["alive"]:
            return state["rc"]
        # 模拟外部触发退出（test 调 exit_after_event.set 后才返）
        if exit_after_event.wait(timeout=timeout):
            state["alive"] = False
            state["rc"] = exit_code
            return exit_code
        # 超时
        import subprocess as _sp
        raise _sp.TimeoutExpired(cmd="ffmpeg", timeout=timeout)

    def fake_poll():
        return None if state["alive"] else state["rc"]

    def fake_send_signal(_sig):
        # SIGINT 让 wait 返；模拟 ffmpeg 收到信号正常退
        exit_after_event.set()

    def fake_kill():
        exit_after_event.set()

    proc.wait.side_effect = fake_wait
    proc.poll.side_effect = fake_poll
    proc.send_signal.side_effect = fake_send_signal
    proc.kill.side_effect = fake_kill
    return proc


def test_supervisor_records_exit_code(tmp_path):
    """ffmpeg 正常退出后 supervisor 线程把 returncode 记到 last_exit_code。

    Regression: 没有 supervisor 时 ffmpeg 退了主流程拿不到 exit_code，
    无法判断"成功推完 vs 中途崩"。
    """
    src = tmp_path / "main.mp4"
    src.write_bytes(b"x")
    exit_evt = threading.Event()
    with patch("sim_dji_cloud.player.video_pusher.subprocess.Popen",
               return_value=_proc_with_blocking_wait(0, exit_evt)):
        vp = VideoPusher(src, "rtmp://srs/live/x")
        vp.start()
        assert vp.last_exit_code is None    # 还在跑
        exit_evt.set()                       # 模拟 ffmpeg 自然结束
        vp._watch_thread.join(timeout=2.0)
        assert vp.last_exit_code == 0


def test_supervisor_logs_warning_on_unexpected_exit(tmp_path):
    """没有主动 stop()，ffmpeg 自己退（RTMP URL 错 / SRS 没起）→ 必须 log WARNING。

    Regression: review C-7 原话 — "RTMP URL 写错、SRS 没起、网络断 → ffmpeg 立即
    exit；play 仍在跑 MQTT，dashboard 视频面板黑屏，progress 显示正常"。supervisor
    log WARNING 让运维 grep 一眼能查到。
    """
    from loguru import logger as _logger
    src = tmp_path / "main.mp4"
    src.write_bytes(b"x")
    captured: list[str] = []
    sink_id = _logger.add(lambda m: captured.append(str(m)), level="WARNING")
    exit_evt = threading.Event()
    try:
        with patch("sim_dji_cloud.player.video_pusher.subprocess.Popen",
                   return_value=_proc_with_blocking_wait(1, exit_evt)):
            vp = VideoPusher(src, "rtmp://bad-host/live/x")
            vp.start()
            exit_evt.set()                    # 模拟 ffmpeg 立即崩
            vp._watch_thread.join(timeout=2.0)
    finally:
        _logger.remove(sink_id)
    assert any("unexpect" in m.lower() or "ffmpeg" in m.lower() and "exit" in m.lower()
               for m in captured), \
        f"未捕获 WARNING：{captured}"
    assert any("rtmp://bad-host" in m for m in captured), \
        f"WARNING 必须带 push_url 给运维定位：{captured}"


def test_supervisor_quiet_on_intentional_stop(tmp_path):
    """主动调 stop() 后 supervisor 看到的退出不应当 log WARNING（避免噪音）。"""
    from loguru import logger as _logger
    src = tmp_path / "main.mp4"
    src.write_bytes(b"x")
    captured: list[str] = []
    sink_id = _logger.add(lambda m: captured.append(str(m)), level="WARNING")
    exit_evt = threading.Event()
    try:
        with patch("sim_dji_cloud.player.video_pusher.subprocess.Popen",
                   return_value=_proc_with_blocking_wait(0, exit_evt)):
            vp = VideoPusher(src, "rtmp://srs/live/x")
            vp.start()
            vp.stop(timeout_s=1.0)            # 主动停；fake_send_signal 触发 exit_evt
            assert vp._watch_thread is not None
            vp._watch_thread.join(timeout=2.0)
    finally:
        _logger.remove(sink_id)
    # 允许 INFO，但 WARNING 不应出现
    warnings = [m for m in captured if "unexpect" in m.lower()
                or ("ffmpeg" in m.lower() and "exit" in m.lower()
                    and "rtmp://srs" in m)]
    assert not warnings, f"主动 stop 不应触发意外退出 WARNING：{warnings}"


@pytest.mark.asyncio
async def test_aclose_does_not_block_event_loop(tmp_path):
    """aclose() 把同步 stop() 丢去 to_thread；ffmpeg 卡 wait() 不会冻 asyncio 主循环。

    Regression: review MAJOR "_proc.wait(timeout=10) 阻塞事件循环 10s"。
    """
    import asyncio
    src = tmp_path / "main.mp4"
    src.write_bytes(b"x")
    exit_evt = threading.Event()

    with patch("sim_dji_cloud.player.video_pusher.subprocess.Popen",
               return_value=_proc_with_blocking_wait(0, exit_evt)):
        vp = VideoPusher(src, "rtmp://srs/live/x")
        vp.start()

        # 在 aclose 跑的同时，asyncio loop 必须能执行别的 task；
        # 用 sleep(0) tick 加计数器证明 loop 没冻。
        ticks = {"n": 0}

        async def ticker():
            for _ in range(20):
                await asyncio.sleep(0.01)
                ticks["n"] += 1

        async def trigger_exit_later():
            # 50ms 后让 ffmpeg 退；aclose 走 SIGINT 路径会立即触发，
            # 但确保即使 SIGINT 没用我们也兜底
            await asyncio.sleep(0.05)
            exit_evt.set()

        t0 = time.monotonic()
        await asyncio.gather(
            vp.aclose(timeout_s=2.0),
            ticker(),
            trigger_exit_later(),
        )
        elapsed = time.monotonic() - t0

    assert elapsed < 1.0, f"aclose 用 {elapsed:.2f}s，主循环显然被冻"
    assert ticks["n"] >= 10, (
        f"aclose 期间 asyncio loop 只 tick 了 {ticks['n']} 次；事件循环被冻"
    )
