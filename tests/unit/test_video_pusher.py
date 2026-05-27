from unittest.mock import MagicMock, patch
from pathlib import Path

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
    with patch("sim_dji_cloud.player.video_pusher.subprocess.Popen") as popen:
        proc = MagicMock()
        proc.poll.return_value = None  # 仍在运行
        # 第一次 wait(timeout) 超时；kill 后的 wait() 立即返回（真实 SIGKILL 行为）
        proc.wait.side_effect = [__import__("subprocess").TimeoutExpired("ffmpeg", 10), None]
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
