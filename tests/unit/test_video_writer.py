import json
from pathlib import Path
from unittest.mock import MagicMock, patch
from sim_dji_cloud.recorder.video_writer import VideoWriter


def test_video_writer_spawns_ffmpeg_with_correct_args(tmp_path: Path):
    video_dir = tmp_path / "video"
    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen") as popen:
        popen.return_value = MagicMock(poll=lambda: None)
        vw = VideoWriter(
            source_url="rtmp://example/live/abc",
            output_dir=video_dir,
            extra_args=["-loglevel", "warning"],
        )
        vw.start(started_at_recv_ms=1715000000000)

        args, _kwargs = popen.call_args
        cmd = args[0]
        assert "ffmpeg" in cmd[0]
        assert "rtmp://example/live/abc" in cmd
        assert "-c" in cmd and "copy" in cmd
        assert "-f" in cmd and "mp4" in cmd
        assert str(video_dir / "main.mp4") in cmd
        assert "-loglevel" in cmd and "warning" in cmd


def test_video_writer_maps_only_video_and_optional_audio(tmp_path: Path):
    # 只录视频流（+可选音频），丢弃源里可能存在的 data 流。dock 原生 RTMP 带
    # 一条 Stream Data:none，若 -c copy 映射全部流，mp4 会 "Could not write header"。
    video_dir = tmp_path / "video"
    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen") as popen:
        popen.return_value = MagicMock(poll=lambda: None)
        vw = VideoWriter(source_url="rtmp://example/live/abc", output_dir=video_dir)
        vw.start(started_at_recv_ms=1)
        cmd = popen.call_args[0][0]
        map_vals = [cmd[i + 1] for i, a in enumerate(cmd) if a == "-map"]
        assert "0:v" in map_vals          # 视频流必录
        assert "0:a?" in map_vals         # 音频可选（有才带，无不报错）
        assert cmd.index("-map") > cmd.index("-i")  # -map 在输入之后


def test_video_writer_emits_timing_json_on_stop(tmp_path: Path):
    video_dir = tmp_path / "video"
    with patch("sim_dji_cloud.recorder.video_writer.subprocess.Popen") as popen:
        proc = MagicMock()
        proc.poll = lambda: None
        proc.send_signal = MagicMock()
        proc.wait = MagicMock(return_value=0)
        popen.return_value = proc

        vw = VideoWriter(source_url="rtmp://example", output_dir=video_dir)
        vw.start(started_at_recv_ms=1715000000500)
        vw.stop()

    timing = json.loads((video_dir / "main.timing.json").read_text())
    assert len(timing["segments"]) == 1
    assert timing["segments"][0]["file"] == "main.mp4"
    assert timing["segments"][0]["ffmpeg_start_wall_ms"] == 1715000000500
    assert timing["segments"][0]["pts_offset_ms"] == 0
