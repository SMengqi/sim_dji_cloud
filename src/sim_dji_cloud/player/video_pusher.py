"""回放端视频推流：把 video/main.mp4 用 ffmpeg -re -c copy 推到 RTMP（SRS）。"""
from __future__ import annotations

import signal
import subprocess
from pathlib import Path
from typing import Optional

from loguru import logger


def plan_video_push(
    manifest: dict, push_url: str | None, speed: float, file_exists: bool,
    anchor_offset_ms: int = 0,
) -> dict | None:
    """决定是否/如何推视频。任一条件不满足返回 None（跳过）。

    Parameters
    ----------
    anchor_offset_ms:
        在 ``wait_virt_ms`` 上叠加的常量偏移（毫秒，可正可负）。用来补偿
        回放端 RTMP push → SRS GOP cache → 客户端 player buffer 这段
        不可避免的管道延迟 L。负值表示让视频比 first-frame 时刻提前 |L| 毫秒
        开始推，正好抵消管道延迟，视频和轨迹视觉同步。最终 wait 仍被
        ``max(0, ...)`` 兜底为非负——offset 把 wait 拉到负值时无意义。
        典型值由具体 SRS + 客户端组合决定（数秒量级），建议实测后写
        run.sh / CLI。默认 0 维持向后兼容。

    返回 {"wait_virt_ms": <视频在飞行时间轴上的起点偏移>, "ss_seconds": 0.0}。
    """
    if not push_url:
        return None
    if speed != 1.0:
        logger.warning("speed={} != 1.0，跳过视频推流（v1 仅 1x）", speed)
        return None
    video = manifest.get("video")
    if not video:
        logger.warning("manifest.video 为空，跳过视频推流")
        return None
    if not file_exists:
        logger.warning("video/main.mp4 不存在，跳过视频推流")
        return None
    base = video.get("started_at_recv_ms", 0) - manifest.get("started_at_recv_ms", 0)
    offset = max(0, base + anchor_offset_ms)
    return {"wait_virt_ms": offset, "ss_seconds": 0.0}


class VideoPusher:
    """ffmpeg -re -c copy 把本地 mp4 推到 RTMP；子进程管理仿 recorder 的 VideoWriter。"""

    def __init__(self, source_file: Path, push_url: str, extra_args: Optional[list[str]] = None):
        self.source_file = Path(source_file)
        self.push_url = push_url
        self.extra_args = list(extra_args or [])
        self._proc: Optional[subprocess.Popen] = None

    def _build_cmd(self, ss_seconds: float) -> list[str]:
        cmd = ["ffmpeg", "-re"]
        if ss_seconds > 0:
            cmd += ["-ss", str(ss_seconds)]
        cmd += ["-i", str(self.source_file), "-c", "copy", *self.extra_args,
                "-f", "flv", self.push_url]
        return cmd

    def start(self, ss_seconds: float = 0.0) -> None:
        # DEVNULL：长跑 ffmpeg 的未读 PIPE 会塞满内核缓冲导致卡死
        self._proc = subprocess.Popen(
            self._build_cmd(ss_seconds),
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )

    def is_alive(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def stop(self, timeout_s: float = 10.0) -> None:
        if self._proc is not None and self._proc.poll() is None:
            self._proc.send_signal(signal.SIGINT)
            try:
                self._proc.wait(timeout=timeout_s)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                self._proc.wait()
