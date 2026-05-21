import json
import signal
import subprocess
from pathlib import Path
from typing import Optional


class VideoWriter:
    """ffmpeg -c copy 直接落盘 mp4；同时维护 main.timing.json 记录启动墙钟。"""

    def __init__(
        self,
        source_url: str,
        output_dir: Path,
        extra_args: Optional[list[str]] = None,
    ):
        self.source_url = source_url
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.extra_args = list(extra_args or [])
        self._proc: Optional[subprocess.Popen] = None
        self._segments: list[dict] = []

    def _build_cmd(self) -> list[str]:
        return [
            "ffmpeg",
            "-y",
            "-i", self.source_url,
            "-c", "copy",
            "-movflags", "+faststart+frag_keyframe",
            "-f", "mp4",
            *self.extra_args,
            str(self.output_dir / "main.mp4"),
        ]

    def start(self, started_at_recv_ms: int) -> None:
        cmd = self._build_cmd()
        # Both DEVNULL: an unread PIPE for long-running ffmpeg fills the kernel
        # pipe buffer (~64KB) and stalls the subprocess. If diagnostic logs are
        # needed, pass `-loglevel warning -report` via extra_args.
        self._proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self._segments.append({
            "file": "main.mp4",
            "ffmpeg_start_wall_ms": started_at_recv_ms,
            "pts_offset_ms": 0,
        })

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
        self._write_timing()

    def _write_timing(self) -> None:
        (self.output_dir / "main.timing.json").write_text(
            json.dumps({"segments": self._segments}, indent=2)
        )

    def manifest_video_block(self, duration_ms: int) -> dict:
        started = self._segments[0]["ffmpeg_start_wall_ms"] if self._segments else None
        return {
            "file": "video/main.mp4",
            "source_url": self.source_url,
            "started_at_recv_ms": started,
            "duration_ms": duration_ms,
            "segments": [
                {"start_ms": 0, "end_ms": duration_ms, "file": "video/main.mp4"},
            ],
        }
