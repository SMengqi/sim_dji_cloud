import json
import signal
import subprocess
import threading
import time
from pathlib import Path
from typing import Callable, Optional

from loguru import logger


def _default_probe(url: str) -> bool:
    """Probe RTMP stream with ffprobe.  Returns True if the stream is reachable."""
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-timeout", "2000000",
                "-rw_timeout", "2000000",
                "-i", url,
                "-show_entries", "stream=index",
                "-of", "csv=p=0",
                "-v", "error",
            ],
            timeout=5,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return result.returncode == 0
    except Exception:
        return False


class VideoWriter:
    """Probe RTMP source first, then launch ffmpeg.

    A background supervisor thread probes the source URL repeatedly until it
    becomes reachable (or stop() is called).  Only after a successful probe
    does ffmpeg start.

    Filename is ``main_<epoch_ms>.mp4`` where the epoch ms is captured at the
    moment ffmpeg is launched (not at start() time).

    ffmpeg stderr is redirected to ``<output_dir>/main.ffmpeg.log``.
    ``main.timing.json`` is always written on stop(), even if ffmpeg never
    launched (in that case segments=[]).
    """

    def __init__(
        self,
        source_url: str,
        output_dir: Path,
        extra_args: Optional[list[str]] = None,
        probe_factory: Optional[Callable[[str], bool]] = None,
        retry_interval_s: float = 2.0,
    ):
        self.source_url = source_url
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.extra_args = list(extra_args or [])
        self._probe = probe_factory if probe_factory is not None else _default_probe
        self._retry_interval_s = retry_interval_s

        self._proc: Optional[subprocess.Popen] = None
        self._stderr_log = None   # open file handle for main.ffmpeg.log
        self._segments: list[dict] = []
        self._filename: Optional[str] = None  # set at launch time

        self._stop_event = threading.Event()
        self._supervisor_thread: Optional[threading.Thread] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Kick off supervisor thread.  Returns immediately; ffmpeg may launch later."""
        self._stop_event.clear()
        self._supervisor_thread = threading.Thread(
            target=self._supervise,
            name="video-writer-supervisor",
            daemon=True,
        )
        self._supervisor_thread.start()
        logger.info("video supervisor started, probing {}", self.source_url)

    def is_alive(self) -> bool:
        """True iff ffmpeg subprocess is currently running."""
        return self._proc is not None and self._proc.poll() is None

    def stop(self, timeout_s: float = 10.0) -> None:
        """Stop supervisor + SIGINT/SIGKILL ffmpeg + write timing.json + close stderr log."""
        self._stop_event.set()
        if self._supervisor_thread is not None:
            self._supervisor_thread.join(timeout=timeout_s)

        if self._proc is not None and self._proc.poll() is None:
            self._proc.send_signal(signal.SIGINT)
            try:
                self._proc.wait(timeout=timeout_s)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                self._proc.wait()

        if self._stderr_log is not None:
            try:
                self._stderr_log.close()
            except Exception:
                pass
            self._stderr_log = None

        self._write_timing()

    def manifest_video_block(self, duration_ms: int) -> dict:
        """Return manifest video block.  If no ffmpeg ever launched, file=None & segments=[]."""
        if not self._segments:
            return {
                "file": None,
                "source_url": self.source_url,
                "started_at_recv_ms": None,
                "duration_ms": duration_ms,
                "segments": [],
            }
        started = self._segments[0]["ffmpeg_start_wall_ms"]
        video_rel = f"video/{self._filename}"
        return {
            "file": video_rel,
            "source_url": self.source_url,
            "started_at_recv_ms": started,
            "duration_ms": duration_ms,
            "segments": [
                {"start_ms": 0, "end_ms": duration_ms, "file": video_rel},
            ],
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _supervise(self) -> None:
        """Background thread: probe loop -> launch ffmpeg once stream is ready."""
        while not self._stop_event.is_set():
            if self._probe(self.source_url):
                self._launch_ffmpeg()
                return
            self._stop_event.wait(timeout=self._retry_interval_s)
        # 走到这里说明 stop_event 在我们探到流之前就被触发；本次飞行没有录到视频。
        logger.warning(
            "video supervisor exited without successful probe — no video recorded "
            "for this flight (RTMP source {} never became reachable)",
            self.source_url,
        )

    def _launch_ffmpeg(self) -> None:
        """Called from supervisor thread once probe succeeds."""
        launch_ms = int(time.time() * 1000)
        self._filename = f"main_{launch_ms}.mp4"
        cmd = self._build_cmd(self._filename)

        log_path = self.output_dir / "main.ffmpeg.log"
        self._stderr_log = open(log_path, "w")  # noqa: WPS515 (kept open intentionally)

        self._proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=self._stderr_log,
        )
        self._segments.append({
            "file": self._filename,
            "ffmpeg_start_wall_ms": launch_ms,
            "pts_offset_ms": 0,
        })
        logger.info("ffmpeg launched: video/{} (pid={})", self._filename, self._proc.pid)

    def _build_cmd(self, filename: str) -> list[str]:
        return [
            "ffmpeg",
            "-y",
            "-i", self.source_url,
            # Only take video stream (+ optional audio); drop data streams that
            # some docks inject (e.g. "Stream Data:none") which break mp4 muxer.
            "-map", "0:v", "-map", "0:a?",
            "-c", "copy",
            "-movflags", "+faststart+frag_keyframe",
            "-f", "mp4",
            *self.extra_args,
            str(self.output_dir / filename),
        ]

    def _write_timing(self) -> None:
        (self.output_dir / "main.timing.json").write_text(
            json.dumps({"segments": self._segments}, indent=2)
        )


def resolve_video_source_url(video_cfg: dict) -> str | None:
    """解析视频拉流 URL。

    v1：只返回 source_url_override（去空白后），空/缺失返回 None。
    OSD 自动提取（source_url_field）延后实现。
    """
    return (video_cfg.get("source_url_override") or "").strip() or None
