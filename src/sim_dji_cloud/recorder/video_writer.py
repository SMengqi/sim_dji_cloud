import json
import signal
import subprocess
import threading
import time
from pathlib import Path
from typing import Callable, Optional

from loguru import logger


def _default_probe(url: str) -> bool:
    """Probe RTMP / HTTP-FLV stream by attempting a bounded short pull with ffmpeg.

    设计要点（三个坑都避开了）：

    1. **不**传 ``-timeout``。ffmpeg 的 RTMP demuxer 会把 ``-timeout`` 解读为
       "作为服务器 listen 等待客户端接入"的语义（URL 上会被改写出 ``?listen&listen_timeout=...``），
       ffprobe/ffmpeg 就尝试**绑定**到目标 IP（非本机地址）当 server，立刻报
       ``Cannot assign requested address``——根本不去当客户端连 dock，
       结果流明明在推，probe 也永远返回 False。

    2. **不用** ``ffprobe -show_entries`` 这条路。DJI dock 推的 H.264 里带自定义
       SEI（NAL type 245），ffprobe 处理这些 SEI 时会持续解析帧、不在 header
       阶段退出，整个 probe 卡死直到外层 timeout。

    3. **加 ``-c copy``**：第二版 probe 用 ``ffmpeg -t 1 -f null -`` 默认走的是
       decode+re-encode 到 null，DJI 自定义 SEI 让 decoder 慢得离谱；同时 RTMP
       handshake + 等 I-frame 也要花墙钟时间，I-frame 间隔 2~3s 是常态。两个一叠加，
       原来 ``-rw_timeout=2s`` + subprocess ``timeout=5s`` 在 P2P RTMP 链路上根本
       拿不到足够数据，probe 240 次全 False。加 ``-c copy`` 跳过 decode，
       同时拉长两个 timeout 到能跨过一个 I-frame 间隔。

    流存在 → 1 秒 PTS 内拷到字节，exit=0；流不在/URL 错 → 连接失败/socket 超时
    快速非 0 退出。``stderr`` 保留，出问题用 debug 看 ffmpeg 报错。
    """
    try:
        result = subprocess.run(
            [
                "ffmpeg",
                "-hide_banner", "-loglevel", "fatal", "-nostats",
                # ``fatal`` 而非 ``error``：在某些 ffmpeg 版本上 libavformat 的 H.264 bitstream
                # parser 会把 DJI 自定义 SEI（NAL 245）的 "truncated" 当 error level 喷出来，
                # 量很大、CPU 也要花在 fprintf 上。这些"错误"实际上不影响 demux 切包，
                # mpegts.js 能播、c copy 能录都是证据。fatal 级别只放真出大事的输出。
                "-rw_timeout", "5000000",   # 5s socket I/O 超时（cover RTMP handshake + I-frame 间隔）
                "-t", "1",                  # 拉到 1 秒 PTS 数据就退出
                "-i", url,
                "-c", "copy",               # 不 decode，直接拷字节到 null
                "-f", "null", "-",
            ],
            timeout=15,                     # subprocess 兜底：足够跨一个 GOP
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        if result.returncode == 0:
            return True
        # 非 0 退出时把 ffmpeg 的 stderr 印出来，方便下次诊断"为什么 probe 还失败"。
        err = (result.stderr or b"").decode("utf-8", errors="replace").strip()
        if err:
            logger.debug("video probe failed (exit={}): {}", result.returncode, err[:500])
        return False
    except subprocess.TimeoutExpired:
        logger.debug("video probe timed out after 15s pulling {}", url)
        return False
    except Exception as e:
        logger.debug("video probe errored: {}", e)
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
