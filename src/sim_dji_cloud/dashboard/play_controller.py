"""管 sim-dji play 子进程的启停 + 状态查询。

跟 run.sh launch/stop 共享同一份 pid 文件 + 日志目录，双向互操作。
设计详见 2026-06-02-sim-dji-cloud-http-control-design.md。
"""
from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import time
from pathlib import Path
from typing import Optional

import httpx
from loguru import logger


class PlayAlreadyRunning(Exception):
    """已有 play 子进程在跑（路由层翻译成 409）。"""

    def __init__(self, pid: int):
        super().__init__(f"play already running, pid={pid}")
        self.pid = pid


class NotRunning(Exception):
    """stop 时发现没在跑（路由层翻译成 404）。"""


class ControlUnavailable(Exception):
    """control server 未起 / 不可达（路由层翻译成 503）。"""


class PlayController:
    """管 sim-dji play 子进程；线程不安全，单 instance per dashboard 进程。"""

    # sidecar wait deadline (seconds). 缩短自历史 5s → 2s：control sidecar
    # 由 sim-dji play 内的 aiohttp ControlServer 在子进程 import + asyncio
    # bootstrap 完即可写出，冷启动实测 < 1s；2s 给冷 venv / NFS 也够。
    # Sprint 2: 缩短让 sidecar 没出现时 503 更快暴露给 dashboard。
    SIDECAR_WAIT_SECONDS = 2.0

    def __init__(self, recordings_root: Path, log_dir: Path, name: str = "play"):
        self.recordings_root = recordings_root.resolve()
        self.log_dir = log_dir
        self.name = name
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self._pid_file = log_dir / f"{name}.pid"
        self._meta_file = log_dir / f"{name}.meta.json"
        self._latest_log = log_dir / f"{name}-latest.log"
        self._control_sidecar = log_dir / f"{name}.control.json"
        self._last_progress: Optional[dict] = None
        self._progress_stale: bool = False

    def start(
        self,
        flight_dir: str,
        *,
        speed: float = 1.0,
        mqtt_url: str = "tcp://localhost:1883",
        video_push_url: Optional[str] = None,
        video_anchor_offset_ms: int = 0,
    ) -> dict:
        """Sync 启动 sim-dji play 子进程；阻塞最长 ``SIDECAR_WAIT_SECONDS``。

        async 上下文（FastAPI handler）请用 ``astart`` 避免冻 loop。
        """
        proc, deadline = self._prepare_start_subprocess(
            flight_dir, speed=speed, mqtt_url=mqtt_url,
            video_push_url=video_push_url,
            video_anchor_offset_ms=video_anchor_offset_ms,
        )
        # Sync sidecar 等待：每 100ms 轮询，最长 SIDECAR_WAIT_SECONDS。
        while time.monotonic() < deadline:
            outcome = self._check_sidecar(proc)
            if outcome is not None:
                break
            time.sleep(0.1)
        else:
            self._log_sidecar_timeout()
        logger.info("PlayController started: pid={} flight_dir={}",
                    proc.pid, self._meta_file)
        return self.status()

    async def astart(
        self,
        flight_dir: str,
        *,
        speed: float = 1.0,
        mqtt_url: str = "tcp://localhost:1883",
        video_push_url: Optional[str] = None,
        video_anchor_offset_ms: int = 0,
    ) -> dict:
        """Async 启动 sim-dji play 子进程。

        跟 ``start()`` 行为对等，但 sidecar 等待用 ``await asyncio.sleep``
        而不是 ``time.sleep``——dashboard FastAPI handler 改 ``async def`` 后
        整条链路不再阻塞任何线程，HTTP 客户端超时也不会霸占 anyio threadpool。
        """
        proc, deadline = self._prepare_start_subprocess(
            flight_dir, speed=speed, mqtt_url=mqtt_url,
            video_push_url=video_push_url,
            video_anchor_offset_ms=video_anchor_offset_ms,
        )
        while time.monotonic() < deadline:
            outcome = self._check_sidecar(proc)
            if outcome is not None:
                break
            await asyncio.sleep(0.1)
        else:
            self._log_sidecar_timeout()
        logger.info("PlayController started (async): pid={}", proc.pid)
        return self.status()

    def _prepare_start_subprocess(
        self,
        flight_dir: str,
        *,
        speed: float,
        mqtt_url: str,
        video_push_url: Optional[str],
        video_anchor_offset_ms: int,
    ) -> tuple[subprocess.Popen, float]:
        """同步 Popen 起 sim-dji play + 写 pid/meta/symlink。

        返回 (proc, deadline_monotonic)。供 ``start()`` 同步路径和 ``astart()``
        异步路径共享，避免双倍 ftruncate/symlink/Popen 逻辑。
        """
        resolved = self._resolve_flight_dir(flight_dir)

        cur = self.status()
        if cur["state"] == "running":
            raise PlayAlreadyRunning(cur["pid"])

        # Clear progress cache from previous flight (if any).
        self._last_progress = None
        self._progress_stale = False

        ts = time.strftime("%Y%m%d-%H%M%S")
        log_path = self.log_dir / f"{self.name}-{ts}.log"
        cmd = [
            "sim-dji", "play", str(resolved),
            "--mqtt-url", mqtt_url,
            "--speed", str(speed),
        ]
        if video_push_url:
            cmd += ["--video-push-url", video_push_url]
        if video_anchor_offset_ms:
            cmd += ["--video-anchor-offset-ms", str(video_anchor_offset_ms)]
        cmd += ["--control-sidecar-path", str(self._control_sidecar)]

        # with-block 保证 Popen 抛错时 log_fp 被关；Popen 已 dup 了 fd，
        # Python 句柄关闭不影响子进程继续写。
        # Regression: test_start_closes_log_fp_when_popen_fails.
        with open(log_path, "ab") as log_fp:
            proc = subprocess.Popen(
                cmd,
                stdout=log_fp,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
            )

        self._pid_file.write_text(f"{proc.pid}\n")
        started_at_ms = int(time.time() * 1000)
        self._meta_file.write_text(json.dumps({
            "flight_dir": str(resolved),
            "speed": speed,
            "started_at_ms": started_at_ms,
        }))
        if self._latest_log.exists() or self._latest_log.is_symlink():
            self._latest_log.unlink()
        self._latest_log.symlink_to(log_path.name)

        deadline = time.monotonic() + self.SIDECAR_WAIT_SECONDS
        return proc, deadline

    def _check_sidecar(self, proc: subprocess.Popen) -> Optional[str]:
        """Sidecar 状态机一次性 poll。

        返回：
          - "ready"   sidecar 已写
          - "dead"    子进程已退（sidecar 不会再出现）
          - None      还在等
        """
        if self._control_sidecar.exists():
            return "ready"
        rc = proc.poll()
        if rc is not None:
            logger.warning(
                "play process exited early (rc={}); sidecar will not appear", rc,
            )
            return "dead"
        return None

    def _log_sidecar_timeout(self) -> None:
        logger.warning(
            "control sidecar timeout for {} ({}); "
            "pause/resume/seek will return 503",
            self.name, self._control_sidecar,
        )

    def stop(self, timeout_s: float = 10.0) -> dict:
        """Sync 停 sim-dji play 子进程；阻塞最长 ``timeout_s``。

        async 上下文（FastAPI handler）请用 ``astop`` 避免冻 loop。
        """
        pid = self._send_term_and_get_pid()
        if pid is None:
            return self._finalize_stop()
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if not self._pid_alive(pid):
                break
            time.sleep(0.2)
        else:
            self._sigkill_after_timeout(pid)
        return self._finalize_stop()

    async def astop(self, timeout_s: float = 10.0) -> dict:
        """Async 停 sim-dji play 子进程。

        跟 ``stop()`` 行为对等，但 SIGTERM 等待用 ``await asyncio.sleep``
        而不是 ``time.sleep``——FastAPI handler 不会被冻 ``timeout_s`` 秒，
        并发 stop 请求不再霸占 anyio threadpool。
        """
        pid = self._send_term_and_get_pid()
        if pid is None:
            return self._finalize_stop()
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if not self._pid_alive(pid):
                break
            await asyncio.sleep(0.2)
        else:
            self._sigkill_after_timeout(pid)
        return self._finalize_stop()

    def _send_term_and_get_pid(self) -> Optional[int]:
        """SIGTERM 给当前 pid；ProcessLookupError 或 not-running 时返 None。

        not-running → 抛 NotRunning（路由层 404）；进程已死 → 清状态文件返 None。
        """
        cur = self.status()
        if cur["state"] != "running":
            raise NotRunning()
        pid = cur["pid"]
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            return None
        return pid

    def _sigkill_after_timeout(self, pid: int) -> None:
        logger.warning(
            "PlayController: pid {} SIGTERM 超时；SIGKILL 兜底", pid,
        )
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    def _finalize_stop(self) -> dict:
        self._cleanup_state_files()
        try:
            self._control_sidecar.unlink()
        except FileNotFoundError:
            pass
        return self.status()

    def _status_basic(self) -> dict:
        if not self._pid_file.exists():
            return self._empty_status()
        try:
            pid = int(self._pid_file.read_text().strip())
        except (ValueError, OSError):
            self._cleanup_state_files()
            return self._empty_status()
        if not self._pid_alive(pid):
            self._cleanup_state_files()
            return self._empty_status()
        meta: dict = {}
        if self._meta_file.exists():
            try:
                meta = json.loads(self._meta_file.read_text())
            except (ValueError, OSError):
                meta = {}
        return {
            "state": "running",
            "pid": pid,
            "flight_dir": meta.get("flight_dir"),
            "speed": meta.get("speed"),
            "started_at_ms": meta.get("started_at_ms"),
            "log_tail": self._tail_log(),
        }

    def status(self) -> dict:
        """对外接口；running 时塞 progress 字段。"""
        s = self._status_basic()
        if s["state"] == "running":
            s["progress"] = self._progress_with_stale_flag()
        else:
            s["progress"] = None
        return s

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _resolve_flight_dir(self, source: str) -> Path:
        if ".." in Path(source).parts:
            raise ValueError("flight_dir must not contain '..'")
        candidate = Path(source)
        if not candidate.is_absolute():
            candidate = self.recordings_root / candidate
        candidate = candidate.resolve()
        if not (candidate == self.recordings_root
                or self.recordings_root in candidate.parents):
            raise ValueError(f"flight_dir must be under {self.recordings_root}")
        if not candidate.is_dir():
            raise ValueError(f"flight_dir not found: {source}")
        return candidate

    @staticmethod
    def _pid_alive(pid: int) -> bool:
        """True iff pid exists AND is not a zombie.

        `os.kill(pid, 0)` alone is insufficient — zombie child processes
        still have a valid pid and signal 0 succeeds, but the process has
        actually exited. Read /proc/<pid>/status State: field to exclude `Z`;
        also call waitpid(WNOHANG) to reap the zombie if we are the parent.

        Linux-only (reads /proc); the project is Linux server-only.
        """
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            # Process exists but belongs to another user (we can't reap it);
            # treat as alive
            return True

        # Process exists; now check it is not a zombie
        try:
            with open(f"/proc/{pid}/status") as f:
                for line in f:
                    if line.startswith("State:"):
                        if line.split()[1] == "Z":
                            # Zombie detected; try to reap it (if we are parent;
                            # if not, ChildProcessError is raised and ignored)
                            try:
                                os.waitpid(pid, os.WNOHANG)
                            except (ChildProcessError, OSError):
                                pass
                            return False
                        break
        except (FileNotFoundError, OSError):
            # /proc read failed (rare); conservatively treat as not alive
            return False
        return True

    def _cleanup_state_files(self) -> None:
        for p in (self._pid_file, self._meta_file):
            try:
                p.unlink()
            except FileNotFoundError:
                pass

    def _tail_log(self, n: int = 20) -> Optional[str]:
        if not self._latest_log.exists():
            return None
        try:
            text = self._latest_log.read_text(errors="replace")
        except OSError:
            return None
        lines = text.splitlines()
        return "\n".join(lines[-n:]) if lines else None

    def _empty_status(self) -> dict:
        return {"state": "stopped", "pid": None, "flight_dir": None,
                "speed": None, "started_at_ms": None, "log_tail": None,
                "progress": None}

    # ==================================================================
    # Pause / Resume / Seek + progress polling  (phase 3 A)
    # ==================================================================

    def pause(self) -> dict:
        self._ensure_running()
        return self._forward_control("POST", "/control/pause", body={})

    def resume(self) -> dict:
        self._ensure_running()
        return self._forward_control("POST", "/control/resume", body={})

    def seek(self, virt_ms) -> dict:
        if not isinstance(virt_ms, int) or isinstance(virt_ms, bool) or virt_ms < 0:
            raise ValueError("virt_ms must be int >= 0")
        self._ensure_running()
        return self._forward_control(
            "POST", "/control/seek", body={"virt_ms": virt_ms})

    def _ensure_running(self) -> None:
        if self._status_basic()["state"] != "running":
            raise NotRunning()

    def _progress_with_stale_flag(self) -> Optional[dict]:
        if self._last_progress is None:
            return None
        out = dict(self._last_progress)
        out["stale"] = self._progress_stale
        return out

    def _read_control_port(self) -> Optional[int]:
        if not self._control_sidecar.exists():
            return None
        try:
            return json.loads(self._control_sidecar.read_text())["control_port"]
        except (ValueError, OSError, KeyError):
            return None

    def _forward_control(self, method: str, path: str, *, body: dict) -> dict:
        port = self._read_control_port()
        if port is None:
            raise ControlUnavailable("control server sidecar missing")
        url = f"http://127.0.0.1:{port}{path}"
        try:
            with httpx.Client(timeout=5.0) as c:
                if method.upper() == "GET":
                    r = c.request(method, url)
                else:
                    r = c.request(method, url, json=body)
            if r.status_code == 400:
                raise ValueError(r.json().get("detail", "bad request"))
            if r.status_code == 409:
                raise RuntimeError(r.json().get("detail", "conflict"))
            r.raise_for_status()
            return r.json()
        except (httpx.HTTPError, OSError) as e:
            raise ControlUnavailable(f"control HTTP failed: {e}") from e

    async def start_progress_polling(self) -> None:
        """1Hz 后台 poll；dashboard_cmd 在 uvicorn startup 钩 + shutdown cancel。
        永不抛出（all exceptions swallowed），让任务终生运行。

        _forward_control 用 sync httpx.Client（最高 5s 阻塞）。直接调会卡
        uvicorn 事件循环——control server 死 / sidecar 缺时整个 dashboard
        冻 5s。用 asyncio.to_thread 丢去 threadpool 跑。
        """
        while True:
            try:
                if self._status_basic()["state"] == "running":
                    self._last_progress = await asyncio.to_thread(
                        self._forward_control, "GET", "/control/progress",
                        body={},
                    )
                    self._progress_stale = False
            except Exception:
                self._progress_stale = True
            try:
                await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                return
