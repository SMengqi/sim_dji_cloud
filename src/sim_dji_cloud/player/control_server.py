"""sim-dji play 子进程内嵌的 aiohttp control HTTP server。

绑 127.0.0.1:0 拿系统分配端口，启动后写 sidecar JSON 让 dashboard PlayController
读到端口。提供 pause / resume / seek / progress 四端点。

无 token：localhost only，同机同 user 信任。跨主机访问由 dashboard 转发 + 鉴权。
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional, Protocol

from aiohttp import web
from loguru import logger


class _PlayerControlProto(Protocol):
    async def pause(self) -> dict: ...
    async def resume(self) -> dict: ...
    async def seek(self, virt_ms: int) -> dict: ...
    def progress(self) -> dict: ...


class ControlServer:
    """运行时绑端口 + 写 sidecar，pause/resume/seek/progress 四端点。"""

    def __init__(self, player: _PlayerControlProto, sidecar_path: Path,
                 pid: int, started_at_ms: int):
        self._player = player
        self._sidecar_path = Path(sidecar_path)
        self._pid = pid
        self._started_at_ms = started_at_ms
        self._app = web.Application()
        self._app.router.add_post("/control/pause", self._h_pause)
        self._app.router.add_post("/control/resume", self._h_resume)
        self._app.router.add_post("/control/seek", self._h_seek)
        self._app.router.add_get("/control/progress", self._h_progress)
        self._runner: Optional[web.AppRunner] = None
        self._site: Optional[web.TCPSite] = None
        self.port: Optional[int] = None

    async def start(self) -> None:
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, host="127.0.0.1", port=0)
        await self._site.start()
        # 用 aiohttp 公开 API runner.addresses 拿端口；老代码访问内部属性
        # _site._server.sockets，aiohttp 改内部 → sidecar control_port=null
        # → pause/resume/seek 整套静默 503。fallback 老路径以容错。
        self.port = self._extract_port()
        if self.port is None:
            raise RuntimeError(
                "ControlServer.start: failed to determine bound port from aiohttp "
                "(checked runner.addresses and _site._server.sockets)"
            )
        self._sidecar_path.parent.mkdir(parents=True, exist_ok=True)
        self._sidecar_path.write_text(json.dumps({
            "control_port": self.port,
            "pid": self._pid,
            "started_at_ms": self._started_at_ms,
        }))
        logger.info("control server listening on 127.0.0.1:{} sidecar={}",
                    self.port, self._sidecar_path)

    def _extract_port(self) -> Optional[int]:
        # 公开 API：aiohttp >= 3.8 暴露 AppRunner.addresses (list of sockaddr tuples)
        addrs = getattr(self._runner, "addresses", None)
        if addrs:
            try:
                return int(addrs[0][1])
            except (IndexError, TypeError, ValueError):
                pass
        # Fallback：保留旧路径以防早期 aiohttp 或运行时 quirk
        try:
            sockets = self._site._server.sockets  # type: ignore[union-attr]
            if sockets:
                return int(sockets[0].getsockname()[1])
        except (AttributeError, IndexError, TypeError):
            pass
        return None

    async def stop(self) -> None:
        try:
            self._sidecar_path.unlink()
        except FileNotFoundError:
            pass
        if self._site is not None:
            await self._site.stop()
        if self._runner is not None:
            await self._runner.cleanup()
        logger.info("control server stopped port={} sidecar={}",
                    self.port, self._sidecar_path)

    async def _h_pause(self, _req: web.Request) -> web.Response:
        try:
            return web.json_response(await self._player.pause())
        except RuntimeError as e:
            return web.json_response({"detail": str(e)}, status=409)
        except Exception:
            logger.exception("_h_pause unexpected error")
            raise

    async def _h_resume(self, _req: web.Request) -> web.Response:
        try:
            return web.json_response(await self._player.resume())
        except RuntimeError as e:
            return web.json_response({"detail": str(e)}, status=409)
        except Exception:
            logger.exception("_h_resume unexpected error")
            raise

    async def _h_seek(self, req: web.Request) -> web.Response:
        try:
            body = await req.json()
        except Exception:
            return web.json_response(
                {"detail": "body must be valid JSON"}, status=400)
        if not isinstance(body, dict):
            return web.json_response(
                {"detail": "body must be JSON object"}, status=400)
        virt_ms = body.get("virt_ms")
        if not isinstance(virt_ms, int) or isinstance(virt_ms, bool) or virt_ms < 0:
            return web.json_response(
                {"detail": "virt_ms must be int >= 0"}, status=400)
        try:
            return web.json_response(await self._player.seek(virt_ms))
        except Exception:
            logger.exception("_h_seek unexpected error")
            raise

    async def _h_progress(self, _req: web.Request) -> web.Response:
        try:
            return web.json_response(self._player.progress())
        except Exception:
            logger.exception("_h_progress unexpected error")
            raise
