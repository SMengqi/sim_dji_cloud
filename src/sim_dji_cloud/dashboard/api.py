import asyncio
import json
from pathlib import Path
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from loguru import logger

from sim_dji_cloud.dashboard.live_state import LiveState


_STATIC_DIR = Path(__file__).parent / "static"


def create_app(state: LiveState, ws_push_interval_ms: int = 500) -> FastAPI:
    app = FastAPI(title="sim-dji dashboard")

    @app.get("/api/health")
    async def health() -> dict:
        return {"ok": True}

    @app.get("/api/snapshot")
    async def snapshot() -> JSONResponse:
        return JSONResponse(state.snapshot())

    @app.get("/", response_class=HTMLResponse)
    async def root() -> HTMLResponse:
        html_path = _STATIC_DIR / "index.html"
        if not html_path.exists():
            return HTMLResponse(
                "<!doctype html><html><body><p>dashboard ui (index.html missing)</p></body></html>"
            )
        return HTMLResponse(html_path.read_text(encoding="utf-8"))

    @app.websocket("/ws/stream")
    async def ws_stream(ws: WebSocket) -> None:
        await ws.accept()
        try:
            while True:
                await ws.send_text(json.dumps(state.snapshot(), separators=(",", ":")))
                await asyncio.sleep(ws_push_interval_ms / 1000.0)
        except WebSocketDisconnect:
            return
        except Exception:
            logger.exception("dashboard ws error")
            return

    return app
