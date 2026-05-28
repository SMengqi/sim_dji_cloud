import asyncio
import json
from pathlib import Path
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from loguru import logger

from sim_dji_cloud.dashboard.live_state import LiveState


_STATIC_DIR = Path(__file__).parent / "static"


def create_app(
    state: LiveState,
    ws_push_interval_ms: int = 2000,
    flight_area: dict | None = None,
    flight_area_png: Path | None = None,
    video_url: str | None = None,
) -> FastAPI:
    app = FastAPI(title="sim-dji dashboard")

    @app.get("/api/health")
    async def health() -> dict:
        return {"ok": True}

    @app.get("/api/flight-area")
    async def flight_area_meta() -> JSONResponse:
        if flight_area is None:
            return JSONResponse({"configured": False})
        body = {
            "configured": True,
            "png_bounds": flight_area["png_bounds"],
            "areas": flight_area["areas"],
        }
        # 仅在确有 PNG 时才广告其 URL，避免前端去拉一个必然 404 的底图
        if flight_area_png is not None:
            body["png_url"] = "/api/flight-area/background.png"
        return JSONResponse(body)

    @app.get("/api/flight-area/background.png")
    async def flight_area_png_file() -> FileResponse:
        if flight_area_png is None:
            raise HTTPException(status_code=404, detail="flight area png not configured")
        if not flight_area_png.exists():
            raise HTTPException(status_code=404, detail="flight area png not found on disk")
        return FileResponse(
            str(flight_area_png),
            media_type="image/png",
            headers={"Cache-Control": "public, max-age=86400"},
        )

    @app.get("/api/video")
    async def video_meta() -> JSONResponse:
        if not video_url:
            return JSONResponse({"configured": False})
        return JSONResponse({"configured": True, "url": video_url, "type": "flv"})

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

    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

    return app
