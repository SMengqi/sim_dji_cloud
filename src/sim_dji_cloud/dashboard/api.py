import asyncio
import csv
import io
import json
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from loguru import logger

from sim_dji_cloud.dashboard.events_archive import (
    EventsArchive,
    read_archive_from_flight_dir,
)
from sim_dji_cloud.dashboard.live_state import LiveState


_STATIC_DIR = Path(__file__).parent / "static"


def create_app(
    state: LiveState,
    ws_push_interval_ms: int = 2000,
    flight_area: dict | None = None,
    flight_area_png: Path | None = None,
    video_url: str | None = None,
    archive: EventsArchive | None = None,
    recordings_root: Path = Path("recordings"),
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

    if archive is not None:
        app.include_router(_timeline_router(archive, Path(recordings_root)))

    return app


def _timeline_router(archive: EventsArchive, recordings_root: Path) -> APIRouter:
    r = APIRouter(prefix="/api/timeline")
    recordings_root_resolved = recordings_root.resolve()

    def _resolve_flight_dir(source: str) -> Path:
        if ".." in Path(source).parts:
            raise HTTPException(status_code=400,
                                detail="source must not contain '..'")
        candidate = Path(source)
        if not candidate.is_absolute():
            candidate = recordings_root / candidate
        candidate = candidate.resolve()
        if not (candidate == recordings_root_resolved
                or recordings_root_resolved in candidate.parents):
            raise HTTPException(
                status_code=400,
                detail=f"source must be under {recordings_root_resolved}",
            )
        if not candidate.is_dir():
            raise HTTPException(status_code=404,
                                detail=f"flight_dir not found: {source}")
        if not (candidate / "manifest.json").exists():
            raise HTTPException(status_code=404, detail="manifest.json missing")
        return candidate

    def _fetch(source: str, kinds: str, since_ms, until_ms, limit):
        kinds_tuple = tuple(k.strip() for k in kinds.split(",") if k.strip())
        if source == "live":
            session = archive.session_started_at_ms
            entries, truncated = archive.query(
                kinds=kinds_tuple, since_ms=since_ms,
                until_ms=until_ms, limit=limit,
            )
        else:
            flight_dir = _resolve_flight_dir(source)
            try:
                tmp_archive, session = read_archive_from_flight_dir(flight_dir)
            except FileNotFoundError as e:
                raise HTTPException(404, str(e))
            except ValueError as e:
                raise HTTPException(422, str(e))
            entries, truncated = tmp_archive.query(
                kinds=kinds_tuple, since_ms=since_ms,
                until_ms=until_ms, limit=limit,
            )
        return entries, session, truncated

    @r.get("")
    def get_timeline(
        source: str = "live",
        kinds: str = "event,control",
        since_ms: int | None = None,
        until_ms: int | None = None,
        limit: int = 5000,
    ):
        entries, session, truncated = _fetch(
            source, kinds, since_ms, until_ms, limit,
        )
        return {
            "source": source,
            "session_started_at_ms": session,
            "entries": entries,
            "truncated": truncated,
        }

    @r.get("/export.csv")
    def export_csv(
        source: str = "live",
        kinds: str = "event,control",
        since_ms: int | None = None,
        until_ms: int | None = None,
        limit: int = 5000,
    ):
        entries, session, _ = _fetch(
            source, kinds, since_ms, until_ms, limit,
        )
        if source == "live":
            ts_safe = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
            filename = f"timeline_live_{ts_safe}.csv"
        else:
            filename = f"timeline_{Path(source).name}.csv"

        def generate():
            buf = io.StringIO()
            writer = csv.writer(buf, lineterminator="\n")
            writer.writerow([
                "recv_ts_ms", "recv_ts_iso", "virt_offset_ms",
                "kind", "topic", "method", "payload_json",
            ])
            yield buf.getvalue()
            for e in entries:
                buf.seek(0)
                buf.truncate(0)
                iso = datetime.fromtimestamp(
                    e["recv_ts_ms"] / 1000.0, tz=timezone.utc,
                ).isoformat()
                virt = e["recv_ts_ms"] - session if session is not None else ""
                writer.writerow([
                    e["recv_ts_ms"], iso, virt,
                    e["kind"], e["topic"], e.get("method") or "",
                    json.dumps(e.get("payload"),
                               separators=(",", ":"), ensure_ascii=False),
                ])
                yield buf.getvalue()

        return StreamingResponse(
            generate(),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    return r
