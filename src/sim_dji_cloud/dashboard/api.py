import asyncio
import csv
import io
import json
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Depends, FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from loguru import logger
from pydantic import BaseModel

from sim_dji_cloud.dashboard.auth import require_token
from sim_dji_cloud.dashboard.events_archive import (
    EventsArchive,
    read_archive_from_flight_dir,
)
from sim_dji_cloud.dashboard.live_state import LiveState
from sim_dji_cloud.dashboard.play_controller import (
    PlayController,
    PlayAlreadyRunning,
    NotRunning,
)


class PlayStartBody(BaseModel):
    flight_dir: str
    speed: float = 1.0
    mqtt_url: str = "tcp://localhost:1883"
    video_push_url: str | None = None
    video_anchor_offset_ms: int = 0


_STATIC_DIR = Path(__file__).parent / "static"


def create_app(
    state: LiveState,
    ws_push_interval_ms: int = 2000,
    flight_area: dict | None = None,
    flight_area_png: Path | None = None,
    video_url: str | None = None,
    archive: EventsArchive | None = None,
    recordings_root: Path = Path("recordings"),
    play_controller: PlayController | None = None,
    default_video_push_url: str = "",
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
        html = html_path.read_text(encoding="utf-8")
        if default_video_push_url:
            html = html.replace(
                '<meta name="default-video-push-url" content="">',
                f'<meta name="default-video-push-url" content="{default_video_push_url}">',
            )
        return HTMLResponse(html)

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

    if play_controller is not None:
        app.include_router(_play_router(play_controller))

    app.include_router(_flights_router(Path(recordings_root)))

    app.include_router(_state_reset_router(state, archive))

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


def _play_router(pc: PlayController) -> APIRouter:
    r = APIRouter(prefix="/api/play")

    @r.post("/start", status_code=201)
    def start_play(body: PlayStartBody, _=Depends(require_token)):
        try:
            return pc.start(
                body.flight_dir,
                speed=body.speed,
                mqtt_url=body.mqtt_url,
                video_push_url=body.video_push_url,
                video_anchor_offset_ms=body.video_anchor_offset_ms,
            )
        except PlayAlreadyRunning as e:
            raise HTTPException(status_code=409,
                                detail=f"play already running, pid={e.pid}")
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @r.post("/stop")
    def stop_play(_=Depends(require_token)):
        try:
            return pc.stop()
        except NotRunning:
            raise HTTPException(status_code=404, detail="no play running")

    @r.get("/status")
    def get_play_status():
        return pc.status()

    return r


def _scan_flights(recordings_root: Path) -> list[dict]:
    """扫 recordings_root 下子目录，提取 5 字段，按 started_at_ms 倒序。

    跳过：非目录 / 以 . 开头 / 缺 manifest.json / manifest 损坏。
    """
    if not recordings_root.is_dir():
        return []
    flights = []
    for child in recordings_root.iterdir():
        if not child.is_dir() or child.name.startswith("."):
            continue
        manifest_path = child / "manifest.json"
        if not manifest_path.exists():
            continue
        try:
            m = json.loads(manifest_path.read_text())
        except (ValueError, OSError):
            logger.warning(
                "flights scan: skip {} (manifest unreadable)", child.name,
            )
            continue
        started = m.get("started_at_recv_ms")
        ended = m.get("ended_at_recv_ms")
        duration = (
            (ended - started)
            if (isinstance(started, int) and isinstance(ended, int))
            else None
        )
        video = m.get("video") or {}
        has_video = bool(video.get("file"))
        flights.append({
            "id": child.name,
            "started_at_ms": started,
            "duration_ms": duration,
            "has_video": has_video,
            "dock_sn": m.get("dock_sn", ""),
        })
    flights.sort(key=lambda f: f["started_at_ms"] or 0, reverse=True)
    return flights


def _flights_router(recordings_root: Path) -> APIRouter:
    r = APIRouter(prefix="/api")

    @r.get("/flights")
    def list_flights():
        return {"flights": _scan_flights(recordings_root)}

    return r


def _state_reset_router(
    state: LiveState, archive: EventsArchive | None,
) -> APIRouter:
    """POST /api/state/reset — 鉴权后清 LiveState（含 trail / events / controls /
    topic_counts / known_*_sn）+ EventsArchive（若挂载）。

    无条件挂载：state 必传；archive 可能为 None（handler 内部 guard）。
    require_token 走 secure-by-default：DASHBOARD_TOKEN 未设 → 503，header 缺/错 → 401。
    """
    r = APIRouter(prefix="/api/state")

    @r.post("/reset")
    def reset_state(_=Depends(require_token)):
        state.reset()
        if archive is not None:
            archive.reset()
        return {"state": "reset"}

    return r
