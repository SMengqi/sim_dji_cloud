from pathlib import Path
from urllib.parse import urlparse
import uvicorn
from loguru import logger

from sim_dji_cloud.dashboard import create_app, LiveState, MqttSubscriber
from sim_dji_cloud.dashboard.events_archive import EventsArchive
from sim_dji_cloud.dashboard.flight_area import (
    load_flight_area, parse_png_bounds, parse_png_bounds_latlon, read_sidecar_bounds,
)
from sim_dji_cloud.dashboard.play_controller import PlayController


def run_dashboard(
    mqtt_url: str,
    host: str,
    port: int,
    ws_push_interval_ms: int = 2000,
    flight_area_xml: str | None = None,
    flight_area_png: str | None = None,
    flight_area_png_bounds: str | None = None,
    flight_area_png_bounds_latlon: str | None = None,
    video_url: str | None = None,
    recordings_root: Path = Path("recordings"),
    log_dir: Path = Path("logs"),
    default_video_push_url: str = "",
) -> int:
    parsed = urlparse(mqtt_url)
    if parsed.scheme not in ("tcp", "mqtt"):
        print(f"ERROR: unsupported mqtt url scheme: {parsed.scheme}")
        return 2
    broker_host = parsed.hostname or "localhost"
    broker_port = parsed.port or 1883

    flight_area = None
    if flight_area_xml:
        try:
            utm = parse_png_bounds(flight_area_png_bounds)
            latlon = parse_png_bounds_latlon(flight_area_png_bounds_latlon)
            # 都没传时，回退读取 PNG 旁的 sidecar <png>.bounds（校准产出，固化对位）
            if utm is None and latlon is None and flight_area_png:
                latlon = read_sidecar_bounds(Path(flight_area_png))
                if latlon is not None:
                    logger.info("PNG 边界自 sidecar 读取：{}.bounds", Path(flight_area_png).name)
            flight_area = load_flight_area(
                Path(flight_area_xml), png_bounds_utm=utm, png_bounds_latlon=latlon)
            logger.info("flight area loaded: {} 个区域", len(flight_area["areas"]["features"]))
        except Exception:
            logger.exception("加载飞行区失败；dashboard 将不带叠加运行")
            flight_area = None
    elif flight_area_png:
        logger.warning("提供了 --flight-area-png 但未提供 --flight-area-xml，PNG 已忽略")

    archive = EventsArchive(soft_cap=5000)
    state = LiveState(on_flight_idle=[archive.reset])
    play_controller = PlayController(
        recordings_root=recordings_root,
        log_dir=log_dir,
    )
    app = create_app(
        state,
        ws_push_interval_ms=ws_push_interval_ms,
        flight_area=flight_area,
        flight_area_png=Path(flight_area_png) if (flight_area and flight_area_png) else None,
        video_url=video_url,
        archive=archive,
        recordings_root=recordings_root,
        play_controller=play_controller,
        default_video_push_url=default_video_push_url,
    )

    @app.on_event("startup")
    async def _start_progress_polling():
        import asyncio
        app.state._progress_task = asyncio.create_task(
            play_controller.start_progress_polling())

    @app.on_event("shutdown")
    async def _stop_progress_polling():
        import asyncio
        t = getattr(app.state, "_progress_task", None)
        if t is not None:
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass

    sub_holder: dict = {"sub": None}

    @app.on_event("startup")
    async def _on_startup() -> None:
        sub = MqttSubscriber(state, host=broker_host, port=broker_port,
                             client_id="sim-dji-dashboard", archive=archive)
        try:
            await sub.connect()
            sub_holder["sub"] = sub
            logger.info("dashboard subscribed to tcp://{}:{}", broker_host, broker_port)
        except Exception:
            logger.exception("dashboard subscriber failed to connect; UI will be live but state empty")

    @app.on_event("shutdown")
    async def _on_shutdown() -> None:
        if sub_holder["sub"]:
            await sub_holder["sub"].disconnect()

    logger.info("dashboard serving on http://{}:{}", host, port)
    config = uvicorn.Config(app, host=host, port=port, log_level="info")
    server = uvicorn.Server(config)
    server.run()
    return 0
