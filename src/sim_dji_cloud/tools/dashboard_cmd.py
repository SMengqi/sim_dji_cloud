from pathlib import Path
from urllib.parse import urlparse
import uvicorn
from loguru import logger

from sim_dji_cloud.dashboard import create_app, LiveState, MqttSubscriber
from sim_dji_cloud.dashboard.flight_area import load_flight_area, parse_png_bounds


def run_dashboard(
    mqtt_url: str,
    host: str,
    port: int,
    ws_push_interval_ms: int = 2000,
    flight_area_xml: str | None = None,
    flight_area_png: str | None = None,
    flight_area_png_bounds: str | None = None,
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
            flight_area = load_flight_area(
                Path(flight_area_xml), parse_png_bounds(flight_area_png_bounds))
            logger.info("flight area loaded: {} 个区域", len(flight_area["areas"]["features"]))
        except Exception:
            logger.exception("加载飞行区失败；dashboard 将不带叠加运行")
            flight_area = None
    elif flight_area_png:
        logger.warning("提供了 --flight-area-png 但未提供 --flight-area-xml，PNG 已忽略")

    state = LiveState()
    app = create_app(
        state,
        ws_push_interval_ms=ws_push_interval_ms,
        flight_area=flight_area,
        flight_area_png=Path(flight_area_png) if (flight_area and flight_area_png) else None,
    )

    sub_holder: dict = {"sub": None}

    @app.on_event("startup")
    async def _on_startup() -> None:
        sub = MqttSubscriber(state, host=broker_host, port=broker_port,
                             client_id="sim-dji-dashboard")
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
