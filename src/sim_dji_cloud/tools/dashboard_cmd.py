from urllib.parse import urlparse
import uvicorn
from loguru import logger

from sim_dji_cloud.dashboard import create_app, LiveState, MqttSubscriber


def run_dashboard(
    mqtt_url: str,
    host: str,
    port: int,
    ws_push_interval_ms: int = 2000,
) -> int:
    parsed = urlparse(mqtt_url)
    if parsed.scheme not in ("tcp", "mqtt"):
        print(f"ERROR: unsupported mqtt url scheme: {parsed.scheme}")
        return 2
    broker_host = parsed.hostname or "localhost"
    broker_port = parsed.port or 1883

    state = LiveState()
    app = create_app(state, ws_push_interval_ms=ws_push_interval_ms)

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
