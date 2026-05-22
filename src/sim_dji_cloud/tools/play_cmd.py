from pathlib import Path
from urllib.parse import urlparse
from loguru import logger

from sim_dji_cloud.player import Player
from sim_dji_cloud.player.mqtt_publisher import MqttPublisher


async def play_flight(
    flight_dir: Path,
    mqtt_url: str,
    speed: float,
    start_offset_ms: int,
) -> int:
    parsed = urlparse(mqtt_url)
    if parsed.scheme not in ("tcp", "mqtt"):
        print(f"ERROR: unsupported mqtt url scheme: {parsed.scheme}")
        return 2
    host = parsed.hostname or "localhost"
    port = parsed.port or 1883

    publisher = MqttPublisher(host=host, port=port, client_id="sim-dji-player")
    player = Player(
        flight_dir=Path(flight_dir),
        publisher=publisher,
        speed=speed,
        start_offset_ms=start_offset_ms,
    )
    logger.info("play start: flight={}, broker=tcp://{}:{}, speed={}",
                flight_dir, host, port, speed)
    await player.start()
    await player.wait_until_done()
    logger.info("play done")
    return 0
