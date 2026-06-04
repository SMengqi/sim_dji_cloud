import asyncio
import signal
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
    video_push_url: str | None = None,
    video_anchor_offset_ms: int = 0,
    control_sidecar_path: Path | None = None,
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
        video_push_url=video_push_url,
        video_anchor_offset_ms=video_anchor_offset_ms,
        control_sidecar_path=control_sidecar_path,
    )

    # 接 SIGINT / SIGTERM：Python 默认对 SIGTERM 是"立即终止"，
    # Player.wait_until_done() 的 finally（里面会 VideoPusher.stop()
    # 给 ffmpeg 发 SIGINT 收尾）就跑不到，结果数据回放停了但 ffmpeg
    # 子进程被孤儿化继续推流。注册 handler 改成走 stop_event 路径，
    # 让 finally 跑到。`./run.sh stop play-video` 发的就是 SIGTERM。
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            # Windows asyncio loop 不支持；用户场景是 Linux nohup，不会走到这里。
            pass

    logger.info("play start: flight={}, broker=tcp://{}:{}, speed={}",
                flight_dir, host, port, speed)
    await player.start()

    wait_task = asyncio.create_task(player.wait_until_done())
    stop_task = asyncio.create_task(stop_event.wait())
    try:
        await asyncio.wait(
            {wait_task, stop_task}, return_when=asyncio.FIRST_COMPLETED,
        )
        if stop_event.is_set() and not wait_task.done():
            # 上游发了停止信号。取消 wait_task 让它的 finally 跑：
            # finally 会取消 publish 任务 + 调 VideoPusher.stop() (SIGINT→10s→SIGKILL)
            # + 断开 MQTT publisher。
            logger.info("play stopping (signal received)")
            wait_task.cancel()
            try:
                await wait_task
            except (asyncio.CancelledError, Exception):
                pass
    finally:
        # 不管走哪条路，stop_task 都收掉，避免悬挂。
        if not stop_task.done():
            stop_task.cancel()
            try:
                await stop_task
            except (asyncio.CancelledError, Exception):
                pass

    logger.info("play done")
    return 0
