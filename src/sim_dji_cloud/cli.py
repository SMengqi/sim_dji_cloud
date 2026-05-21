import asyncio
import signal
from pathlib import Path
import click

from sim_dji_cloud.logging_setup import configure_logging
from sim_dji_cloud.config import load_config
from sim_dji_cloud.tools.inspect_cmd import inspect_flight
from sim_dji_cloud.tools.validate_config_cmd import validate_config_file
from sim_dji_cloud.tools.stop_record_cmd import stop_record as do_stop_record


@click.group()
@click.option("--log-level", default="INFO")
@click.option("--log-file", default=None)
def main(log_level: str, log_file: str | None) -> None:
    configure_logging(level=log_level, log_file=log_file)


@main.command("validate-config")
@click.option("--config", "config_path", required=True, type=click.Path(exists=True, dir_okay=False))
def validate_config_cmd(config_path: str) -> None:
    raise SystemExit(validate_config_file(Path(config_path)))


@main.command("inspect")
@click.argument("flight_dir", type=click.Path(exists=True, file_okay=False))
def inspect_cmd(flight_dir: str) -> None:
    raise SystemExit(inspect_flight(Path(flight_dir)))


@main.command("stop-record")
@click.argument("task_id")
@click.option("--state-dir", default=".sim-dji-state", type=click.Path())
def stop_record_cmd(task_id: str, state_dir: str) -> None:
    raise SystemExit(do_stop_record(task_id=task_id, state_dir=Path(state_dir)))


@main.command("record")
@click.option("--config", "config_path", required=True, type=click.Path(exists=True, dir_okay=False))
@click.option("--video/--no-video", default=None, help="overrides yaml video.enabled")
@click.option("--storage-root", default=None, type=click.Path())
@click.option("--state-dir", default=".sim-dji-state", type=click.Path())
def record_cmd(config_path: str, video: bool | None, storage_root: str | None, state_dir: str) -> None:
    from sim_dji_cloud.recorder import Recorder
    from sim_dji_cloud.recorder.mqtt_client import MqttRecorderClient, MqttConfig
    from sim_dji_cloud.recorder.stop_signal import StopSignalFile, read_stop_signal_if_present
    from sim_dji_cloud.recorder.flight_detector import FlightState

    cfg = load_config(Path(config_path))
    if video is not None:
        cfg["video"]["enabled"] = video
    if storage_root is not None:
        cfg["storage"]["root"] = storage_root

    dock_sn = cfg["mqtt"].get("dock_sn") or click.prompt("dock_sn (机场 SN)", type=str)

    async def run():
        rec = Recorder(cfg, dock_sn=dock_sn, drone_sn=None)
        await rec.start_async_components()

        async def on_msg(topic: str, payload: bytes, recv_ts_ms: int) -> None:
            await rec.on_mqtt_message(topic, payload, recv_ts_ms)

        m = cfg["mqtt"]
        mqtt = MqttRecorderClient(
            MqttConfig(
                host=m["host"], port=m["port"], tls=m["tls"],
                client_id=m["client_id"],
                username=m.get("username"), password=m.get("password"),
                ca_file=m.get("ca_file"), cert_file=m.get("cert_file"), key_file=m.get("key_file"),
                subscribe_patterns=m["subscribe_patterns"],
            ),
            on_message=on_msg,
        )
        loop_task = asyncio.create_task(mqtt.run_forever())

        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop_event.set)

        state_path = Path(state_dir)
        # 决定退出原因，优先级：manual_stop（信号/stop-file）→ auto_idle（detector）
        finalize_reason = "manual_stop"
        while not stop_event.is_set():
            await asyncio.sleep(1.0)
            # 检查 detector 是否已自动判定飞行结束（mode_code=0 sustain 30s 等）
            if rec._detector.state == FlightState.FINALIZING:
                finalize_reason = rec._detector.end_reason or "auto_idle"
                click.echo(f"auto-finalize: detector reached FINALIZING ({finalize_reason})")
                break
            # 检查外部 stop-record 命令写的信号文件
            if rec._detector.task_id:
                sig_file = StopSignalFile(state_path, rec._detector.task_id)
                if read_stop_signal_if_present(sig_file) is not None:
                    break

        await mqtt.stop()
        loop_task.cancel()
        if rec.flight_dir is not None:
            flight_dir = await rec.finalize_and_close(finalize_reason=finalize_reason)
            click.echo(f"finalized: {flight_dir}  (reason={finalize_reason})")

    asyncio.run(run())


if __name__ == "__main__":
    main()
