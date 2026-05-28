import json
from pathlib import Path

import pytest

from sim_dji_cloud.recorder import Recorder


class FakeVideoWriter:
    """测试替身：记录调用，不起 ffmpeg。签名与 VideoWriter 一致。"""

    # A synthetic epoch-ms filename matching the new convention.
    _FAKE_FILENAME = "main_1779937234567.mp4"
    _FAKE_START_MS = 1779937234567

    def __init__(self, source_url, output_dir, extra_args=None):
        self.source_url = source_url
        self.output_dir = Path(output_dir)
        self.extra_args = extra_args
        self.started = False
        self.stopped = False

    def start(self) -> None:
        self.started = True

    def stop(self, timeout_s=10.0):
        self.stopped = True

    def manifest_video_block(self, duration_ms):
        rel = f"video/{self._FAKE_FILENAME}"
        return {
            "file": rel,
            "source_url": self.source_url,
            "started_at_recv_ms": self._FAKE_START_MS,
            "duration_ms": duration_ms,
            "segments": [{"start_ms": 0, "end_ms": duration_ms, "file": rel}],
        }


def _cfg(tmp_path: Path, video: dict) -> dict:
    return {
        "mqtt": {
            "host": "localhost", "port": 1883, "tls": False,
            "client_id": "t", "username": None, "password": None,
            "ca_file": None, "cert_file": None, "key_file": None,
            "subscribe_patterns": [], "deny_topics": [],
        },
        "storage": {
            "root": str(tmp_path / "rec"), "enable_raw_firehose": False,
            "flush_max_records": 1, "flush_interval_ms": 50, "queue_max_size": 1000,
            "rotate_max_bytes": 10**9, "rotate_max_records": 10**6,
        },
        "video": video,
        "flight_detection": {"record_steps": [0, 1, 2], "idle_debounce_seconds": 0},
    }


async def _drive_flight(rec: Recorder) -> None:
    """dock flighttask_step=1 起录（@1000，带 sub_device 回填 drone_sn）→ step=5 空闲（@10000）。"""
    await rec.on_mqtt_message(
        "thing/product/SN_DOCK/osd",
        json.dumps({"data": {"flighttask_step_code": 1,
                             "sub_device": {"device_sn": "SN_DRONE"}}}).encode(),
        1000,
    )
    await rec.on_mqtt_message(
        "thing/product/SN_DOCK/osd",
        json.dumps({"data": {"flighttask_step_code": 1}}).encode(),
        1500,
    )
    await rec.on_mqtt_message(
        "thing/product/SN_DOCK/osd",
        json.dumps({"data": {"flighttask_step_code": 5}}).encode(),
        10_000,
    )


@pytest.mark.asyncio
async def test_video_started_and_manifest_set(tmp_path):
    created = []

    def factory(url, out, args):
        vw = FakeVideoWriter(url, out, args)
        created.append(vw)
        return vw

    cfg = _cfg(tmp_path, {"enabled": True,
                          "source_url_override": "rtmp://srs/live/x",
                          "ffmpeg_extra_args": []})
    rec = Recorder(cfg, dock_sn="SN_DOCK", drone_sn=None, video_writer_factory=factory)
    await rec.start_async_components()
    await _drive_flight(rec)
    flight_dir = await rec.finalize_and_close("auto_idle")

    import re
    assert len(created) == 1
    vw = created[0]
    assert vw.source_url == "rtmp://srs/live/x"
    assert vw.output_dir.name == "video"
    assert vw.started is True
    assert vw.stopped is True

    manifest = json.loads((flight_dir / "manifest.json").read_text())
    assert manifest["video"]["source_url"] == "rtmp://srs/live/x"
    assert re.match(r"^video/main_\d+\.mp4$", manifest["video"]["file"]), (
        f"Expected video/main_<ms>.mp4, got: {manifest['video']['file']}"
    )
    assert manifest["video"]["started_at_recv_ms"] == FakeVideoWriter._FAKE_START_MS


@pytest.mark.asyncio
async def test_video_disabled_no_writer(tmp_path):
    created = []
    cfg = _cfg(tmp_path, {"enabled": False})
    rec = Recorder(cfg, dock_sn="SN_DOCK", drone_sn=None,
                   video_writer_factory=lambda *a: created.append(a))
    await rec.start_async_components()
    await _drive_flight(rec)
    flight_dir = await rec.finalize_and_close("auto_idle")

    assert created == []
    manifest = json.loads((flight_dir / "manifest.json").read_text())
    assert manifest["video"] is None


@pytest.mark.asyncio
async def test_video_enabled_but_no_override_skips(tmp_path):
    created = []
    cfg = _cfg(tmp_path, {"enabled": True, "source_url_override": ""})
    rec = Recorder(cfg, dock_sn="SN_DOCK", drone_sn=None,
                   video_writer_factory=lambda *a: created.append(a))
    await rec.start_async_components()
    await _drive_flight(rec)
    flight_dir = await rec.finalize_and_close("auto_idle")

    assert created == []
    manifest = json.loads((flight_dir / "manifest.json").read_text())
    assert manifest["video"] is None


@pytest.mark.asyncio
async def test_video_start_failure_is_non_fatal(tmp_path):
    def boom(url, out, args):
        raise RuntimeError("ffmpeg not found")

    cfg = _cfg(tmp_path, {"enabled": True,
                          "source_url_override": "rtmp://srs/live/x",
                          "ffmpeg_extra_args": []})
    rec = Recorder(cfg, dock_sn="SN_DOCK", drone_sn=None, video_writer_factory=boom)
    await rec.start_async_components()
    await _drive_flight(rec)
    flight_dir = await rec.finalize_and_close("auto_idle")
    manifest = json.loads((flight_dir / "manifest.json").read_text())
    assert manifest["video"] is None
    assert (flight_dir / "topics").exists()


@pytest.mark.asyncio
async def test_video_stop_failure_is_non_fatal(tmp_path):
    # finalize 时 stop() 抛异常也不能炸；manifest.video 置 null，飞行仍正常收尾
    class BoomOnStop(FakeVideoWriter):
        def stop(self, timeout_s=10.0):
            raise OSError("ffmpeg stop failed")

    cfg = _cfg(tmp_path, {"enabled": True,
                          "source_url_override": "rtmp://srs/live/x",
                          "ffmpeg_extra_args": []})
    rec = Recorder(cfg, dock_sn="SN_DOCK", drone_sn=None,
                   video_writer_factory=lambda u, o, a: BoomOnStop(u, o, a))
    await rec.start_async_components()
    await _drive_flight(rec)
    flight_dir = await rec.finalize_and_close("auto_idle")
    manifest = json.loads((flight_dir / "manifest.json").read_text())
    assert manifest["video"] is None
    assert (flight_dir / "manifest.json").exists()


@pytest.mark.asyncio
async def test_rename_updates_video_output_dir(tmp_path):
    created = []

    def factory(url, out, args):
        vw = FakeVideoWriter(url, out, args)
        created.append(vw)
        return vw

    cfg = _cfg(tmp_path, {"enabled": True,
                          "source_url_override": "rtmp://srs/live/x",
                          "ffmpeg_extra_args": []})
    rec = Recorder(cfg, dock_sn="SN_DOCK", drone_sn=None, video_writer_factory=factory)

    # 直接驱动内部：task_id 未知 -> pending 目录 + 起视频
    await rec._open_flight_dir(1000)
    assert created[0].output_dir.parent.name.startswith("pending_")

    # 拿到 task_id -> 改名，视频 output_dir 应跟到新目录
    await rec._rename_pending_to_task("T1")
    assert not rec.flight_dir.name.startswith("pending_"), \
        f"Expected rename to have happened, got: {rec.flight_dir.name}"
    assert created[0].output_dir == rec.flight_dir / "video"
