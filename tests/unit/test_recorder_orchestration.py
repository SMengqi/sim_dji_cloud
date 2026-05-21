import json
import pytest
from pathlib import Path
from sim_dji_cloud.recorder import Recorder


@pytest.fixture
def minimal_config(tmp_path: Path):
    return {
        "mqtt": {
            "host": "localhost", "port": 1883, "tls": False,
            "client_id": "t", "username": None, "password": None,
            "ca_file": None, "cert_file": None, "key_file": None,
            "subscribe_patterns": [],
            "deny_topics": [],
        },
        "storage": {
            "root": str(tmp_path / "rec"),
            "enable_raw_firehose": False,
            "flush_max_records": 1,
            "flush_interval_ms": 50,
            "queue_max_size": 1000,
            "rotate_max_bytes": 10**9,
            "rotate_max_records": 10**6,
        },
        "video": {"enabled": False},
        "flight_detection": {
            "finalize_idle_seconds": 0,
            "rules": {
                "start": [{"source": "dock_services", "payload_match": {"method": "wayline_prepare"}}],
                "end": [{"source": "dock_osd", "field": "wayline_mission_state", "equals": "idle"}],
            },
        },
    }


@pytest.mark.asyncio
async def test_recorder_writes_jsonl_when_recording(minimal_config, tmp_path: Path):
    rec = Recorder(minimal_config, dock_sn="SN_DOCK", drone_sn=None)
    await rec.start_async_components()

    await rec.on_mqtt_message(
        topic="thing/product/SN_DOCK/services",
        payload=json.dumps({"method": "wayline_prepare", "data": {"flight_id": "T1"}}).encode(),
        recv_ts_ms=1000,
    )
    # dock_osd 必须含 data.sub_device.device_sn 才能回填 drone_sn
    await rec.on_mqtt_message(
        topic="thing/product/SN_DOCK/osd",
        payload=json.dumps({
            "wayline_mission_state": "executing",
            "data": {"sub_device": {"device_sn": "SN_DRONE"}},
        }).encode(),
        recv_ts_ms=1500,
    )
    await rec.on_mqtt_message(
        topic="thing/product/SN_DRONE/osd",
        payload=json.dumps({"mode_code": "manual_flight"}).encode(),
        recv_ts_ms=2000,
    )
    await rec.on_mqtt_message(
        topic="thing/product/SN_DOCK/osd",
        payload=json.dumps({"wayline_mission_state": "idle"}).encode(),
        recv_ts_ms=10_000,
    )

    flight_dir = await rec.finalize_and_close(finalize_reason="auto_idle")
    assert flight_dir.exists()
    assert (flight_dir / "manifest.json").exists()

    manifest = json.loads((flight_dir / "manifest.json").read_text())
    assert manifest["task_id"] == "T1"
    assert manifest["dock_sn"] == "SN_DOCK"
    assert manifest["drone_sn"] == "SN_DRONE"
    topics = {t["topic"] for t in manifest["topics"]}
    assert "thing/product/SN_DOCK/services" in topics
    assert "thing/product/SN_DOCK/osd" in topics
    assert "thing/product/SN_DRONE/osd" in topics


@pytest.mark.asyncio
async def test_drone_sn_backfilled_from_dock_osd_sub_device(minimal_config, tmp_path: Path):
    """关键修复：drone_sn 应从 dock_osd.data.sub_device.device_sn 精确提取，
    不再用'首条非 dock osd 即认为是飞行器'的启发式。"""
    cfg = dict(minimal_config)
    cfg["flight_detection"]["rules"]["start"] = [
        {"source": "dock_osd", "field": "wayline_mission_state", "not_equals": "idle"}
    ]
    rec = Recorder(cfg, dock_sn="SN_DOCK", drone_sn=None)
    await rec.start_async_components()

    # dock_osd 携带 sub_device.device_sn = "REAL_DRONE_SN"
    await rec.on_mqtt_message(
        "thing/product/SN_DOCK/osd",
        json.dumps({
            "wayline_mission_state": "executing",
            "data": {"sub_device": {"device_sn": "REAL_DRONE_SN"}},
        }).encode(),
        500,
    )
    assert rec.drone_sn == "REAL_DRONE_SN"

    flight_dir = await rec.finalize_and_close(finalize_reason="manual_stop")
    manifest = json.loads((flight_dir / "manifest.json").read_text())
    assert manifest["drone_sn"] == "REAL_DRONE_SN"


@pytest.mark.asyncio
async def test_post_rename_writes_continue_to_disk(minimal_config, tmp_path: Path):
    """关键回归：rename 之后 writer 不应失效。
    曾经的 bug：rename 时 drain+close 所有 writer 再重开，
    导致 writer/queue 状态错乱，rename 之后所有写入都停了。"""
    cfg = dict(minimal_config)
    cfg["flight_detection"]["rules"]["start"] = [
        {"source": "dock_osd", "field": "wayline_mission_state", "not_equals": "idle"}
    ]
    rec = Recorder(cfg, dock_sn="SN_DOCK", drone_sn=None)
    await rec.start_async_components()

    # Pre-rename: 触发 RECORDING + 回填 drone_sn
    await rec.on_mqtt_message(
        "thing/product/SN_DOCK/osd",
        json.dumps({
            "wayline_mission_state": "executing",
            "data": {"sub_device": {"device_sn": "SN_DRONE"}},
        }).encode(),
        100,
    )
    assert rec.flight_dir is not None
    assert rec.flight_dir.name.startswith("pending_")

    # 触发 rename
    await rec.on_mqtt_message(
        "thing/product/SN_DOCK/events",
        json.dumps({"method": "return_home_info",
                    "data": {"flight_id": "T-POST-RENAME"}}).encode(),
        200,
    )
    assert "T-POST-RENAME" in rec.flight_dir.name

    # Post-rename：模拟持续 30 条飞行器 osd（之前 bug 会丢全部）
    for i in range(30):
        await rec.on_mqtt_message(
            "thing/product/SN_DRONE/osd",
            json.dumps({"mode_code": 0, "seq": i}).encode(),
            300 + i * 100,
        )

    # 也持续录机场 osd 10 条
    for i in range(10):
        await rec.on_mqtt_message(
            "thing/product/SN_DOCK/osd",
            json.dumps({"wayline_mission_state": "executing", "i": i}).encode(),
            500 + i * 200,
        )

    flight_dir = await rec.finalize_and_close(finalize_reason="manual_stop")
    manifest = json.loads((flight_dir / "manifest.json").read_text())

    # 飞行器 osd: 30 条全部应在 manifest 里（rename 后写入）
    drone_osd = next(t for t in manifest["topics"]
                     if t["topic"] == "thing/product/SN_DRONE/osd")
    assert drone_osd["count"] == 30, \
        f"rename 后丢了飞行器 osd！期望 30，实际 {drone_osd['count']}"

    # 机场 osd: pre-rename 1 + post-rename 10 = 11
    dock_osd = next(t for t in manifest["topics"]
                    if t["topic"] == "thing/product/SN_DOCK/osd")
    assert dock_osd["count"] == 11, \
        f"rename 后丢了机场 osd！期望 11，实际 {dock_osd['count']}"

    # 同时校验磁盘上的 jsonl 内容
    drone_file = flight_dir / "topics" / "thing__product__SN_DRONE__osd.0001.jsonl"
    assert drone_file.exists()
    drone_lines = drone_file.read_text().strip().splitlines()
    assert len(drone_lines) == 30


@pytest.mark.asyncio
async def test_foreign_dock_topics_are_dropped(minimal_config, tmp_path: Path):
    """多机场共享 broker 时，非本机场 SN 的 topic 必须被 Recorder 丢弃，
    不能流入 manifest 或 jsonl。"""
    cfg = dict(minimal_config)
    cfg["flight_detection"]["rules"]["start"] = [
        {"source": "dock_osd", "field": "wayline_mission_state", "not_equals": "idle"}
    ]
    rec = Recorder(cfg, dock_sn="SN_DOCK", drone_sn=None)
    await rec.start_async_components()

    # 启动录制：dock_osd 携带 sub_device.device_sn
    await rec.on_mqtt_message(
        "thing/product/SN_DOCK/osd",
        json.dumps({
            "wayline_mission_state": "executing",
            "data": {"sub_device": {"device_sn": "SN_OURS_DRONE"}},
        }).encode(),
        500,
    )

    # 其他机场的 osd / drc / services 全部应被丢弃
    await rec.on_mqtt_message(
        "thing/product/SN_OTHER_DOCK/osd",
        json.dumps({"data": {"sub_device": {"device_sn": "SN_OTHER_DRONE"}}}).encode(),
        600,
    )
    await rec.on_mqtt_message(
        "thing/product/SN_OTHER_DRONE/osd",
        json.dumps({"mode_code": "manual_flight"}).encode(),
        700,
    )
    await rec.on_mqtt_message(
        "thing/product/SN_THIRD_DOCK/services",
        json.dumps({"method": "wayline_prepare", "data": {"flight_id": "OTHER_T"}}).encode(),
        800,
    )

    # 本机场 + 本飞行器的消息应保留
    await rec.on_mqtt_message(
        "thing/product/SN_OURS_DRONE/osd",
        json.dumps({"mode_code": "manual_flight"}).encode(),
        900,
    )

    flight_dir = await rec.finalize_and_close(finalize_reason="manual_stop")
    manifest = json.loads((flight_dir / "manifest.json").read_text())

    topics_in_manifest = {t["topic"] for t in manifest["topics"]}
    # 本机场和本飞行器的 topic 在
    assert "thing/product/SN_DOCK/osd" in topics_in_manifest
    assert "thing/product/SN_OURS_DRONE/osd" in topics_in_manifest
    # 其他机场/飞行器的 topic 不在
    assert "thing/product/SN_OTHER_DOCK/osd" not in topics_in_manifest
    assert "thing/product/SN_OTHER_DRONE/osd" not in topics_in_manifest
    assert "thing/product/SN_THIRD_DOCK/services" not in topics_in_manifest

    # 也确认磁盘上没有这些文件
    foreign_files = list((flight_dir / "topics").glob("thing__product__SN_OTHER*"))
    assert foreign_files == []
    foreign_files = list((flight_dir / "topics").glob("thing__product__SN_THIRD*"))
    assert foreign_files == []


@pytest.mark.asyncio
async def test_recorder_writes_pending_dir_before_task_id(minimal_config, tmp_path: Path):
    """task_id unknown → pending_<ts> dir; arrives → rename"""
    cfg = dict(minimal_config)
    cfg["flight_detection"]["rules"]["start"] = [
        {"source": "dock_osd", "field": "wayline_mission_state", "not_equals": "idle"}
    ]
    rec = Recorder(cfg, dock_sn="SN_DOCK", drone_sn=None)
    await rec.start_async_components()

    await rec.on_mqtt_message(
        "thing/product/SN_DOCK/osd",
        json.dumps({"wayline_mission_state": "executing"}).encode(),
        500,
    )
    assert rec.flight_dir is not None
    assert rec.flight_dir.name.startswith("pending_")

    await rec.on_mqtt_message(
        "thing/product/SN_DOCK/services",
        json.dumps({"method": "wayline_prepare", "data": {"flight_id": "T-RENAMED"}}).encode(),
        1000,
    )
    assert "T-RENAMED" in rec.flight_dir.name


@pytest.mark.asyncio
async def test_recorder_rename_preserves_pre_rename_records_in_manifest(
    minimal_config, tmp_path: Path,
):
    """Records written to pending dir before rename must appear in final manifest counts."""
    cfg = dict(minimal_config)
    cfg["flight_detection"]["rules"]["start"] = [
        {"source": "dock_osd", "field": "wayline_mission_state", "not_equals": "idle"}
    ]
    rec = Recorder(cfg, dock_sn="SN_DOCK", drone_sn=None)
    await rec.start_async_components()

    # 2 records to dock_osd pre-rename
    await rec.on_mqtt_message(
        "thing/product/SN_DOCK/osd",
        json.dumps({"wayline_mission_state": "executing", "n": 1}).encode(),
        500,
    )
    await rec.on_mqtt_message(
        "thing/product/SN_DOCK/osd",
        json.dumps({"wayline_mission_state": "executing", "n": 2}).encode(),
        600,
    )

    # rename trigger
    await rec.on_mqtt_message(
        "thing/product/SN_DOCK/services",
        json.dumps({"method": "wayline_prepare", "data": {"flight_id": "T-MERGE"}}).encode(),
        1000,
    )

    # 1 more record post-rename
    await rec.on_mqtt_message(
        "thing/product/SN_DOCK/osd",
        json.dumps({"wayline_mission_state": "executing", "n": 3}).encode(),
        1500,
    )

    flight_dir = await rec.finalize_and_close(finalize_reason="auto_idle")
    manifest = json.loads((flight_dir / "manifest.json").read_text())
    assert "T-MERGE" in flight_dir.name

    # dock_osd entry should reflect ALL 3 records (2 pre + 1 post)
    dock_osd = next(t for t in manifest["topics"] if t["topic"] == "thing/product/SN_DOCK/osd")
    assert dock_osd["count"] == 3
    assert dock_osd["first_recv_ts_ms"] == 500
    assert dock_osd["last_recv_ts_ms"] == 1500
