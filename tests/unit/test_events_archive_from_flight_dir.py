import json
import pytest
from pathlib import Path

from sim_dji_cloud.dashboard.events_archive import read_archive_from_flight_dir


_DOCK = "SN_DOCK_FIXTURE"


def _build_minimal_flight(tmp_path, *, with_events=True, with_drc=True,
                          with_services=True, manifest_extra=None):
    """构造一个最小 flight_dir。"""
    flight = tmp_path / "T-T__SN_DOCK_FIXTURE__20260601-000000"
    (flight / "topics").mkdir(parents=True)
    topics_entries = [
        {"topic": f"thing/product/{_DOCK}/osd",
         "device_sn": _DOCK, "direction": "up",
         "files": [{"name": "topics/dock_osd.jsonl",
                    "count": 1, "first_ms": 1, "last_ms": 1}]},
    ]
    (flight / "topics" / "dock_osd.jsonl").write_text(
        json.dumps({"recv_ts_ms": 1, "dji_ts_ms": 1, "direction": "up",
                    "topic": f"thing/product/{_DOCK}/osd",
                    "payload": {"data": {}}}) + "\n"
    )
    if with_events:
        topics_entries.append({
            "topic": f"thing/product/{_DOCK}/events",
            "device_sn": _DOCK, "direction": "up",
            "files": [{"name": "topics/events.jsonl",
                       "count": 2, "first_ms": 100, "last_ms": 200}],
        })
        (flight / "topics" / "events.jsonl").write_text(
            json.dumps({"recv_ts_ms": 100, "dji_ts_ms": 100, "direction": "up",
                        "topic": f"thing/product/{_DOCK}/events",
                        "payload": {"method": "alarm_1"}}) + "\n" +
            json.dumps({"recv_ts_ms": 200, "dji_ts_ms": 200, "direction": "up",
                        "topic": f"thing/product/{_DOCK}/events",
                        "payload": {"method": "alarm_2"}}) + "\n"
        )
    if with_drc:
        topics_entries.append({
            "topic": f"thing/product/{_DOCK}/drc/down",
            "device_sn": _DOCK, "direction": "down",
            "files": [{"name": "topics/drc_down.jsonl",
                       "count": 1, "first_ms": 150, "last_ms": 150}],
        })
        (flight / "topics" / "drc_down.jsonl").write_text(
            json.dumps({"recv_ts_ms": 150, "dji_ts_ms": 150, "direction": "down",
                        "topic": f"thing/product/{_DOCK}/drc/down",
                        "payload": {"method": "stick_control"}}) + "\n"
        )
    if with_services:
        topics_entries.append({
            "topic": f"thing/product/{_DOCK}/services",
            "device_sn": _DOCK, "direction": "up",
            "files": [{"name": "topics/services.jsonl",
                       "count": 1, "first_ms": 250, "last_ms": 250}],
        })
        (flight / "topics" / "services.jsonl").write_text(
            json.dumps({"recv_ts_ms": 250, "dji_ts_ms": 250, "direction": "up",
                        "topic": f"thing/product/{_DOCK}/services",
                        "payload": {"method": "wayline_create"}}) + "\n"
        )

    manifest = {
        "schema_version": 1, "status": "ok", "finalize_reason": "auto_idle",
        "task_id": "T-T", "dock_sn": _DOCK, "drone_sn": "SN_DRONE",
        "started_at_recv_ms": 50, "ended_at_recv_ms": 300,
        "gaps": [], "video": None,
        "topics": topics_entries,
    }
    if manifest_extra:
        manifest.update(manifest_extra)
    (flight / "manifest.json").write_text(json.dumps(manifest))
    return flight


def test_reads_events_and_controls_jsonl_into_archive(tmp_path):
    flight = _build_minimal_flight(tmp_path)
    archive, session = read_archive_from_flight_dir(flight)
    entries, _ = archive.query()
    methods = [e["method"] for e in entries]
    # 升序按 recv_ts_ms 排：100 events, 150 drc, 200 events, 250 services
    assert methods == ["alarm_1", "stick_control", "alarm_2", "wayline_create"]


def test_session_started_at_ms_uses_manifest_field(tmp_path):
    """offline session 起点是 manifest.started_at_recv_ms，不是第一条 event。"""
    flight = _build_minimal_flight(tmp_path)
    _, session = read_archive_from_flight_dir(flight)
    assert session == 50  # 不是 100


def test_skips_osd_topics_from_jsonl(tmp_path):
    flight = _build_minimal_flight(tmp_path, with_drc=False, with_services=False)
    archive, _ = read_archive_from_flight_dir(flight)
    entries, _ = archive.query()
    topics = {e["topic"] for e in entries}
    assert f"thing/product/{_DOCK}/osd" not in topics


def test_skips_missing_jsonl_files_listed_in_manifest(tmp_path):
    flight = _build_minimal_flight(tmp_path)
    (flight / "topics" / "drc_down.jsonl").unlink()
    archive, _ = read_archive_from_flight_dir(flight)
    entries, _ = archive.query()
    methods = [e["method"] for e in entries]
    assert methods == ["alarm_1", "alarm_2", "wayline_create"]


def test_raises_filenotfound_on_missing_manifest(tmp_path):
    flight = tmp_path / "bogus"
    flight.mkdir()
    with pytest.raises(FileNotFoundError):
        read_archive_from_flight_dir(flight)


def test_raises_valueerror_on_corrupt_manifest(tmp_path):
    flight = tmp_path / "T-T__corrupt__20260601-000000"
    (flight / "topics").mkdir(parents=True)
    (flight / "manifest.json").write_text("{not json")
    with pytest.raises(ValueError):
        read_archive_from_flight_dir(flight)


def test_rejects_path_traversal_in_manifest(tmp_path):
    """manifest 里 file['name'] 含 ../ → 跳过 + warning，不读到 flight_dir 外的文件。"""
    flight = tmp_path / "T-T__attack__20260601-000000"
    (flight / "topics").mkdir(parents=True)
    # 构造一个无害但越界的"目标文件"
    decoy = tmp_path / "decoy.jsonl"
    decoy.write_text(
        json.dumps({"recv_ts_ms": 1, "topic": "thing/product/X/events",
                    "payload": {"method": "should_NOT_be_read"}}) + "\n"
    )
    manifest = {
        "schema_version": 1, "status": "ok", "task_id": "T-T",
        "dock_sn": "SN", "drone_sn": "D",
        "started_at_recv_ms": 0, "ended_at_recv_ms": 100,
        "gaps": [], "video": None,
        "topics": [
            {"topic": "thing/product/SN/events", "device_sn": "SN",
             "direction": "up",
             "files": [{"name": "../decoy.jsonl",
                        "count": 1, "first_ms": 1, "last_ms": 1}]},
        ],
    }
    (flight / "manifest.json").write_text(json.dumps(manifest))
    archive, _ = read_archive_from_flight_dir(flight)
    entries, _ = archive.query()
    # 没读到外部 decoy，所以 archive 空
    assert entries == []
