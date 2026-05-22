import json
import tempfile
import pytest
from pathlib import Path
from sim_dji_cloud.selfcheck import SelfCheck


def _make_fixture(tmp_path: Path) -> Path:
    flight = tmp_path / "T-T__SN_DOCK_FIXTURE__20260521-000000"
    (flight / "topics").mkdir(parents=True)
    (flight / "topics" / "thing__product__SN_DOCK_FIXTURE__osd.0001.jsonl").write_text(
        '{"recv_ts_ms":0,"dji_ts_ms":0,"direction":"up","topic":"thing/product/SN_DOCK_FIXTURE/osd","payload":{"a":1}}\n'
        '{"recv_ts_ms":1000,"dji_ts_ms":1000,"direction":"up","topic":"thing/product/SN_DOCK_FIXTURE/osd","payload":{"a":2}}\n'
    )
    manifest = {
        "schema_version": 1, "status": "ok", "finalize_reason": "auto_idle",
        "task_id": "T-T", "dock_sn": "SN_DOCK_FIXTURE", "drone_sn": "SN_DRONE_FIXTURE",
        "started_at_recv_ms": 0, "ended_at_recv_ms": 1000,
        "gaps": [], "video": None,
        "topics": [{
            "topic": "thing/product/SN_DOCK_FIXTURE/osd",
            "device_sn": "SN_DOCK_FIXTURE", "direction": "up",
            "count": 2, "first_recv_ts_ms": 0, "last_recv_ts_ms": 1000,
            "files": [{"name": "topics/thing__product__SN_DOCK_FIXTURE__osd.0001.jsonl",
                       "count": 2, "first_ms": 0, "last_ms": 1000}],
        }],
    }
    (flight / "manifest.json").write_text(json.dumps(manifest))
    return flight


@pytest.mark.asyncio
async def test_selfcheck_loopback_passes_against_self(tmp_path: Path):
    flight = _make_fixture(tmp_path)
    sc = SelfCheck(
        flight_dir=flight,
        report_dir=tmp_path / "report",
        tolerance_ms=50,
    )
    result = await sc.run_with_inprocess_loopback()

    assert result.overall == "PASS"
    summary = json.loads((tmp_path / "report" / "summary.json").read_text())
    assert summary["overall"] == "PASS"
    assert summary["tolerance_ms"] == 50
    assert len(summary["topics"]) == 1
    assert summary["topics"][0]["orig_count"] == 2
    assert summary["topics"][0]["replayed_count"] == 2


import socket


def _is_port_open(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.3):
            return True
    except OSError:
        return False


def test_pick_free_broker_port_in_range():
    from sim_dji_cloud.selfcheck import pick_free_port_in_range
    p = pick_free_port_in_range(20883, 20893)
    assert 20883 <= p <= 20893
    assert not _is_port_open(p)


@pytest.mark.asyncio
async def test_selfcheck_on_real_flight_fixture():
    """对真机脱敏 fixture 跑 SelfCheck，应得 PASS。"""
    fixture_path = Path(__file__).parent.parent / "fixtures" / "real_flight_basic"
    if not (fixture_path / "manifest.json").exists():
        pytest.skip("fixture not yet generated; run scripts/anonymize_flight.py")

    with tempfile.TemporaryDirectory() as tmp:
        sc = SelfCheck(
            flight_dir=fixture_path,
            report_dir=Path(tmp) / "report",
            tolerance_ms=200,
        )
        result = await sc.run_with_inprocess_loopback()
        assert result.overall == "PASS", (
            f"SelfCheck FAILED on real fixture, see {result.report_dir}"
        )
        drone_osd = next(
            (r for r in result.topic_results if "SN_DRONE_FIXTURE/osd" in r.topic),
            None,
        )
        assert drone_osd is not None
        assert drone_osd.status == "PASS"
        assert drone_osd.orig_count >= 100
