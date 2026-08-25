from sim_dji_cloud.recorder.pilot_flight_detector import (
    PilotFlightDetector, PilotFlightState,
)
from sim_dji_cloud.recorder.pilot_topic_router import Source


class FakeMonoClock:
    """注入到 PilotFlightDetector 的可控单调时钟，跟 dock 版 FlightDetector 测试同款。"""

    def __init__(self):
        self.ns = 0

    def __call__(self) -> int:
        return self.ns

    def advance_s(self, seconds: float) -> None:
        self.ns += int(seconds * 1_000_000_000)


def _topo(sns):
    return {"data": {"sub_devices": [{"sn": s} for s in sns]}}


def _osd(track_id=None):
    data = {}
    if track_id is not None:
        data["track_id"] = track_id
    return {"data": data}


def test_starts_recording_when_aircraft_appears_in_topology():
    d = PilotFlightDetector(idle_debounce_seconds=5)
    assert d.feed(Source.RC_STATUS, _topo(["SN_AIRCRAFT"]), 1000) == PilotFlightState.RECORDING
    assert d.aircraft_sn == "SN_AIRCRAFT"
    assert d.task_started_ms == 1000


def test_stays_waiting_when_topology_empty():
    d = PilotFlightDetector(idle_debounce_seconds=5)
    assert d.feed(Source.RC_STATUS, _topo([]), 1000) == PilotFlightState.WAITING_AIRCRAFT
    assert d.aircraft_sn is None


def test_takes_first_sub_device_when_multiple():
    d = PilotFlightDetector(idle_debounce_seconds=5)
    d.feed(Source.RC_STATUS, _topo(["SN_A", "SN_B"]), 1000)
    assert d.aircraft_sn == "SN_A"


def test_presence_sticky_across_non_topology_messages():
    d = PilotFlightDetector(idle_debounce_seconds=5)
    d.feed(Source.RC_STATUS, _topo(["SN_AIRCRAFT"]), 1000)
    for t in range(2000, 20000, 2000):
        assert d.feed(Source.AIRCRAFT_OSD, _osd(), t) == PilotFlightState.RECORDING


def test_idle_debounce_finalizes_after_aircraft_leaves():
    clock = FakeMonoClock()
    d = PilotFlightDetector(idle_debounce_seconds=5, mono_clock=clock)
    d.feed(Source.RC_STATUS, _topo(["SN_AIRCRAFT"]), 1000)
    clock.advance_s(9)
    d.feed(Source.RC_STATUS, _topo([]), 10_000)   # 飞行器从拓扑消失
    assert d.state == PilotFlightState.RECORDING
    clock.advance_s(3)
    assert d.tick(13_000) == PilotFlightState.RECORDING   # 3s 不够 5s debounce
    clock.advance_s(2)                                     # 累计 5s
    assert d.tick(15_000) == PilotFlightState.FINALIZING
    assert d.end_reason == "aircraft_offline"
    assert d.task_ended_ms == 15_000


def test_wall_clock_jump_does_not_trigger_premature_finalize():
    clock = FakeMonoClock()
    d = PilotFlightDetector(idle_debounce_seconds=5, mono_clock=clock)
    d.feed(Source.RC_STATUS, _topo(["SN_AIRCRAFT"]), 1000)
    d.feed(Source.RC_STATUS, _topo([]), 10_000)
    clock.advance_s(1)
    assert d.tick(40_000) == PilotFlightState.RECORDING, (
        "wall 跳 30s 而 monotonic 只 1s → debounce 5s 不应触发"
    )
    clock.advance_s(5)
    assert d.tick(40_500) == PilotFlightState.FINALIZING


def test_idle_debounce_cancelled_if_aircraft_returns():
    d = PilotFlightDetector(idle_debounce_seconds=5)
    d.feed(Source.RC_STATUS, _topo(["SN_AIRCRAFT"]), 1000)
    d.feed(Source.RC_STATUS, _topo([]), 10_000)
    d.tick(12_000)
    d.feed(Source.RC_STATUS, _topo(["SN_AIRCRAFT"]), 13_000)
    assert d.tick(20_000) == PilotFlightState.RECORDING


def test_reset_keeps_aircraft_sn_but_clears_task_state():
    d = PilotFlightDetector(idle_debounce_seconds=0)
    d.feed(Source.RC_STATUS, _topo(["SN_AIRCRAFT"]), 1000)
    d.feed(Source.RC_STATUS, _topo([]), 2000)
    assert d.state == PilotFlightState.FINALIZING
    d.reset()
    assert d.state == PilotFlightState.WAITING_AIRCRAFT
    assert d.task_id is None and d.task_started_ms is None
    assert d.aircraft_sn == "SN_AIRCRAFT"   # 学到的 SN 跨段保留（sticky，同 dock 版设计）
    # 同一飞行器再次上线，直接进入 RECORDING
    assert d.feed(Source.RC_STATUS, _topo(["SN_AIRCRAFT"]), 4000) == PilotFlightState.RECORDING


def test_track_id_backfilled_from_aircraft_osd_while_recording():
    d = PilotFlightDetector(idle_debounce_seconds=5)
    d.feed(Source.RC_STATUS, _topo(["SN_AIRCRAFT"]), 1000)
    assert d.task_id is None
    d.feed(Source.AIRCRAFT_OSD, _osd(track_id="TRK-9"), 1500)
    assert d.task_id == "TRK-9"


def test_rc_osd_does_not_change_presence():
    d = PilotFlightDetector(idle_debounce_seconds=5)
    assert d.feed(Source.RC_OSD, {"data": {}}, 1000) == PilotFlightState.WAITING_AIRCRAFT
