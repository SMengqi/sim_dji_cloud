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
    return {"method": "update_topo", "data": {"sub_devices": [{"sn": s} for s in sns]}}


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


def test_rc_status_without_update_topo_method_does_not_change_presence():
    """method != "update_topo"（或缺失）不应更新 presence，即使 sub_devices
    看起来是合法拓扑——DJI Pilot-to-Cloud 只在 update_topo 上发拓扑，但代码
    不能假设这一点没有被别的 method 复用同一 topic。"""
    d = PilotFlightDetector(idle_debounce_seconds=5)
    payload_wrong_method = {
        "method": "something_else",
        "data": {"sub_devices": [{"sn": "SN_AIRCRAFT"}]},
    }
    assert d.feed(Source.RC_STATUS, payload_wrong_method, 1000) == PilotFlightState.WAITING_AIRCRAFT
    assert d.aircraft_sn is None

    payload_no_method = {"data": {"sub_devices": [{"sn": "SN_AIRCRAFT"}]}}
    assert d.feed(Source.RC_STATUS, payload_no_method, 2000) == PilotFlightState.WAITING_AIRCRAFT
    assert d.aircraft_sn is None


def test_missing_sub_devices_key_leaves_presence_unchanged_while_waiting():
    """data 存在但完全没有 sub_devices key（不是空列表）——这条消息不携带
    拓扑信息，presence 应保持不变（sticky-by-default，同 dock 版哲学）。"""
    d = PilotFlightDetector(idle_debounce_seconds=5)
    payload = {"method": "update_topo", "data": {"some_other_field": 1}}
    assert d.feed(Source.RC_STATUS, payload, 1000) == PilotFlightState.WAITING_AIRCRAFT
    assert d.aircraft_sn is None


def test_missing_sub_devices_key_leaves_presence_unchanged_while_recording():
    """同上，但飞机已经在录制中：缺 sub_devices key 的拓扑消息不应触发
    offline debounce（不能等价于 sub_devices: []）。"""
    clock = FakeMonoClock()
    d = PilotFlightDetector(idle_debounce_seconds=5, mono_clock=clock)
    d.feed(Source.RC_STATUS, _topo(["SN_AIRCRAFT"]), 1000)
    assert d.state == PilotFlightState.RECORDING

    payload = {"method": "update_topo", "data": {"some_other_field": 1}}
    clock.advance_s(9)
    assert d.feed(Source.RC_STATUS, payload, 10_000) == PilotFlightState.RECORDING
    # debounce 不应被启动——再 tick 到远超 5s debounce 之后仍是 RECORDING
    clock.advance_s(10)
    assert d.tick(20_000) == PilotFlightState.RECORDING


def test_explicit_empty_sub_devices_still_starts_offline_debounce():
    """sub_devices: []（显式空列表，带 method: update_topo）代表子设备显式
    下线，应该跟之前行为一样启动/持续 offline debounce（对比上面两个
    「key 缺失」的用例，这个是「key 存在但为空」）。"""
    clock = FakeMonoClock()
    d = PilotFlightDetector(idle_debounce_seconds=5, mono_clock=clock)
    d.feed(Source.RC_STATUS, _topo(["SN_AIRCRAFT"]), 1000)
    assert d.state == PilotFlightState.RECORDING

    clock.advance_s(1)
    d.feed(Source.RC_STATUS, _topo([]), 2000)
    assert d.state == PilotFlightState.RECORDING  # debounce 刚启动，还没到 5s
    clock.advance_s(5)
    assert d.tick(7000) == PilotFlightState.FINALIZING


def test_presence_scans_whole_sub_devices_list_once_sn_known():
    """aircraft_sn 学到之后，拓扑里该 SN 排在非 0 位置（比如其它子设备排前面）
    也应该被识别为在线——不能只看 index 0。"""
    d = PilotFlightDetector(idle_debounce_seconds=5)
    d.feed(Source.RC_STATUS, _topo(["SN_AIRCRAFT"]), 1000)
    assert d.aircraft_sn == "SN_AIRCRAFT"
    d.feed(Source.RC_STATUS, _topo([]), 2000)  # 离线，进入 debounce（不会推进到 FINALIZING，因为下面立刻用 tick 验证 RECORDING）
    payload = {
        "method": "update_topo",
        "data": {"sub_devices": [{"sn": "SN_OTHER"}, {"sn": "SN_AIRCRAFT"}]},
    }
    assert d.feed(Source.RC_STATUS, payload, 3000) == PilotFlightState.RECORDING
