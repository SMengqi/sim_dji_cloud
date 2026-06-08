from sim_dji_cloud.recorder.flight_detector import (
    FlightDetector, FlightState, _extract_task_id,
)
from sim_dji_cloud.recorder.topic_router import Source


class FakeMonoClock:
    """注入到 FlightDetector 的可控单调时钟。测试推进 advance_s(n) 模拟真实时间流逝。"""

    def __init__(self):
        self.ns = 0

    def __call__(self) -> int:
        return self.ns

    def advance_s(self, seconds: float) -> None:
        self.ns += int(seconds * 1_000_000_000)


def test_extract_task_id_handles_non_dict_data():
    # data 可能是 list（部分 events payload）或缺失 → 不崩，回退顶层 flight_id
    assert _extract_task_id({"data": [{"x": 1}]}) is None
    assert _extract_task_id({"data": None}) is None
    assert _extract_task_id({}) is None
    assert _extract_task_id({"flight_id": "T-top"}) == "T-top"
    assert _extract_task_id({"data": {"flight_id": "T-d"}}) == "T-d"


def _osd(step=None, flight_id=None):
    data = {}
    if step is not None:
        data["flighttask_step_code"] = step
    if flight_id is not None:
        data["flight_id"] = flight_id
    return {"data": data}


def test_starts_recording_on_step_in_record_set():
    d = FlightDetector(record_steps=[0, 1, 2], idle_debounce_seconds=5)
    assert d.feed(Source.DOCK_OSD, _osd(step=1), 1000) == FlightState.RECORDING
    assert d.task_started_ms == 1000


def test_does_not_start_on_idle_or_other_steps():
    for step in (3, 4, 5, 255, 256):
        d = FlightDetector(record_steps=[0, 1, 2], idle_debounce_seconds=5)
        assert d.feed(Source.DOCK_OSD, _osd(step=step), 1000) == FlightState.WAITING_TASK


def test_sticky_field_absent_keeps_recording():
    d = FlightDetector(record_steps=[0, 1, 2], idle_debounce_seconds=5)
    d.feed(Source.DOCK_OSD, _osd(step=1), 1000)
    for t in range(2000, 60000, 2000):
        assert d.feed(Source.DOCK_OSD, _osd(), t) == FlightState.RECORDING


def test_idle_debounce_finalizes_after_sustain():
    """debounce 计时改 monotonic 后，必须由 mono clock 推进驱动 finalize。

    wall ms 仍然作为 task_started_ms / task_ended_ms 写出（manifest 用）。
    """
    clock = FakeMonoClock()
    d = FlightDetector(record_steps=[0, 1, 2], idle_debounce_seconds=5,
                       mono_clock=clock)
    d.feed(Source.DOCK_OSD, _osd(step=1), 1000)
    clock.advance_s(9)
    d.feed(Source.DOCK_OSD, _osd(step=5), 10_000)   # 离开 record set
    assert d.state == FlightState.RECORDING
    clock.advance_s(3)
    assert d.tick(13_000) == FlightState.RECORDING   # 3s 不够 5s debounce
    clock.advance_s(2)                               # 累计 5s
    assert d.tick(15_000) == FlightState.FINALIZING
    assert d.end_reason == "task_idle"
    assert d.task_ended_ms == 15_000                 # wall ms 输出保留


def test_wall_clock_jump_does_not_trigger_premature_finalize():
    """NTP 跳前进 30s 但 monotonic 只走了 1s → debounce 仍不触发。

    Regression: review MAJOR — 旧 wall-clock debounce 在 NTP 跳变时会瞬时触发
    finalize，把正在进行的飞行错误地终止。
    """
    clock = FakeMonoClock()
    d = FlightDetector(record_steps=[0, 1, 2], idle_debounce_seconds=5,
                       mono_clock=clock)
    d.feed(Source.DOCK_OSD, _osd(step=1), 1000)
    d.feed(Source.DOCK_OSD, _osd(step=5), 10_000)
    # NTP 跳前进 30s（wall 从 10_000 跳到 40_000），但 monotonic 只推进 1s
    clock.advance_s(1)
    assert d.tick(40_000) == FlightState.RECORDING, (
        "wall 跳 30s 而 monotonic 只 1s → debounce 5s 不应触发"
    )
    # 真实再过 5s monotonic
    clock.advance_s(5)
    assert d.tick(40_500) == FlightState.FINALIZING
    assert d.task_ended_ms == 40_500     # wall ms 输出按入参


def test_idle_debounce_cancelled_if_returns():
    d = FlightDetector(record_steps=[0, 1, 2], idle_debounce_seconds=5)
    d.feed(Source.DOCK_OSD, _osd(step=1), 1000)
    d.feed(Source.DOCK_OSD, _osd(step=5), 10_000)
    d.tick(12_000)
    d.feed(Source.DOCK_OSD, _osd(step=1), 13_000)
    assert d.tick(20_000) == FlightState.RECORDING


def test_reset_allows_next_task_cycle():
    d = FlightDetector(record_steps=[0, 1, 2], idle_debounce_seconds=0)
    d.feed(Source.DOCK_OSD, _osd(step=1), 1000)
    d.feed(Source.DOCK_OSD, _osd(step=5), 2000)
    assert d.state == FlightState.FINALIZING
    d.reset()
    assert d.state == FlightState.WAITING_TASK
    assert d.task_id is None and d.task_started_ms is None
    assert d.tick(3000) == FlightState.WAITING_TASK
    assert d.feed(Source.DOCK_OSD, _osd(step=1), 4000) == FlightState.RECORDING


def test_task_id_backfilled_while_recording():
    d = FlightDetector(record_steps=[0, 1, 2], idle_debounce_seconds=5)
    d.feed(Source.DOCK_OSD, _osd(step=1), 1000)
    assert d.task_id is None
    d.feed(Source.DOCK_EVENTS, {"data": {"flight_id": "T-9"}}, 1500)
    assert d.task_id == "T-9"


def test_non_dock_osd_does_not_change_step():
    d = FlightDetector(record_steps=[0, 1, 2], idle_debounce_seconds=5)
    assert d.feed(Source.DRONE_OSD, {"data": {"flighttask_step_code": 1}}, 1000) \
        == FlightState.WAITING_TASK
