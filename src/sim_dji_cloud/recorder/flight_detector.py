import time
from enum import Enum
from typing import Any, Callable, Optional

from sim_dji_cloud.recorder.topic_router import Source


class FlightState(str, Enum):
    WAITING_TASK = "waiting_task"
    RECORDING = "recording"
    FINALIZING = "finalizing"


def _get_field(obj: Any, path: str) -> Any:
    """点路径取值；任一层非 dict 返回 None。"""
    if "." not in path:
        return obj.get(path) if isinstance(obj, dict) else None
    cur: Any = obj
    for seg in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(seg)
    return cur


def _extract_task_id(payload: dict[str, Any]) -> Optional[str]:
    data = payload.get("data")
    if not isinstance(data, dict):
        data = {}
    return data.get("flight_id") or data.get("task_id") or payload.get("flight_id")


class FlightDetector:
    """按机场 flighttask_step_code / drone_in_dock 的 sticky 状态机驱动录制。

    录制条件 = step ∈ record_steps OR（启用时）drone_in_dock == 0。后者覆盖
    无航线任务的手动飞行（一键起飞 + DRC），其 step 不进 record set。
    两字段都可能仅在变化时下发；本检测器保持"最后已知"值，OSD 不带字段时
    不改变状态（绝不据此误停）。多任务循环：finalize 后调用 reset() 回到
    等待，下一段任务自动重新进入 RECORDING。
    """

    def __init__(
        self,
        record_steps: list[int],
        idle_debounce_seconds: int,
        *,
        record_when_drone_out_of_dock: bool = True,
        mono_clock: Optional[Callable[[], int]] = None,
    ):
        """Args
        ----
        record_when_drone_out_of_dock:
            机场 OSD ``data.drone_in_dock == 0``（飞行器在舱外）时也录制。
        mono_clock:
            返回单调递增纳秒数（如 ``time.monotonic_ns``）。debounce 计时只用
            这条时钟，**不**用传入 feed/tick 的 wall ms —— 后者会被 NTP 跳变
            污染，导致 idle 检测漏触（wall 跳回去）或误触（wall 跳前进）。
            ``task_started_ms`` / ``task_ended_ms`` / manifest 输出仍用 wall
            ms（来自 feed/tick 入参），与外部时间轴对齐。测试可注入 fake
            clock 完全控制 debounce 推进。默认 ``time.monotonic_ns``。
        """
        self.record_steps = set(record_steps)
        self.idle_debounce_ms = int(idle_debounce_seconds * 1000)  # 支持小数秒
        self._mono_clock: Callable[[], int] = mono_clock or time.monotonic_ns
        self.state = FlightState.WAITING_TASK
        self._last_step: Optional[int] = None
        self.record_when_drone_out_of_dock = record_when_drone_out_of_dock
        self._last_in_dock: Optional[int] = None
        # 离开 record set 的 monotonic_ns 时刻；None 表示当前仍在 record set
        # （或还没进入 RECORDING）。debounce 用 self._mono_clock() - this 计差。
        self._left_record_at_mono_ns: Optional[int] = None
        self.task_id: Optional[str] = None
        self.task_started_ms: Optional[int] = None
        self.task_ended_ms: Optional[int] = None
        self.end_reason: Optional[str] = None

    def feed(self, source: Source, payload: dict[str, Any], now_ms: int) -> FlightState:
        if source == Source.DOCK_OSD:
            step = _get_field(payload, "data.flighttask_step_code")
            if step is not None:
                self._last_step = step
            in_dock = _get_field(payload, "data.drone_in_dock")
            if in_dock is not None:
                self._last_in_dock = in_dock
        result = self._advance(now_ms)
        # 在 _advance 之后补 task_id：使起录那条消息（若同时带 flight_id）也能被捕获
        if self.state == FlightState.RECORDING and not self.task_id:
            tid = _extract_task_id(payload)
            if tid:
                self.task_id = tid
        return result

    def tick(self, now_ms: int) -> FlightState:
        return self._advance(now_ms)

    def reset(self) -> None:
        self.state = FlightState.WAITING_TASK
        self._left_record_at_mono_ns = None
        self.task_id = None
        self.task_started_ms = None
        self.task_ended_ms = None
        self.end_reason = None

    def _advance(self, now_ms: int) -> FlightState:
        recording_now = self._last_step in self.record_steps or (
            self.record_when_drone_out_of_dock and self._last_in_dock == 0
        )
        if self.state == FlightState.WAITING_TASK:
            if recording_now:
                self.task_started_ms = now_ms
                self.task_id = None
                self.state = FlightState.RECORDING
        elif self.state == FlightState.RECORDING:
            if recording_now:
                self._left_record_at_mono_ns = None
            else:
                mono_now_ns = self._mono_clock()
                if self._left_record_at_mono_ns is None:
                    self._left_record_at_mono_ns = mono_now_ns
                elapsed_ms = (mono_now_ns - self._left_record_at_mono_ns) // 1_000_000
                if elapsed_ms >= self.idle_debounce_ms:
                    # task_ended_ms 仍用 wall ms（manifest 写出与 dji_ts/recv_ts
                    # 时间轴对齐），debounce 判定用 monotonic 不受 NTP 跳影响。
                    self.task_ended_ms = now_ms
                    self.end_reason = "task_idle"
                    self.state = FlightState.FINALIZING
        return self.state
