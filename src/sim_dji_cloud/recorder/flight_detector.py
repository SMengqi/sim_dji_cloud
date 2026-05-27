from enum import Enum
from typing import Any, Optional

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
    """按机场 flighttask_step_code 的 sticky 状态机驱动录制。

    flighttask_step_code 仅在状态变化时下发；本检测器保持"最后已知"值，
    OSD 不带该字段时不改变状态（绝不据此误停）。多任务循环：finalize 后
    调用 reset() 回到等待，下一段任务自动重新进入 RECORDING。
    """

    def __init__(self, record_steps: list[int], idle_debounce_seconds: int):
        self.record_steps = set(record_steps)
        self.idle_debounce_ms = int(idle_debounce_seconds * 1000)  # 支持小数秒
        self.state = FlightState.WAITING_TASK
        self._last_step: Optional[int] = None
        self._left_record_at_ms: Optional[int] = None
        self.task_id: Optional[str] = None
        self.task_started_ms: Optional[int] = None
        self.task_ended_ms: Optional[int] = None
        self.end_reason: Optional[str] = None

    def feed(self, source: Source, payload: dict[str, Any], now_ms: int) -> FlightState:
        if source == Source.DOCK_OSD:
            step = _get_field(payload, "data.flighttask_step_code")
            if step is not None:
                self._last_step = step
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
        self._left_record_at_ms = None
        self.task_id = None
        self.task_started_ms = None
        self.task_ended_ms = None
        self.end_reason = None

    def _advance(self, now_ms: int) -> FlightState:
        recording_now = self._last_step in self.record_steps
        if self.state == FlightState.WAITING_TASK:
            if recording_now:
                self.task_started_ms = now_ms
                self.task_id = None
                self.state = FlightState.RECORDING
        elif self.state == FlightState.RECORDING:
            if recording_now:
                self._left_record_at_ms = None
            else:
                if self._left_record_at_ms is None:
                    self._left_record_at_ms = now_ms
                if now_ms - self._left_record_at_ms >= self.idle_debounce_ms:
                    self.task_ended_ms = now_ms
                    self.end_reason = "task_idle"
                    self.state = FlightState.FINALIZING
        return self.state
