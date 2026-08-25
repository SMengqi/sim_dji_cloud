import time
from enum import Enum
from typing import Any, Callable, Optional

from loguru import logger

from sim_dji_cloud.recorder.pilot_topic_router import Source


class PilotFlightState(str, Enum):
    WAITING_AIRCRAFT = "waiting_aircraft"
    RECORDING = "recording"
    FINALIZING = "finalizing"


def _extract_aircraft_sn(payload: dict[str, Any]) -> Optional[str]:
    data = payload.get("data")
    if not isinstance(data, dict):
        return None
    sub_devices = data.get("sub_devices")
    if not isinstance(sub_devices, list) or not sub_devices:
        return None
    if len(sub_devices) > 1:
        logger.warning(
            "update_topo sub_devices 有 {} 个元素，只取第一个（RC Plus 2 单机配对场景）",
            len(sub_devices),
        )
    first = sub_devices[0]
    if not isinstance(first, dict):
        return None
    sn = first.get("sn")
    return sn if isinstance(sn, str) and sn else None


def _extract_track_id(payload: dict[str, Any]) -> Optional[str]:
    data = payload.get("data")
    if not isinstance(data, dict):
        return None
    tid = data.get("track_id")
    return tid if isinstance(tid, str) and tid else None


class PilotFlightDetector:
    """按遥控器拓扑（sys/product/{rc_sn}/status 的 update_topo）驱动录制窗口。

    飞行器出现在 sub_devices[] → 学到 aircraft_sn + 开始录制；
    从 sub_devices[] 消失，持续 idle_debounce_seconds → 结束当前段。
    debounce 计时用单调时钟，不受 wall clock/NTP 跳变影响（同 dock 版 FlightDetector）。
    """

    def __init__(
        self,
        idle_debounce_seconds: int,
        *,
        mono_clock: Optional[Callable[[], int]] = None,
    ):
        self.idle_debounce_ms = int(idle_debounce_seconds * 1000)
        self._mono_clock: Callable[[], int] = mono_clock or time.monotonic_ns
        self.state = PilotFlightState.WAITING_AIRCRAFT
        self.aircraft_sn: Optional[str] = None
        self._aircraft_present: bool = False
        self._left_record_at_mono_ns: Optional[int] = None
        self.task_id: Optional[str] = None
        self.task_started_ms: Optional[int] = None
        self.task_ended_ms: Optional[int] = None
        self.end_reason: Optional[str] = None

    def feed(self, source: Source, payload: dict[str, Any], now_ms: int) -> PilotFlightState:
        if source == Source.RC_STATUS:
            sn = _extract_aircraft_sn(payload)
            if sn is not None:
                if self.aircraft_sn is None:
                    self.aircraft_sn = sn
                self._aircraft_present = (sn == self.aircraft_sn)
            else:
                self._aircraft_present = False
        result = self._advance(now_ms)
        if (
            source == Source.AIRCRAFT_OSD
            and self.state == PilotFlightState.RECORDING
            and not self.task_id
        ):
            tid = _extract_track_id(payload)
            if tid:
                self.task_id = tid
        return result

    def tick(self, now_ms: int) -> PilotFlightState:
        return self._advance(now_ms)

    def reset(self) -> None:
        self.state = PilotFlightState.WAITING_AIRCRAFT
        self._left_record_at_mono_ns = None
        self.task_id = None
        self.task_started_ms = None
        self.task_ended_ms = None
        self.end_reason = None

    def _advance(self, now_ms: int) -> PilotFlightState:
        if self.state == PilotFlightState.WAITING_AIRCRAFT:
            if self._aircraft_present:
                self.task_started_ms = now_ms
                self.task_id = None
                self.state = PilotFlightState.RECORDING
        elif self.state == PilotFlightState.RECORDING:
            if self._aircraft_present:
                self._left_record_at_mono_ns = None
            else:
                mono_now_ns = self._mono_clock()
                if self._left_record_at_mono_ns is None:
                    self._left_record_at_mono_ns = mono_now_ns
                elapsed_ms = (mono_now_ns - self._left_record_at_mono_ns) // 1_000_000
                if elapsed_ms >= self.idle_debounce_ms:
                    self.task_ended_ms = now_ms
                    self.end_reason = "aircraft_offline"
                    self.state = PilotFlightState.FINALIZING
        return self.state
