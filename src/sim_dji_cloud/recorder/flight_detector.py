from enum import Enum
from typing import Any, Optional

from sim_dji_cloud.recorder.topic_router import Source


class FlightState(str, Enum):
    IDLE = "idle"
    WAITING_TASK = "waiting_task"
    RECORDING = "recording"
    FINALIZING = "finalizing"


class RuleEvaluator:
    """评估一组规则，维护 sustain_seconds 类规则的内部计时状态。"""

    def __init__(self, rules: list[dict[str, Any]]):
        self.rules = rules
        self._sustain_start: dict[int, int] = {}  # rule_idx → first_match_ms

    def evaluate(
        self, source: Source, payload: dict[str, Any], now_ms: int,
    ) -> tuple[bool, Optional[dict[str, Any]]]:
        for idx, rule in enumerate(self.rules):
            if rule.get("source") != source.value:
                continue

            ok = True
            if "payload_match" in rule:
                for k, v in rule["payload_match"].items():
                    if payload.get(k) != v:
                        ok = False
                        break
            if ok and "field" in rule:
                actual = payload.get(rule["field"])
                if "equals" in rule and actual != rule["equals"]:
                    ok = False
                if "not_equals" in rule and actual == rule["not_equals"]:
                    ok = False
                if "in" in rule and actual not in rule["in"]:
                    ok = False

            if not ok:
                self._sustain_start.pop(idx, None)
                continue

            sustain = rule.get("sustain_seconds")
            if sustain is None:
                return True, rule

            first = self._sustain_start.setdefault(idx, now_ms)
            if (now_ms - first) >= sustain * 1000:
                return True, rule

        return False, None


class FlightDetector:
    def __init__(
        self,
        start_rules: list[dict[str, Any]],
        end_rules: list[dict[str, Any]],
    ):
        self._start = RuleEvaluator(start_rules)
        self._end = RuleEvaluator(end_rules)
        self.state = FlightState.IDLE
        self.task_id: Optional[str] = None
        self.task_started_ms: Optional[int] = None
        self.task_ended_ms: Optional[int] = None
        self.end_reason: Optional[str] = None

    def feed(self, source: Source, payload: dict[str, Any], now_ms: int) -> FlightState:
        if self.state in (FlightState.IDLE, FlightState.WAITING_TASK):
            matched, _ = self._start.evaluate(source, payload, now_ms)
            if matched:
                self.task_started_ms = now_ms
                self.task_id = self._extract_task_id(payload) or self.task_id
                self.state = FlightState.RECORDING

        elif self.state == FlightState.RECORDING:
            # 持续提取 task_id（首次触发未带 ID 时）
            if not self.task_id:
                self.task_id = self._extract_task_id(payload)
            matched, _ = self._end.evaluate(source, payload, now_ms)
            if matched:
                self.task_ended_ms = now_ms
                self.end_reason = "auto_idle"
                self.state = FlightState.FINALIZING

        return self.state

    def force_stop(self, now_ms: int, reason: str) -> None:
        self.task_ended_ms = now_ms
        self.end_reason = reason
        self.state = FlightState.FINALIZING

    @staticmethod
    def _extract_task_id(payload: dict[str, Any]) -> Optional[str]:
        # `data` may be a dict (services payload) or a list (some events payloads
        # like dock_events carrying ADS-B arrays). Only dict has flight_id/task_id;
        # otherwise fall back to top-level payload.flight_id.
        data = payload.get("data")
        if not isinstance(data, dict):
            data = {}
        return data.get("flight_id") or data.get("task_id") or payload.get("flight_id")
