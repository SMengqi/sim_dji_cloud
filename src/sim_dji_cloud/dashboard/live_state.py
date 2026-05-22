import copy
from collections import deque
from typing import Any


class LiveState:
    """In-memory snapshot of dock/drone state, fed by MqttSubscriber, read by API.

    单进程线程不安全（无需锁）—— FastAPI 单线程 event loop + gmqtt 同一 loop。
    """

    def __init__(self, events_ring_size: int = 20, trail_max: int = 500):
        self._dock: dict[str, Any] = {}
        self._drone: dict[str, Any] = {}
        self._events: deque[dict[str, Any]] = deque(maxlen=events_ring_size)
        self._trail: deque[list[float]] = deque(maxlen=trail_max)
        self._topic_counts: dict[str, int] = {}

    def update(self, topic: str, payload: dict[str, Any], recv_ts_ms: int) -> None:
        self._topic_counts[topic] = self._topic_counts.get(topic, 0) + 1

        parts = topic.split("/")
        suffix = "/".join(parts[3:]) if len(parts) >= 4 else ""
        device_sn = parts[2] if len(parts) >= 3 else ""
        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}

        if suffix == "osd":
            if isinstance(data.get("sub_device"), dict):
                self._update_dock(device_sn, data, recv_ts_ms)
            else:
                self._update_drone(device_sn, data, recv_ts_ms)
        elif suffix == "events":
            self._events.append({
                "recv_ts_ms": recv_ts_ms,
                "topic": topic,
                "method": payload.get("method", "?"),
                "flight_id": data.get("flight_id"),
            })

    def _update_dock(self, sn: str, data: dict, recv_ts_ms: int) -> None:
        net = data.get("network_state") if isinstance(data.get("network_state"), dict) else {}
        sub = data.get("sub_device") if isinstance(data.get("sub_device"), dict) else {}
        self._dock = {
            "sn": sn,
            "mode_code": data.get("mode_code"),
            "cover_state": data.get("cover_state"),
            "drone_in_dock": data.get("drone_in_dock"),
            "flighttask_step_code": data.get("flighttask_step_code"),
            "drc_state": data.get("drc_state"),
            "temperature": data.get("environment_temperature"),
            "humidity": data.get("humidity"),
            "wind_speed": data.get("wind_speed"),
            "rainfall": data.get("rainfall"),
            "network_quality": net.get("quality"),
            "network_rate": net.get("rate"),
            "latitude": data.get("latitude"),
            "longitude": data.get("longitude"),
            "height": data.get("height"),
            "paired_drone_sn": sub.get("device_sn"),
            "last_recv_ts_ms": recv_ts_ms,
        }

    def _update_drone(self, sn: str, data: dict, recv_ts_ms: int) -> None:
        battery = data.get("battery") if isinstance(data.get("battery"), dict) else {}
        position = data.get("position_state") if isinstance(data.get("position_state"), dict) else {}
        self._drone = {
            "sn": sn,
            "mode_code": data.get("mode_code"),
            "battery_pct": battery.get("capacity_percent"),
            "attitude_head": data.get("attitude_head"),
            "attitude_pitch": data.get("attitude_pitch"),
            "attitude_roll": data.get("attitude_roll"),
            "horizontal_speed": data.get("horizontal_speed"),
            "vertical_speed": data.get("vertical_speed"),
            "height": data.get("height"),
            "home_distance": data.get("home_distance"),
            "latitude": data.get("latitude"),
            "longitude": data.get("longitude"),
            "gps_satellites": position.get("gps_number"),
            "rtk_satellites": position.get("rtk_number"),
            "wind_speed": data.get("wind_speed"),
            "last_recv_ts_ms": recv_ts_ms,
        }
        lat = data.get("latitude")
        lon = data.get("longitude")
        if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
            self._trail.append([lat, lon])

    def snapshot(self) -> dict[str, Any]:
        """返回深拷贝，外部修改不影响内部。"""
        return {
            "dock": copy.deepcopy(self._dock),
            "drone": copy.deepcopy(self._drone),
            "drone_trail": [list(p) for p in self._trail],
            "events": [copy.deepcopy(e) for e in self._events],
            "topic_counts": dict(self._topic_counts),
        }
