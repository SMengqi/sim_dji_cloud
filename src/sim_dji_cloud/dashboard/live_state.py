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
        self._known_dock_sn: str | None = None
        self._known_drone_sn: str | None = None

    def update(self, topic: str, payload: dict[str, Any], recv_ts_ms: int) -> None:
        self._topic_counts[topic] = self._topic_counts.get(topic, 0) + 1

        parts = topic.split("/")
        suffix = "/".join(parts[3:]) if len(parts) >= 4 else ""
        device_sn = parts[2] if len(parts) >= 3 else ""
        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}

        if suffix == "osd":
            sub = data.get("sub_device")
            if isinstance(sub, dict):
                self._known_dock_sn = device_sn
                child_sn = sub.get("device_sn")
                if child_sn:
                    self._known_drone_sn = child_sn

            if device_sn and device_sn == self._known_dock_sn:
                self._update_dock(device_sn, data, recv_ts_ms)
            elif device_sn and device_sn == self._known_drone_sn:
                self._update_drone(device_sn, data, recv_ts_ms)
            elif isinstance(sub, dict):
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

    # (osd_data_key, snapshot_key) — dock 顶层字段映射
    _DOCK_FIELD_MAP = (
        ("mode_code", "mode_code"),
        ("cover_state", "cover_state"),
        ("drone_in_dock", "drone_in_dock"),
        ("flighttask_step_code", "flighttask_step_code"),
        ("drc_state", "drc_state"),
        ("environment_temperature", "temperature"),
        ("humidity", "humidity"),
        ("wind_speed", "wind_speed"),
        ("rainfall", "rainfall"),
        ("latitude", "latitude"),
        ("longitude", "longitude"),
        ("height", "height"),
    )

    # drone 顶层字段映射
    _DRONE_FIELD_MAP = (
        ("mode_code", "mode_code"),
        ("attitude_head", "attitude_head"),
        ("attitude_pitch", "attitude_pitch"),
        ("attitude_roll", "attitude_roll"),
        ("horizontal_speed", "horizontal_speed"),
        ("vertical_speed", "vertical_speed"),
        ("height", "height"),
        ("home_distance", "home_distance"),
        ("latitude", "latitude"),
        ("longitude", "longitude"),
        ("wind_speed", "wind_speed"),
    )

    def _update_dock(self, sn: str, data: dict, recv_ts_ms: int) -> None:
        self._dock["sn"] = sn
        self._dock["last_recv_ts_ms"] = recv_ts_ms
        for src, dst in self._DOCK_FIELD_MAP:
            if src in data:
                self._dock[dst] = data[src]
        net = data.get("network_state")
        if isinstance(net, dict):
            if "quality" in net:
                self._dock["network_quality"] = net["quality"]
            if "rate" in net:
                self._dock["network_rate"] = net["rate"]
        sub = data.get("sub_device")
        if isinstance(sub, dict) and "device_sn" in sub:
            self._dock["paired_drone_sn"] = sub["device_sn"]

    def _update_drone(self, sn: str, data: dict, recv_ts_ms: int) -> None:
        self._drone["sn"] = sn
        self._drone["last_recv_ts_ms"] = recv_ts_ms
        for src, dst in self._DRONE_FIELD_MAP:
            if src in data:
                self._drone[dst] = data[src]
        battery = data.get("battery")
        if isinstance(battery, dict) and "capacity_percent" in battery:
            self._drone["battery_pct"] = battery["capacity_percent"]
        position = data.get("position_state")
        if isinstance(position, dict):
            if "gps_number" in position:
                self._drone["gps_satellites"] = position["gps_number"]
            if "rtk_number" in position:
                self._drone["rtk_satellites"] = position["rtk_number"]
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
