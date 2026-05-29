import copy
from collections import deque
from typing import Any


class LiveState:
    """In-memory snapshot of dock/drone state, fed by MqttSubscriber, read by API.

    单进程线程不安全（无需锁）—— FastAPI 单线程 event loop + gmqtt 同一 loop。
    """

    def __init__(
        self,
        events_ring_size: int = 20,
        trail_max: int = 500,
        controls_ring_size: int = 30,
    ):
        self._dock: dict[str, Any] = {}
        self._drone: dict[str, Any] = {}
        self._events: deque[dict[str, Any]] = deque(maxlen=events_ring_size)
        # 控制消息环：drc/down（飞控指令，如 stick_control）+ services（航线/任务）。
        # 大小独立于 events 因为 DRC 在真实飞行中可能 30 Hz 推，比 events 密度高。
        self._controls: deque[dict[str, Any]] = deque(maxlen=controls_ring_size)
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
                # 完整 payload 一起留，前端点击事件行后能弹 modal 看明细 + 复制。
                "payload": payload,
            })
        elif suffix in ("drc/down", "services"):
            # 控制消息：飞控指令（drc/down，如 stick_control）+ 航线/服务（services）。
            # 完整 payload 一起留下，前端 modal 展开后能看 + 复制（30 条环冷启动后才会顶出来）。
            self._controls.append({
                "recv_ts_ms": recv_ts_ms,
                "topic": topic,
                "suffix": suffix,
                "method": payload.get("method", "?"),
                "payload": payload,
            })

    # (osd_data_key, snapshot_key) — dock 顶层字段映射
    _DOCK_FIELD_MAP = (
        ("mode_code", "mode_code"),
        ("cover_state", "cover_state"),
        ("drone_in_dock", "drone_in_dock"),
        ("flighttask_step_code", "flighttask_step_code"),
        ("drc_state", "drc_state"),
        ("environment_temperature", "environment_temperature"),
        ("temperature", "temperature"),
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

    # 与 recorder/FlightDetector 同步的"正在作业"集合：
    # 0=作业准备中、1=飞行作业中、2=作业后状态恢复。其它（典型 5=任务空闲）
    # 视为"空闲"，dashboard 在收到这种状态时把已累积的飞行轨迹清空。
    _RECORDING_STEP_CODES = (0, 1, 2)

    def _update_dock(self, sn: str, data: dict, recv_ts_ms: int) -> None:
        self._dock["sn"] = sn
        self._dock["last_recv_ts_ms"] = recv_ts_ms
        # 在写入 dock 字段之前先看新的 flighttask_step_code，决定要不要清轨迹。
        # 注意：DJI 只在状态变化时下发 flighttask_step_code，本 OSD 不带该字段时
        # 视为"沿用上一次"，不触发清空。
        new_step = data.get("flighttask_step_code")
        if isinstance(new_step, int) and new_step not in self._RECORDING_STEP_CODES:
            # 任务空闲 → 清空轨迹（幂等：空轨迹再清还是空）。
            self._trail.clear()
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
        # 相机变焦倍数 + 云台角：DJI Dock 3 在 OSD 里这样组织——
        #   data.cameras[]  : 每个挂载点一条，含 payload_index ("99-0-0") 与 zoom_factor
        #   data.<payload_index> : 该挂载点的子对象，含 gimbal_pitch / gimbal_yaw / gimbal_roll
        # 取 cameras[0]（Dock 3 / Matrice 3D 主荷载只有一个），并以它的 payload_index
        # 反查 data 顶层里对应的子对象拿 gimbal 角。
        # 注意：变焦倍数有意取 cameras[].zoom_factor（综合/活动倍率），
        # 不取 data.<payload_index>.zoom_factor（单镜头几何倍率），两者数值不同。
        cameras = data.get("cameras")
        if isinstance(cameras, list) and cameras and isinstance(cameras[0], dict):
            cam = cameras[0]
            if "zoom_factor" in cam:
                self._drone["zoom_factor"] = cam["zoom_factor"]
            payload_idx = cam.get("payload_index")
            mount = data.get(payload_idx) if isinstance(payload_idx, str) else None
            if isinstance(mount, dict):
                if "gimbal_pitch" in mount:
                    self._drone["gimbal_pitch"] = mount["gimbal_pitch"]
                if "gimbal_yaw" in mount:
                    self._drone["gimbal_yaw"] = mount["gimbal_yaw"]
        lat = data.get("latitude")
        lon = data.get("longitude")
        if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
            # 只在 dock 处于"正在作业"状态时累积轨迹。
            # 未知（dock OSD 还没来过）时宽容收，避免开盘瞬间漏点；
            # 一旦 dock 报了空闲（→ _update_dock 也会清掉历史），
            # drone 这边就不再追加（否则 drone 落地后 dock 位置会被反复追加成一团）。
            current_step = self._dock.get("flighttask_step_code")
            if current_step is None or current_step in self._RECORDING_STEP_CODES:
                self._trail.append([lat, lon])

    def snapshot(self) -> dict[str, Any]:
        """返回深拷贝，外部修改不影响内部。"""
        return {
            "dock": copy.deepcopy(self._dock),
            "drone": copy.deepcopy(self._drone),
            "drone_trail": [list(p) for p in self._trail],
            "events": [copy.deepcopy(e) for e in self._events],
            "controls": [copy.deepcopy(c) for c in self._controls],
            "topic_counts": dict(self._topic_counts),
        }
