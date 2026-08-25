import json
import time
from pathlib import Path
from typing import Any, Optional
from loguru import logger

from sim_dji_cloud.utils.time_ms import now_ms
from sim_dji_cloud.storage.manifest import ManifestBuilder
from sim_dji_cloud.storage.rotation import RotatingJsonlWriter
from sim_dji_cloud.recorder.topic_router import file_name_for_topic, is_denied
from sim_dji_cloud.recorder.pilot_topic_router import Source, route_topic
from sim_dji_cloud.recorder.write_queue import TopicWriteQueue
from sim_dji_cloud.recorder.pilot_flight_detector import PilotFlightDetector, PilotFlightState


class PilotRecorder:
    """Pilot-to-Cloud（RC Plus 2 + M400）录制顶层编排。

    结构照抄 dock 版 Recorder（recorder/__init__.py）：网关是遥控器（rc_sn）
    而不是机场；子设备（aircraft_sn）从 sys/product/{rc_sn}/status 的拓扑
    消息学到，不是从 osd payload；录制窗口由 PilotFlightDetector 基于拓扑
    上下线驱动，不是 flighttask_step_code 状态机。不含视频。
    manifest 字段复用现有 dock_sn/drone_sn（语义上装 rc_sn/aircraft_sn），
    schema 不变，保留未来接 play/dashboard 的兼容余地。
    """

    def __init__(
        self,
        config: dict[str, Any],
        rc_sn: str,
        aircraft_sn: Optional[str],
    ):
        self.config = config
        self.rc_sn = rc_sn
        self.aircraft_sn = aircraft_sn
        self.storage_root = Path(config["storage"]["root"])
        fd = config.get("pilot_flight_detection", {}) or {}
        self._detector = PilotFlightDetector(
            idle_debounce_seconds=fd.get("idle_debounce_seconds", 5),
        )
        self._queues: dict[str, TopicWriteQueue] = {}
        self._topic_routed: dict[str, Any] = {}
        self._writers: dict[str, RotatingJsonlWriter] = {}
        self.flight_dir: Optional[Path] = None
        self._manifest: Optional[ManifestBuilder] = None
        self._task_started_ms: Optional[int] = None
        self._drained_topics: set[str] = set()

    async def start_async_components(self) -> None:
        """无后台任务；接口跟 dock 版 Recorder 对齐。"""

    async def on_mqtt_message(self, topic: str, payload: bytes, recv_ts_ms: int) -> None:
        if is_denied(topic, self.config["mqtt"].get("deny_topics", [])):
            return

        parts = topic.split("/")
        msg_device_sn = parts[2] if len(parts) >= 3 else ""
        if msg_device_sn != self.rc_sn and (
            self.aircraft_sn is None or msg_device_sn != self.aircraft_sn
        ):
            return

        try:
            if not payload:
                payload_obj = {}
            elif isinstance(payload, bytes):
                payload_obj = json.loads(payload.decode("utf-8"))
            elif isinstance(payload, str):
                payload_obj = json.loads(payload)
            else:
                logger.warning("unexpected payload type {} on topic {}",
                               type(payload).__name__, topic)
                return
        except (json.JSONDecodeError, UnicodeDecodeError):
            logger.warning("non-JSON payload on topic {}", topic)
            return

        routed = self._topic_routed.get(topic)
        if routed is None:
            routed = route_topic(topic, self.rc_sn, self.aircraft_sn)
            self._topic_routed[topic] = routed

        prev_state = self._detector.state
        new_state = self._detector.feed(routed.source, payload_obj, recv_ts_ms)

        # 回填 aircraft_sn —— 从拓扑消息学到；跟 dock 版一样，之前缓存的路由
        # 需要重新解析（学到 SN 前，飞行器自己的 topic 会被判成 UNKNOWN）。
        if self.aircraft_sn is None and self._detector.aircraft_sn is not None:
            self.aircraft_sn = self._detector.aircraft_sn
            if self._manifest is not None:
                self._manifest.update_drone_sn(self.aircraft_sn)
            for cached_topic in list(self._topic_routed.keys()):
                self._topic_routed[cached_topic] = route_topic(
                    cached_topic, self.rc_sn, self.aircraft_sn,
                )
            routed = self._topic_routed[topic]
            logger.info("aircraft_sn learned from topology: {}", self.aircraft_sn)

        if prev_state != PilotFlightState.RECORDING and new_state == PilotFlightState.RECORDING:
            await self._open_flight_dir(recv_ts_ms)

        if self._detector.state != PilotFlightState.RECORDING:
            return

        if (
            self.flight_dir
            and self.flight_dir.name.startswith("pending_")
            and self._detector.task_id
        ):
            await self._rename_pending_to_task(self._detector.task_id)

        await self._write_record(topic, routed, payload_obj, recv_ts_ms)

    async def _open_flight_dir(self, started_ms: int) -> None:
        ts = time.strftime("%Y%m%d-%H%M%S", time.localtime(started_ms / 1000))
        task_id = self._detector.task_id
        if task_id:
            base_name = f"{self.rc_sn}_{ts}"
            candidate = self.storage_root / base_name
            if candidate.exists():
                ms3 = f"{started_ms % 1000:03d}"
                base_name = f"{base_name}_{ms3}"
            name = base_name
        else:
            name = f"pending_{started_ms}"
        self.flight_dir = self.storage_root / name
        self.flight_dir.mkdir(parents=True, exist_ok=True)
        (self.flight_dir / "topics").mkdir(exist_ok=True)
        self._task_started_ms = started_ms
        self._manifest = ManifestBuilder(
            flight_dir=self.flight_dir,
            task_id=task_id or "unknown",
            dock_sn=self.rc_sn,
            drone_sn=self.aircraft_sn or "unknown",
            started_at_recv_ms=started_ms,
        )
        logger.info("pilot flight dir opened: {}", self.flight_dir)

    async def _rename_pending_to_task(self, task_id: str) -> None:
        assert self.flight_dir is not None
        if not self.flight_dir.name.startswith("pending_"):
            return  # 已被另一并发调用 rename 完成

        started_ms = self._task_started_ms or now_ms()
        ts = time.strftime("%Y%m%d-%H%M%S", time.localtime(started_ms / 1000))
        base_name = f"{self.rc_sn}_{ts}"
        new_dir = self.flight_dir.parent / base_name
        if new_dir.exists() and new_dir != self.flight_dir:
            ms3 = f"{started_ms % 1000:03d}"
            new_dir = self.flight_dir.parent / f"{base_name}_{ms3}"

        self.flight_dir.rename(new_dir)
        self.flight_dir = new_dir

        new_topics_dir = new_dir / "topics"
        for writer in self._writers.values():
            writer.base_path = new_topics_dir / writer.base_path.name

        prior_gaps = self._manifest.data["gaps"] if self._manifest else []
        self._manifest = ManifestBuilder(
            flight_dir=self.flight_dir,
            task_id=task_id,
            dock_sn=self.rc_sn,
            drone_sn=self.aircraft_sn or "unknown",
            started_at_recv_ms=self._task_started_ms or now_ms(),
        )
        for g in prior_gaps:
            self._manifest.add_gap(g["reason"], g["start_ms"], g["end_ms"])
        logger.info("renamed pending dir to: {}", new_dir.name)

    async def _ensure_topic_writer(self, topic: str) -> TopicWriteQueue:
        if topic in self._queues:
            return self._queues[topic]
        assert self.flight_dir is not None
        file_base = self.flight_dir / "topics" / file_name_for_topic(topic).replace(".jsonl", "")
        writer = RotatingJsonlWriter(
            base_path=file_base,
            rotate_max_records=self.config["storage"]["rotate_max_records"],
            rotate_max_bytes=self.config["storage"]["rotate_max_bytes"],
            flush_max_records=self.config["storage"]["flush_max_records"],
        )
        self._writers[topic] = writer
        q = TopicWriteQueue(
            writer=writer,
            flush_max_records=self.config["storage"]["flush_max_records"],
            flush_interval_ms=self.config["storage"]["flush_interval_ms"],
            queue_max_size=self.config["storage"]["queue_max_size"],
        )
        await q.start()
        self._queues[topic] = q
        return q

    async def _write_record(
        self, topic: str, routed, payload_obj: dict, recv_ts_ms: int,
    ) -> None:
        q = await self._ensure_topic_writer(topic)
        record = {
            "recv_ts_ms": recv_ts_ms,
            "dji_ts_ms": payload_obj.get("timestamp"),
            "direction": routed.direction,
            "topic": topic,
            "payload": payload_obj,
        }
        await q.put(record)

    async def finalize_and_close(
        self,
        finalize_reason: str,
        *,
        extra_gaps: Optional[list[dict]] = None,
    ) -> Path:
        if self.flight_dir is None:
            raise RuntimeError("no flight in progress")

        for topic, q in self._queues.items():
            await q.drain_and_close()
            self._drained_topics.add(topic)

        assert self._manifest is not None
        if extra_gaps:
            for g in extra_gaps:
                self._manifest.add_gap(
                    g.get("reason", "unknown"),
                    int(g["start_ms"]),
                    int(g["end_ms"]),
                )
        for topic, writer in self._writers.items():
            routed = self._topic_routed[topic]
            files = writer.files_metadata()
            self._manifest.record_topic(
                topic=topic,
                device_sn=routed.device_sn,
                direction=routed.direction,
                files=[
                    {"name": f"topics/{f['name']}", **{k: v for k, v in f.items() if k != "name"}}
                    for f in files
                ],
            )

        ended = self._detector.task_ended_ms or now_ms()
        self._manifest.finalize(
            ended_at_recv_ms=ended,
            finalize_reason=finalize_reason,
            status="ok",
        )
        return self.flight_dir

    async def reset_for_next_flight(self) -> None:
        for topic, q in self._queues.items():
            if topic in self._drained_topics:
                continue
            try:
                await q.drain_and_close()
            except Exception:
                logger.exception("drain queue on reset failed (topic={})", topic)
        self._queues = {}
        self._writers = {}
        self._topic_routed = {}
        self.flight_dir = None
        self._manifest = None
        self._task_started_ms = None
        self._drained_topics.clear()
        self._detector.reset()
