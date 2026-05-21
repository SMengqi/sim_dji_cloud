import json
import time
from pathlib import Path
from typing import Any, Optional
from loguru import logger

from sim_dji_cloud.utils.time_ms import now_ms
from sim_dji_cloud.storage.manifest import ManifestBuilder
from sim_dji_cloud.storage.rotation import RotatingJsonlWriter
from sim_dji_cloud.recorder.topic_router import (
    Source, route_topic, file_name_for_topic, is_denied,
)
from sim_dji_cloud.recorder.write_queue import TopicWriteQueue
from sim_dji_cloud.recorder.flight_detector import FlightDetector, FlightState


class Recorder:
    """录制顶层编排。

    阶段一约束：调用 on_mqtt_message 注入消息（CLI 把真实 MQTT 客户端接进来）。
    """

    def __init__(
        self,
        config: dict[str, Any],
        dock_sn: str,
        drone_sn: Optional[str],
    ):
        self.config = config
        self.dock_sn = dock_sn
        self.drone_sn = drone_sn
        self.storage_root = Path(config["storage"]["root"])
        rules = config["flight_detection"]["rules"]
        self._detector = FlightDetector(
            start_rules=rules.get("start", []),
            end_rules=rules.get("end", []),
        )
        self._queues: dict[str, TopicWriteQueue] = {}
        self._topic_routed: dict[str, Any] = {}
        self._writers: dict[str, RotatingJsonlWriter] = {}
        # Accumulator for files_metadata across writer lifecycles (e.g. pending → task_id rename
        # discards the old writers but their .jsonl files move with the directory and must stay
        # in the manifest). Keyed by topic, value is the list of file-meta dicts from prior writers.
        self._closed_files_meta: dict[str, list[dict[str, Any]]] = {}
        self.flight_dir: Optional[Path] = None
        self._manifest: Optional[ManifestBuilder] = None
        self._task_started_ms: Optional[int] = None

    async def start_async_components(self) -> None:
        """阶段一无后台任务；预留接口给阶段二。"""

    async def on_mqtt_message(self, topic: str, payload: bytes, recv_ts_ms: int) -> None:
        if is_denied(topic, self.config["mqtt"].get("deny_topics", [])):
            return

        try:
            payload_obj = json.loads(payload.decode("utf-8")) if payload else {}
        except (json.JSONDecodeError, UnicodeDecodeError):
            logger.warning("non-JSON payload on topic {}", topic)
            return

        routed = self._topic_routed.get(topic)
        if routed is None:
            routed = route_topic(topic, self.dock_sn, self.drone_sn)
            self._topic_routed[topic] = routed

        # 回填 drone_sn（首条飞行器 osd）
        if self.drone_sn is None and routed.source == Source.DRONE_OSD:
            self.drone_sn = routed.device_sn
            if self._manifest is not None:
                self._manifest.update_drone_sn(self.drone_sn)
            # 重新解析所有已缓存 topic（dock/drone 判定可能改变）
            for cached_topic in list(self._topic_routed.keys()):
                self._topic_routed[cached_topic] = route_topic(
                    cached_topic, self.dock_sn, self.drone_sn,
                )
            routed = route_topic(topic, self.dock_sn, self.drone_sn)
            self._topic_routed[topic] = routed

        prev_state = self._detector.state
        new_state = self._detector.feed(routed.source, payload_obj, recv_ts_ms)

        if prev_state != FlightState.RECORDING and new_state == FlightState.RECORDING:
            await self._open_flight_dir(recv_ts_ms)

        if self._detector.state != FlightState.RECORDING:
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
            name = f"{task_id}__{self.dock_sn}__{ts}"
        else:
            name = f"pending_{started_ms}"
        self.flight_dir = self.storage_root / name
        self.flight_dir.mkdir(parents=True, exist_ok=True)
        (self.flight_dir / "topics").mkdir(exist_ok=True)
        self._task_started_ms = started_ms
        self._manifest = ManifestBuilder(
            flight_dir=self.flight_dir,
            task_id=task_id or "unknown",
            dock_sn=self.dock_sn,
            drone_sn=self.drone_sn or "unknown",
            started_at_recv_ms=started_ms,
        )
        logger.info("flight dir opened: {}", self.flight_dir)

    async def _rename_pending_to_task(self, task_id: str) -> None:
        assert self.flight_dir is not None
        if not self.flight_dir.name.startswith("pending_"):
            return

        # Drain existing queues, preserve their files_metadata before clearing writers.
        # The .jsonl files themselves move with the directory rename, but the per-writer
        # metadata (volume names, counts, first/last ms) lives only in memory — without
        # this snapshot the records written pre-rename would be on disk but missing from
        # the final manifest.
        for q in list(self._queues.values()):
            await q.drain_and_close()
        for topic, writer in self._writers.items():
            self._closed_files_meta.setdefault(topic, []).extend(writer.files_metadata())
        old_queues = list(self._queues.keys())
        self._queues.clear()
        self._writers.clear()

        ts = time.strftime("%Y%m%d-%H%M%S", time.localtime((self._task_started_ms or now_ms()) / 1000))
        new_dir = self.flight_dir.parent / f"{task_id}__{self.dock_sn}__{ts}"
        self.flight_dir.rename(new_dir)
        self.flight_dir = new_dir
        self._manifest = ManifestBuilder(
            flight_dir=self.flight_dir,
            task_id=task_id,
            dock_sn=self.dock_sn,
            drone_sn=self.drone_sn or "unknown",
            started_at_recv_ms=self._task_started_ms or now_ms(),
        )
        # Reopen writers (now pointing into renamed dir). Each new writer will start
        # its volume index at 1; since pre-rename volume files already exist with
        # the same name (e.g. topic.0001.jsonl), JsonlWriter opens in "ab" mode and
        # appends to them rather than truncating — so pre-rename records survive
        # and post-rename records are appended in order.
        for topic in old_queues:
            await self._ensure_topic_writer(topic)

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

    async def finalize_and_close(self, finalize_reason: str) -> Path:
        if self.flight_dir is None:
            raise RuntimeError("no flight in progress")

        for q in self._queues.values():
            await q.drain_and_close()

        assert self._manifest is not None
        # Merge pre-rename (_closed_files_meta) + post-rename (current writers) metadata
        # per topic. Same volume file may appear in both lists (rename caused new writer
        # to append to existing file); dedupe by file name and aggregate count + ts range.
        all_topics = set(self._writers.keys()) | set(self._closed_files_meta.keys())
        for topic in all_topics:
            routed = self._topic_routed[topic]
            pre = self._closed_files_meta.get(topic, [])
            post = self._writers[topic].files_metadata() if topic in self._writers else []
            files_merged: dict[str, dict[str, Any]] = {}
            for meta in pre + post:
                name = meta["name"]
                if name not in files_merged:
                    files_merged[name] = {
                        "name": f"topics/{name}",
                        "count": meta["count"],
                        "first_ms": meta.get("first_ms"),
                        "last_ms": meta.get("last_ms"),
                    }
                else:
                    existing = files_merged[name]
                    existing["count"] += meta["count"]
                    if meta.get("first_ms") is not None:
                        existing["first_ms"] = (
                            meta["first_ms"] if existing["first_ms"] is None
                            else min(existing["first_ms"], meta["first_ms"])
                        )
                    if meta.get("last_ms") is not None:
                        existing["last_ms"] = (
                            meta["last_ms"] if existing["last_ms"] is None
                            else max(existing["last_ms"], meta["last_ms"])
                        )
            self._manifest.record_topic(
                topic=topic,
                device_sn=routed.device_sn,
                direction=routed.direction,
                files=list(files_merged.values()),
            )

        ended = self._detector.task_ended_ms or now_ms()
        self._manifest.finalize(
            ended_at_recv_ms=ended,
            finalize_reason=finalize_reason,
            status="ok",
        )
        return self.flight_dir
