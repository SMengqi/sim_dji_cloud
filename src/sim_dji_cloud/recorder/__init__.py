import json
import time
from pathlib import Path
from typing import Any, Callable, Optional
from loguru import logger

from sim_dji_cloud.utils.time_ms import now_ms
from sim_dji_cloud.storage.manifest import ManifestBuilder
from sim_dji_cloud.storage.rotation import RotatingJsonlWriter
from sim_dji_cloud.recorder.topic_router import (
    Source, route_topic, file_name_for_topic, is_denied,
)
from sim_dji_cloud.recorder.write_queue import TopicWriteQueue
from sim_dji_cloud.recorder.flight_detector import FlightDetector, FlightState
from sim_dji_cloud.recorder.video_writer import VideoWriter, resolve_video_source_url


class Recorder:
    """录制顶层编排。

    阶段一约束：调用 on_mqtt_message 注入消息（CLI 把真实 MQTT 客户端接进来）。
    """

    def __init__(
        self,
        config: dict[str, Any],
        dock_sn: str,
        drone_sn: Optional[str],
        video_writer_factory: Callable[..., VideoWriter] = VideoWriter,
    ):
        self.config = config
        self.dock_sn = dock_sn
        self.drone_sn = drone_sn
        self.storage_root = Path(config["storage"]["root"])
        fd = config.get("flight_detection", {}) or {}
        self._detector = FlightDetector(
            record_steps=fd.get("record_steps", [0, 1, 2]),
            idle_debounce_seconds=fd.get("idle_debounce_seconds", 5),
        )
        self._queues: dict[str, TopicWriteQueue] = {}
        self._topic_routed: dict[str, Any] = {}
        self._writers: dict[str, RotatingJsonlWriter] = {}
        self.flight_dir: Optional[Path] = None
        self._manifest: Optional[ManifestBuilder] = None
        self._task_started_ms: Optional[int] = None
        self._video_writer_factory = video_writer_factory
        self._video_writer: Optional[VideoWriter] = None
        self._video_started_ms: Optional[int] = None
        self._drained_topics: set[str] = set()

    async def start_async_components(self) -> None:
        """阶段一无后台任务；预留接口给阶段二。"""

    async def on_mqtt_message(self, topic: str, payload: bytes, recv_ts_ms: int) -> None:
        if is_denied(topic, self.config["mqtt"].get("deny_topics", [])):
            return

        # Device-SN 白名单：多机场共享 broker 时，仅保留本机场 + 已确认飞行器
        # 的 topic。drone_sn 未知时，只接受 dock_sn 的消息（drone_sn 会从
        # dock_osd.data.sub_device.device_sn 回填，之后才放行飞行器消息）。
        parts = topic.split("/")
        msg_device_sn = parts[2] if len(parts) >= 3 else ""
        if msg_device_sn != self.dock_sn and (
            self.drone_sn is None or msg_device_sn != self.drone_sn
        ):
            return

        # gmqtt 通常给 bytes，但某些版本 / qos 路径会给 str；
        # 守一下避免裸 .decode 触发 AttributeError。
        # Regression: test_on_mqtt_message_accepts_str_payload.
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
            routed = route_topic(topic, self.dock_sn, self.drone_sn)
            self._topic_routed[topic] = routed

        # 回填 drone_sn —— 从 dock_osd.data.sub_device.device_sn 精确提取，
        # 避免之前"首条非 dock osd 即认为是飞行器"启发式在多机场环境下出错。
        if self.drone_sn is None and routed.source == Source.DOCK_OSD:
            data = payload_obj.get("data") if isinstance(payload_obj.get("data"), dict) else {}
            sub = data.get("sub_device") if isinstance(data, dict) else None
            sn = sub.get("device_sn") if isinstance(sub, dict) else None
            if sn:
                self.drone_sn = sn
                if self._manifest is not None:
                    self._manifest.update_drone_sn(self.drone_sn)
                # 重新解析所有已缓存 topic（drone_sn 现在已知，路由可能变化）
                for cached_topic in list(self._topic_routed.keys()):
                    self._topic_routed[cached_topic] = route_topic(
                        cached_topic, self.dock_sn, self.drone_sn,
                    )
                routed = self._topic_routed[topic]
                logger.info(
                    "drone_sn backfilled from dock_osd.sub_device: {}", self.drone_sn,
                )

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
            base_name = f"{self.dock_sn}_{ts}"
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
            dock_sn=self.dock_sn,
            drone_sn=self.drone_sn or "unknown",
            started_at_recv_ms=started_ms,
        )
        logger.info("flight dir opened: {}", self.flight_dir)

        # 单进程一次飞行；进入新飞行前清掉上一段视频状态（防御性，正常不会触发）
        self._video_writer = None
        self._video_started_ms = None
        vcfg = self.config.get("video") or {}  # video: 空段时 yaml 给 None，归一为 {}
        if vcfg.get("enabled"):
            url = resolve_video_source_url(vcfg)
            if url:
                try:
                    video_dir = self.flight_dir / "video"
                    vw = self._video_writer_factory(
                        url, video_dir, vcfg.get("ffmpeg_extra_args", []))
                    vw.start()
                    self._video_writer = vw
                    self._video_started_ms = started_ms
                    # 真正的"已开始录"由 video_writer 内部在 ffmpeg 起来时打 "ffmpeg launched"；
                    # supervisor 起来 / 没探到流退出 也都由 video_writer 自己日志，这里不再重复。
                except Exception:
                    logger.exception("视频启动失败；继续录 MQTT（manifest.video=null）")
                    self._video_writer = None
            else:
                logger.warning("video.enabled 但未配置 source_url_override，跳过视频")

    async def _rename_pending_to_task(self, task_id: str) -> None:
        """Rename pending_ dir to task_id-named dir.

        关键性质：函数体内**全部同步**（无 await），保证调用期间不会被
        并发 on_mqtt_message 打断。文件句柄按 inode 跟踪，目录 rename 后
        已打开的 writer 仍可继续写入；我们只需把 base_path 指向新目录，
        让未来 rotation 的新分卷落在新位置。
        """
        assert self.flight_dir is not None
        if not self.flight_dir.name.startswith("pending_"):
            return  # 已被另一并发调用 rename 完成

        started_ms = self._task_started_ms or now_ms()
        ts = time.strftime("%Y%m%d-%H%M%S", time.localtime(started_ms / 1000))
        base_name = f"{self.dock_sn}_{ts}"
        new_dir = self.flight_dir.parent / base_name
        if new_dir.exists() and new_dir != self.flight_dir:
            ms3 = f"{started_ms % 1000:03d}"
            new_dir = self.flight_dir.parent / f"{base_name}_{ms3}"

        # 原子 rename。文件句柄继续有效（Linux inode 语义）。
        self.flight_dir.rename(new_dir)
        self.flight_dir = new_dir

        # 修正每个 writer 的 base_path，使后续分卷落在新目录里。
        # 已打开的当前卷 fd 不需要重建——它仍然有效，写入会落到 rename
        # 后的新路径（因为 fd 跟随 inode，inode 跟随磁盘文件，文件跟随目录）。
        new_topics_dir = new_dir / "topics"
        for writer in self._writers.values():
            writer.base_path = new_topics_dir / writer.base_path.name

        # ManifestBuilder 持有 flight_dir 路径，需要重建指向新位置。
        # 旧 builder 已累积的 gaps（pending 阶段 MQTT 断连 / 未来其它来源）
        # 必须复制进新 builder，否则 rename 后丢失。
        # Regression: test_rename_preserves_prior_manifest_gaps.
        prior_gaps = self._manifest.data["gaps"] if self._manifest else []
        self._manifest = ManifestBuilder(
            flight_dir=self.flight_dir,
            task_id=task_id,
            dock_sn=self.dock_sn,
            drone_sn=self.drone_sn or "unknown",
            started_at_recv_ms=self._task_started_ms or now_ms(),
        )
        for g in prior_gaps:
            self._manifest.add_gap(g["reason"], g["start_ms"], g["end_ms"])
        # 视频：目录已 rename（ffmpeg fd 跟随 inode 继续写），仅需把 output_dir
        # 指向新目录，使 stop() 时 timing.json 落在新位置。
        if self._video_writer is not None:
            self._video_writer.output_dir = self.flight_dir / "video"
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
        """完成当前飞行段，把 manifest 写盘。

        Args
        ----
        extra_gaps:
            外部累积的 gap 列表（典型来源：``MqttRecorderClient.gaps`` 在
            MQTT 断连时填充）。每条 dict 含 ``reason / start_ms / end_ms``。
            finalize 前一次性合并进 manifest.gaps，让下游 inspect/selfcheck
            看到真实的录制中断段。
            Regression (review MAJOR): 旧实现 mqtt 的 gaps 累积但从未写到
            manifest → manifest.gaps 永远空。
        """
        if self.flight_dir is None:
            raise RuntimeError("no flight in progress")

        # 跟踪已被 finalize drain 过的 queues —— 后续 reset_for_next_flight
        # 不再重复 drain。重复 await 一个已 crash 的 consumer 会再次抛出存储
        # 的异常被 reset 的 except 静默吃掉。
        # Regression: test_reset_does_not_redrain_after_finalize.
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
        # rename 现在不再关闭/重建 writer，每个 topic 的全生命周期数据都在
        # 同一 RotatingJsonlWriter 里累积，files_metadata() 直接给出完整列表。
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

        if self._video_writer is not None:
            try:
                self._video_writer.stop()  # SIGINT→10s→SIGKILL，并写 timing.json
                # _video_writer 非空即 _video_started_ms 已设；显式 is-not-None 防 0 被当假
                start_ms = self._video_started_ms if self._video_started_ms is not None else ended
                duration = max(0, ended - start_ms)  # 时钟异常时兜底，避免负时长
                self._manifest.set_video(self._video_writer.manifest_video_block(duration))
            except Exception:
                logger.exception("视频收尾失败；manifest.video 置 null")
                self._manifest.set_video(None)

        self._manifest.finalize(
            ended_at_recv_ms=ended,
            finalize_reason=finalize_reason,
            status="ok",
        )
        return self.flight_dir

    async def reset_for_next_flight(self) -> None:
        """一段任务 finalize 后清空状态，准备录下一段（长跑多任务）。

        注意：保留 dock_sn / 已学到的 drone_sn / detector 的 sticky _last_step。
        已被 finalize drain 过的 queues 不重复 drain（重复 await crashed
        consumer 会再次抛出存储的异常被 except 静默吃掉）。
        """
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
        self._video_writer = None
        self._video_started_ms = None
        self._drained_topics.clear()
        self._detector.reset()
