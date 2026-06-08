import asyncio
import json
import os
import time
from pathlib import Path
from typing import Optional, Protocol, Callable
from loguru import logger

from sim_dji_cloud.player.jsonl_iterator import JsonlIterator
from sim_dji_cloud.player.scheduler import VirtTimeScheduler
from sim_dji_cloud.player.video_pusher import VideoPusher, plan_video_push
from sim_dji_cloud.player.control_server import ControlServer


class PlayerError(Exception):
    """Player 启动 / 运行时的预期错误（manifest 缺失 / 损坏 / 路径错等）。

    CLI 路径接住翻成 print + exit 2；不要让原生 FileNotFoundError /
    JSONDecodeError 冒到 Python 顶层 traceback。
    """


class _PublisherProto(Protocol):
    async def connect(self) -> None: ...
    async def publish(self, topic: str, payload: bytes, qos: int = 0) -> None: ...
    async def disconnect(self) -> None: ...


class Player:
    """飞行目录回放编排：
    - 加载 manifest，每个 topic 一个 asyncio publisher 协程
    - 协程读 jsonl，按 recv_ts_ms - started_at_recv_ms 等到虚拟时间到再 publish
    - wait_until_done 等所有协程结束
    """

    def __init__(
        self,
        flight_dir: Path,
        publisher: _PublisherProto,
        speed: float = 1.0,
        start_offset_ms: int = 0,
        video_push_url: str | None = None,
        video_pusher_factory: Callable[..., VideoPusher] = VideoPusher,
        video_anchor_offset_ms: int = 0,
        publish_clear_marker_on_stop: bool = True,
        control_sidecar_path: Path | None = None,
    ):
        self.flight_dir = Path(flight_dir)
        self._publisher = publisher
        self._speed = speed
        self._start_offset_ms = start_offset_ms
        self._scheduler = VirtTimeScheduler()
        self._manifest: dict = {}
        self._tasks: list[asyncio.Task] = []
        self._video_push_url = video_push_url
        self._video_pusher_factory = video_pusher_factory
        self._video_anchor_offset_ms = video_anchor_offset_ms
        # SelfCheck loopback 关掉这个；否则合成 OSD 会被回环 Recorder 收掉，
        # 跟原录制对不上、Comparator FAIL。其他正常播放场景默认开。
        self._publish_clear_marker_on_stop = publish_clear_marker_on_stop
        self._video_pusher: Optional[VideoPusher] = None
        self._video_task: Optional[asyncio.Task] = None
        self._control_sidecar_path = control_sidecar_path
        self._control_server: Optional[ControlServer] = None
        # 单一控制锁覆盖 pause/resume/seek —— 三者都 mutate scheduler /
        # _video_task / _video_pusher，必须串行。原来只 seek 有锁，
        # 高频 pause+seek 交错时窗会孤儿掉 ffmpeg 子进程。
        # Regression: test_pause_during_seek_serializes_via_control_lock.
        self._control_lock = asyncio.Lock()
        # 兼容：保留 _seek_lock 别名给 wait_until_done 用同一把锁。
        self._seek_lock = self._control_lock

    async def start(self) -> None:
        # manifest 是 player 的硬依赖；缺失 / 损坏 / 不可读时给 CLI 用户一个
        # 清晰的中文错误而不是原始 Python traceback。
        # Regression: test_start_raises_player_error_on_missing_manifest /
        #             test_start_raises_player_error_on_malformed_manifest.
        manifest_path = self.flight_dir / "manifest.json"
        try:
            manifest_text = manifest_path.read_text()
        except FileNotFoundError as e:
            raise PlayerError(
                f"flight_dir 不存在或缺少 manifest.json: {self.flight_dir}"
            ) from e
        except OSError as e:
            raise PlayerError(
                f"无法读取 manifest.json ({manifest_path}): {e}"
            ) from e
        try:
            self._manifest = json.loads(manifest_text)
        except json.JSONDecodeError as e:
            raise PlayerError(
                f"manifest.json 损坏 ({manifest_path}): {e}"
            ) from e
        await self._publisher.connect()

        started_at = self._manifest.get("started_at_recv_ms", 0)
        self._scheduler.start(
            virt_zero_ms=self._start_offset_ms,
            speed=self._speed,
        )

        for topic_entry in self._manifest.get("topics", []):
            files = [self.flight_dir / f["name"] for f in topic_entry.get("files", [])]
            files = [f for f in files if f.exists()]
            if not files:
                logger.warning("topic {} has no readable files, skipping",
                               topic_entry["topic"])
                continue
            task = asyncio.create_task(
                self._replay_topic(topic_entry, files, started_at)
            )
            self._tasks.append(task)

        video_meta = self._manifest.get("video") or {}
        video_rel = video_meta.get("file")  # e.g. "video/main_<ms>.mp4" or "video/main.mp4"
        video_file = (self.flight_dir / video_rel) if video_rel else None
        video_exists = bool(video_file and video_file.exists())
        plan = plan_video_push(
            self._manifest, self._video_push_url, self._speed,
            video_exists,
            anchor_offset_ms=self._video_anchor_offset_ms,
        )
        if plan is not None:
            self._video_task = asyncio.create_task(self._run_video_push(plan, video_file))

        if self._control_sidecar_path is not None:
            self._control_server = ControlServer(
                player=self,
                sidecar_path=self._control_sidecar_path,
                pid=os.getpid(),
                started_at_ms=int(time.time() * 1000),
            )
            await self._control_server.start()

    async def _run_video_push(self, plan: dict, video_file: "Path | None") -> None:
        try:
            await self._scheduler.wait_until_virt(plan["wait_virt_ms"])
            pusher = self._video_pusher_factory(
                video_file, self._video_push_url, [])
            # 先存引用再 start：ffmpeg Popen 成功后 start() 内若再失败，
            # 仍能在 wait_until_done / _stop_video_for_pause 里被 stop()
            # 清理，避免孤儿子进程。
            # Regression: test_video_pusher_reference_set_before_start.
            self._video_pusher = pusher
            pusher.start(plan["ss_seconds"])
            logger.info("video push started -> {}", self._video_push_url)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("视频推流启动失败；MQTT 回放不受影响")

    async def _replay_topic(
        self,
        topic_entry: dict,
        files: list[Path],
        started_at_ms: int,
    ) -> None:
        topic = topic_entry["topic"]
        for record in JsonlIterator(files):
            recv_ts = record.get("recv_ts_ms")
            if not isinstance(recv_ts, int):
                continue
            virt_target = recv_ts - started_at_ms + self._start_offset_ms
            await self._scheduler.wait_until_virt(virt_target)
            payload = record.get("payload", {})
            payload_bytes = json.dumps(payload, ensure_ascii=False,
                                       separators=(",", ":")).encode("utf-8")
            try:
                await self._publisher.publish(topic, payload_bytes)
            except Exception:
                logger.exception("publish failed for topic {}", topic)

    async def wait_until_done(self) -> None:
        try:
            while True:
                tasks = list(self._tasks)
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                # After gather, an in-progress seek may have cancelled tasks but
                # not yet replaced them. Acquire seek_lock to wait for any pending
                # seek to finish, then re-check final state.
                async with self._seek_lock:
                    remaining = [t for t in self._tasks if not t.done()]
                    if not remaining:
                        break
                    # Else: seek replaced tasks while we were waiting; loop to
                    # wait on the new tasks.
        finally:
            if self._video_task is not None:
                self._video_task.cancel()
                try:
                    await self._video_task
                except (asyncio.CancelledError, Exception):
                    pass
            if self._video_pusher is not None:
                # aclose 把同步 ffmpeg wait 丢线程池，避免冻 asyncio loop
                # 最长 10s（review MAJOR: video_pusher.stop 阻塞事件循环）。
                try:
                    await self._video_pusher.aclose()
                except Exception:
                    logger.exception("停止视频推流失败")
            # 发一条合成 dock OSD 带 flighttask_step_code=5（任务空闲），让
            # sim-dji dashboard 的 LiveState 清掉飞行轨迹。背景：dashboard 只在
            # dock OSD 转到 idle 时清 trail，而 DJI 这字段只在状态变化时发——
            # 录制半途 Ctrl-C 时录制里可能根本没这条 idle 信号，回放结束 / stop
            # 后 dashboard 轨迹会卡在最后一帧。这里主动补一条。SelfCheck
            # loopback 模式下通过 publish_clear_marker_on_stop=False 关掉，
            # 避免污染录制-回放对称性。
            if self._control_server is not None:
                await self._control_server.stop()
                self._control_server = None
            if self._publish_clear_marker_on_stop:
                await self._publish_dashboard_clear_marker()
            await self._publisher.disconnect()

    # ==================================================================
    # Pause / Resume / Seek / Progress  (phase 3 A)
    # ==================================================================

    async def pause(self) -> dict:
        async with self._control_lock:
            if self._scheduler.paused:
                raise RuntimeError("already paused")
            self._scheduler.pause()
            await self._stop_video_for_pause()
            return {"state": "paused", "virt_ms": self._scheduler.virt_now_ms()}

    async def resume(self) -> dict:
        async with self._control_lock:
            if not self._scheduler.paused:
                raise RuntimeError("not paused")
            self._scheduler.resume()
            # Snapshot virt 立即取，传给 _restart —— restart 内部还要 cancel/await
            # 老 video task + aclose 老 pusher，期间 virt 会按 wall 推进。
            # 用 snapshot 让视频 ss_seconds 跟 resume 时刻精确对齐而不是落在
            # await 之后（review MAJOR: 视频起点比 MQTT 落后数秒）。
            target_virt = self._scheduler.virt_now_ms()
            await self._restart_video_at_current_virt(at_virt_ms=target_virt)
            return {"state": "running", "virt_ms": target_virt}

    async def seek(self, virt_ms: int) -> dict:
        async with self._control_lock:
            total = self._total_ms()
            if total is not None and virt_ms >= total:
                virt_ms = max(0, total - 1)
                logger.warning("seek clamped to {}ms (manifest total {})",
                               virt_ms, total)
            for t in self._tasks:
                t.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks.clear()

            self._scheduler.set_virt(virt_ms)

            await self._stop_video_for_pause()

            started_at = self._manifest.get("started_at_recv_ms", 0)
            for topic_entry in self._manifest.get("topics", []):
                files = [self.flight_dir / f["name"]
                         for f in topic_entry.get("files", [])]
                files = [f for f in files if f.exists()]
                if not files:
                    continue
                task = asyncio.create_task(
                    self._replay_topic_from_virt(
                        topic_entry, files, started_at, virt_ms)
                )
                self._tasks.append(task)

            if not self._scheduler.paused:
                # 用 seek 的目标 virt_ms 作 snapshot，跟 _replay_topic_from_virt
                # 的起点对齐；不要再调 virt_now_ms（会包含 stop_video / create_task
                # 这段 await 推进的几十-几百 ms）。
                await self._restart_video_at_current_virt(at_virt_ms=virt_ms)

            return {
                "state": "paused" if self._scheduler.paused else "running",
                "virt_ms": virt_ms,
            }

    def progress(self) -> dict:
        return {
            "virt_ms": self._scheduler.virt_now_ms(),
            "total_ms": self._total_ms(),
            "paused": self._scheduler.paused,
            "speed": self._scheduler.speed,
        }

    def _total_ms(self) -> Optional[int]:
        started = self._manifest.get("started_at_recv_ms")
        ended = self._manifest.get("ended_at_recv_ms")
        if isinstance(started, int) and isinstance(ended, int):
            return ended - started
        return None

    async def _stop_video_for_pause(self) -> None:
        if self._video_task is not None:
            self._video_task.cancel()
            try:
                await self._video_task
            except (asyncio.CancelledError, Exception):
                pass
            self._video_task = None
        if self._video_pusher is not None:
            try:
                await self._video_pusher.aclose()
            except Exception:
                logger.exception("video pusher stop failed during pause/seek")
            self._video_pusher = None

    async def _restart_video_at_current_virt(
        self, *, at_virt_ms: Optional[int] = None,
    ) -> None:
        """Args
        ----
        at_virt_ms:
            Snapshot 的 virt 时刻（resume/seek 调用方在 await 链最早处取）。
            None 时回退到 ``self._scheduler.virt_now_ms()`` 当前值——仅给直接调用
            兼容；resume/seek 路径必须传 snapshot 避免 await 期间 virt 推进
            （review MAJOR）。
        """
        video_meta = self._manifest.get("video") or {}
        video_rel = video_meta.get("file")
        video_file = (self.flight_dir / video_rel) if video_rel else None
        video_exists = bool(video_file and video_file.exists())
        plan = plan_video_push(
            self._manifest, self._video_push_url, self._speed,
            video_exists, anchor_offset_ms=self._video_anchor_offset_ms,
        )
        if plan is None:
            return
        plan = dict(plan)
        # video_offset_in_flight_ms: how many ms into flight virt-time does the
        # video file's frame-0 correspond to.  Matches the same field plan_video_push
        # reads (video["started_at_recv_ms"] - manifest["started_at_recv_ms"]).
        started_at = self._manifest.get("started_at_recv_ms") or 0
        video_started_recv_ms = video_meta.get("started_at_recv_ms") or started_at
        video_offset_in_flight_ms = video_started_recv_ms - started_at
        seek_virt = (at_virt_ms if at_virt_ms is not None
                     else self._scheduler.virt_now_ms())
        # ss_seconds: which second within the video file to seek to so that
        # frame-0 of the pushed stream corresponds to the current seek target.
        plan["ss_seconds"] = max(0.0, (seek_virt - video_offset_in_flight_ms) / 1000.0)
        # wait_virt_ms: apply the same anchor_offset compensation as initial play.
        # Negative anchor_offset_ms means "launch ffmpeg earlier in virt-time to
        # compensate the RTMP→SRS→mpegts.js pipeline delay".  If the result is
        # below seek_virt, wait_until_virt returns immediately and ffmpeg launches
        # as fast as possible — the same behaviour as initial play when anchor
        # offset pushed the wait before play start.
        plan["wait_virt_ms"] = seek_virt + self._video_anchor_offset_ms
        # Defensive: cancel any video task that may have been assigned by a
        # concurrent path (pause / seek interleave) so we don't orphan ffmpeg.
        if self._video_task is not None:
            self._video_task.cancel()
            try:
                await self._video_task
            except (asyncio.CancelledError, Exception):
                pass
            self._video_task = None
        if self._video_pusher is not None:
            try:
                await self._video_pusher.aclose()
            except Exception:
                logger.exception("video pusher stop failed during restart")
            self._video_pusher = None

        self._video_task = asyncio.create_task(
            self._run_video_push(plan, video_file))

    async def _replay_topic_from_virt(
        self,
        topic_entry: dict,
        files: list[Path],
        started_at_ms: int,
        seek_virt_ms: int,
    ) -> None:
        """跟 _replay_topic 同逻辑，但 linear scan 跳过 seek 之前的 record。"""
        topic = topic_entry["topic"]
        target_recv_ts = started_at_ms + seek_virt_ms
        for record in JsonlIterator(files):
            recv_ts = record.get("recv_ts_ms")
            if not isinstance(recv_ts, int):
                continue
            if recv_ts < target_recv_ts:
                continue
            # Don't add _start_offset_ms: scheduler.set_virt() in seek() already
            # moved virt_zero to seek_virt_ms; record at offset T should wait T-S ms
            # from the current virt = S, which `wait_until_virt(T)` achieves.
            virt_target = recv_ts - started_at_ms
            await self._scheduler.wait_until_virt(virt_target)
            payload = record.get("payload", {})
            payload_bytes = json.dumps(
                payload, ensure_ascii=False, separators=(",", ":")
            ).encode("utf-8")
            try:
                await self._publisher.publish(topic, payload_bytes)
            except Exception:
                logger.exception("publish failed for topic {}", topic)

    async def _publish_dashboard_clear_marker(self) -> None:
        """补发一条合成 dock OSD 让 dashboard 清轨迹（详见 wait_until_done finally
        里的调用注释）。失败时只 log 不抛——这是 best-effort 的收尾动作，
        不能影响停止流程。manifest 没 dock_sn 时静默跳过。
        """
        dock_sn = self._manifest.get("dock_sn")
        if not dock_sn:
            return
        topic = f"thing/product/{dock_sn}/osd"
        payload = json.dumps(
            {"data": {"flighttask_step_code": 5}},
            separators=(",", ":"),
        ).encode("utf-8")
        try:
            await self._publisher.publish(topic, payload)
            logger.info(
                "published synthetic idle marker to {} (dashboard trail clear)",
                topic,
            )
        except Exception:
            logger.exception(
                "synthetic idle marker publish failed; dashboard 轨迹可能不会自动清",
            )
