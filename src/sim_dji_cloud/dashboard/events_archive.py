"""dashboard 事件归档：把 MQTT events / drc-down / services 流暂存内存，
供 timeline 视图查询、CSV 导出。

设计要点（详见 design.md）：
- 两 deque (events / controls) 按任务清；soft cap 5000 LRU 兜底
- session_started_at_ms 首条 append 时点亮；reset 时清
- query 返回 list 拷贝（断开 deque 引用，防 race）
"""
from __future__ import annotations

import json
import threading
from collections import deque
from pathlib import Path
from typing import Any, Optional

from loguru import logger

from sim_dji_cloud.player.jsonl_iterator import JsonlIterator

_EVENT_SUFFIXES = {"events"}
_CONTROL_SUFFIXES = {"drc/down", "services"}


def _topic_suffix(topic: str) -> str:
    parts = topic.split("/")
    return "/".join(parts[3:]) if len(parts) >= 4 else ""


class EventsArchive:
    """内存 events / controls 归档。线程安全（threading.Lock 保护 deque 操作 + session 字段），
    支持 asyncio loop（MqttSubscriber.append）与 FastAPI sync handler threadpool（API query）
    跨线程访问。"""

    def __init__(self, soft_cap: int = 5000):
        self._events:   deque[dict[str, Any]] = deque(maxlen=soft_cap)
        self._controls: deque[dict[str, Any]] = deque(maxlen=soft_cap)
        self._session_started_at_ms: Optional[int] = None
        # 跨线程保护：MqttSubscriber 在 asyncio loop 上 append，
        # FastAPI sync 路由 handler 在 threadpool 上 query —— 需要锁
        # 防止 deque 迭代时另一线程 append 改变长度。
        self._lock = threading.Lock()

    def append(self, topic: str, payload: Any, recv_ts_ms: int) -> None:
        if not isinstance(payload, dict):
            logger.warning(
                "EventsArchive.append skipped: payload not dict, topic={}", topic,
            )
            return
        suffix = _topic_suffix(topic)
        if suffix in _EVENT_SUFFIXES:
            kind, bucket = "event", self._events
        elif suffix in _CONTROL_SUFFIXES:
            kind, bucket = "control", self._controls
        else:
            return
        entry = {
            "kind": kind,
            "recv_ts_ms": recv_ts_ms,
            "topic": topic,
            "method": payload.get("method"),
            "payload": payload,
        }
        with self._lock:
            bucket.append(entry)
            if self._session_started_at_ms is None:
                self._session_started_at_ms = recv_ts_ms

    def reset(self) -> None:
        with self._lock:
            self._events.clear()
            self._controls.clear()
            self._session_started_at_ms = None

    def query(
        self,
        *,
        kinds: tuple[str, ...] = ("event", "control"),
        since_ms: Optional[int] = None,
        until_ms: Optional[int] = None,
        limit: int = 5000,
    ) -> tuple[list[dict[str, Any]], bool]:
        """返回 (entries, truncated)。entries 是新 list，断开 deque 引用。"""
        kinds_set = set(kinds)
        # 只在拷贝 deque 时锁；filter/sort/limit 操作在 list 上、纯本地、无并发。
        with self._lock:
            if "event" in kinds_set:
                events_copy = list(self._events)
            else:
                events_copy = []
            if "control" in kinds_set:
                controls_copy = list(self._controls)
            else:
                controls_copy = []
        merged = events_copy + controls_copy
        if since_ms is not None:
            merged = [e for e in merged if e["recv_ts_ms"] >= since_ms]
        if until_ms is not None:
            merged = [e for e in merged if e["recv_ts_ms"] <= until_ms]
        merged.sort(key=lambda e: e["recv_ts_ms"])
        truncated = len(merged) > limit
        if truncated:
            merged = merged[-limit:]
        return merged, truncated

    @property
    def session_started_at_ms(self) -> Optional[int]:
        return self._session_started_at_ms


def read_archive_from_flight_dir(
    flight_dir: Path,
) -> tuple["EventsArchive", Optional[int]]:
    """读 flight_dir 下的 manifest.json + 对应 events/drc-down/services jsonl，
    返回 (archive, session_started_at_ms_from_manifest)。

    OSD jsonl 即使存在也被跳过；这是 events timeline，只关心事件 + 控制。
    Archive 用 soft_cap=10**9（即不卡）：飞行就那么长，全量加载；
    查询层 (REST /api/timeline) 自己有 limit 参数控返回大小。

    安全：manifest 里 file["name"] 经 resolve() 后必须仍在 flight_dir 下，
    否则跳过 + 警告（防 ../../etc/passwd 类型的 path traversal）。

    JSONL 容错：JsonlIterator 不捕获 json.JSONDecodeError（直接 yield
    json.loads(line)），所以每个 topic 的迭代包在 try/except 里；单条坏行
    导致该 topic 提前结束，已加载部分保留，其余 topic 正常继续。

    Raises:
        FileNotFoundError: flight_dir 不存在或缺 manifest.json
        ValueError: manifest.json 无法解析
    """
    flight_dir = Path(flight_dir)
    manifest_path = flight_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"manifest.json not found under {flight_dir}")

    try:
        manifest = json.loads(manifest_path.read_text())
    except json.JSONDecodeError as e:
        raise ValueError(f"manifest.json corrupt: {e}") from e

    session = manifest.get("started_at_recv_ms")
    # 离线模式不卡内存上限（飞行就那么长全量加载）；limit 由查询层控
    archive = EventsArchive(soft_cap=10**9)

    # Resolve once outside the loop so each file check is O(1) string prefix.
    flight_dir_resolved = flight_dir.resolve()

    for topic_entry in manifest.get("topics", []):
        topic = topic_entry.get("topic", "")
        suffix = _topic_suffix(topic)
        if suffix not in _EVENT_SUFFIXES and suffix not in _CONTROL_SUFFIXES:
            continue
        files = []
        for file_entry in topic_entry.get("files", []):
            file_path = (flight_dir / file_entry["name"]).resolve()
            try:
                file_path.relative_to(flight_dir_resolved)
            except ValueError:
                logger.warning(
                    "read_archive_from_flight_dir: file path escapes flight_dir, "
                    "skipping {} (resolved to {})",
                    file_entry["name"], file_path,
                )
                continue
            if not file_path.exists():
                logger.warning(
                    "read_archive_from_flight_dir: file missing on disk, skipping {}",
                    file_path,
                )
                continue
            files.append(file_path)
        if not files:
            continue
        try:
            for record in JsonlIterator(files):
                recv_ts = record.get("recv_ts_ms")
                payload = record.get("payload")
                if not isinstance(recv_ts, int):
                    continue
                archive.append(topic, payload, recv_ts)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(
                "read_archive_from_flight_dir: JSONL iteration aborted on topic {}: {}",
                topic, e,
            )
            # Don't re-raise; continue with the data we already loaded
    return archive, session
