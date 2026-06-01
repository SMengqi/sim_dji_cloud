import json
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from loguru import logger

from sim_dji_cloud.player import Player
from sim_dji_cloud.selfcheck.comparator import compare_topic, TopicCompareResult
from sim_dji_cloud.selfcheck.report import write_reports
from sim_dji_cloud.storage.jsonl import JsonlWriter


@dataclass
class SelfCheckResult:
    overall: str
    topic_results: list[TopicCompareResult]
    report_dir: Path


class _InProcessLoopbackPublisher:
    """In-process publisher → 直接转发给 sink。

    payload bytes 不带 recv/dji 时间戳，Player 只发 record.payload。
    所以 SelfCheck 必须从 orig 取出元数据；recorder 用预填的元数据队列还原。
    """

    def __init__(self, sink: "_InProcessRecorder"):
        self._sink = sink

    async def connect(self) -> None:
        pass

    async def disconnect(self) -> None:
        pass

    async def publish(self, topic: str, payload: bytes, qos: int = 0) -> None:
        await self._sink.receive(topic, payload)


class _InProcessRecorder:
    """收消息直接写到 capture_dir/topics/<topic>.0001.jsonl。仅 SelfCheck 用。

    通过 prime_metadata 注入每个 topic 的 (recv_ts_ms, dji_ts_ms) 队列，
    每次 receive 按顺序消费一条元数据，确保和 orig 严格一一对应。
    """

    def __init__(self, capture_dir: Path):
        self.capture_dir = Path(capture_dir)
        (self.capture_dir / "topics").mkdir(parents=True, exist_ok=True)
        self._writers: dict[str, JsonlWriter] = {}
        self._metadata: dict[str, deque[tuple[Optional[int], Optional[int], str]]] = (
            defaultdict(deque)
        )

    def prime_metadata(
        self,
        topic: str,
        records: list[tuple[Optional[int], Optional[int], str]],
    ) -> None:
        self._metadata[topic].extend(records)

    async def receive(self, topic: str, payload: bytes) -> None:
        from sim_dji_cloud.recorder.topic_router import file_name_for_topic
        from sim_dji_cloud.utils.time_ms import now_ms

        if topic not in self._writers:
            name = file_name_for_topic(topic).replace(".jsonl", ".0001.jsonl")
            path = self.capture_dir / "topics" / name
            self._writers[topic] = JsonlWriter(path, flush_max_records=1)
        try:
            payload_obj = json.loads(payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            payload_obj = {}

        if self._metadata[topic]:
            recv_ts, dji_ts, direction = self._metadata[topic].popleft()
        else:
            recv_ts, dji_ts, direction = now_ms(), payload_obj.get("timestamp"), "up"

        self._writers[topic].write({
            "recv_ts_ms": recv_ts,
            "dji_ts_ms": dji_ts,
            "direction": direction,
            "topic": topic,
            "payload": payload_obj,
        })

    def close(self) -> None:
        for w in self._writers.values():
            w.close()


class SelfCheck:
    def __init__(
        self,
        flight_dir: Path,
        report_dir: Optional[Path] = None,
        tolerance_ms: int = 50,
    ):
        self.flight_dir = Path(flight_dir)
        self.report_dir = Path(report_dir) if report_dir else (
            self.flight_dir / "selfcheck" / time.strftime("%Y%m%d-%H%M%S")
        )
        self.tolerance_ms = tolerance_ms

    async def run_with_inprocess_loopback(self) -> SelfCheckResult:
        capture_dir = self.report_dir / "replay_capture"
        recorder = _InProcessRecorder(capture_dir)
        publisher = _InProcessLoopbackPublisher(recorder)

        manifest = json.loads((self.flight_dir / "manifest.json").read_text())

        # Prime per-topic metadata queues so recorder can reproduce recv/dji ts.
        for topic_entry in manifest.get("topics", []):
            topic = topic_entry["topic"]
            orig_files = [self.flight_dir / f["name"] for f in topic_entry.get("files", [])]
            records = self._collect_metadata(orig_files)
            recorder.prime_metadata(topic, records)

        player = Player(
            flight_dir=self.flight_dir,
            publisher=publisher,
            speed=100.0,
            # Loopback 模式不能让 Player 自己加一条"清 dashboard 轨迹"的合成
            # OSD：会被回环 Recorder 收掉，跟原录制对不上 → Comparator FAIL。
            publish_clear_marker_on_stop=False,
        )
        await player.start()
        await player.wait_until_done()
        recorder.close()

        results: list[TopicCompareResult] = []
        for topic_entry in manifest.get("topics", []):
            topic = topic_entry["topic"]
            device_sn = topic_entry.get("device_sn", "")
            orig_files = [self.flight_dir / f["name"] for f in topic_entry.get("files", [])]
            replay_name = self._replay_filename(topic)
            replay_path = capture_dir / "topics" / replay_name

            orig_path = self._merge_volumes_for_compare(orig_files, capture_dir, topic)
            result = compare_topic(
                orig_path, replay_path,
                tolerance_ms=self.tolerance_ms,
                topic_label=topic,
                device_sn=device_sn,
            )
            results.append(result)

        overall = "PASS" if all(r.status == "PASS" for r in results) else "FAIL"
        write_reports(
            self.report_dir, results, self.tolerance_ms,
            flight_dir_name=self.flight_dir.name,
        )
        return SelfCheckResult(
            overall=overall, topic_results=results, report_dir=self.report_dir,
        )

    @staticmethod
    def _replay_filename(topic: str) -> str:
        return topic.replace("/", "__") + ".0001.jsonl"

    @staticmethod
    def _collect_metadata(
        orig_files: list[Path],
    ) -> list[tuple[Optional[int], Optional[int], str]]:
        out: list[tuple[Optional[int], Optional[int], str]] = []
        for f in orig_files:
            if not f.exists():
                continue
            with f.open("r", encoding="utf-8") as inp:
                for line in inp:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    out.append((
                        rec.get("recv_ts_ms"),
                        rec.get("dji_ts_ms"),
                        rec.get("direction", "up"),
                    ))
        return out

    def _merge_volumes_for_compare(
        self, orig_files: list[Path], capture_dir: Path, topic: str,
    ) -> Path:
        merge_dir = capture_dir.parent / "_orig_merge"
        merge_dir.mkdir(parents=True, exist_ok=True)
        merged = merge_dir / self._replay_filename(topic)
        with merged.open("w", encoding="utf-8") as out:
            for f in orig_files:
                if not f.exists():
                    continue
                with f.open("r", encoding="utf-8") as inp:
                    for line in inp:
                        if line.strip():
                            out.write(line if line.endswith("\n") else line + "\n")
        return merged


import socket


def pick_free_port_in_range(lo: int, hi: int) -> int:
    """在 [lo, hi] 内找一个能 bind 的空闲端口；都占用时 RuntimeError。"""
    for port in range(lo, hi + 1):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s.bind(("127.0.0.1", port))
                return port
        except OSError:
            continue
    raise RuntimeError(f"no free port in range {lo}-{hi}")
