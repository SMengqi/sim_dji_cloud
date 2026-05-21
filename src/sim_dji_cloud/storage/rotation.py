from pathlib import Path
from typing import Any
from .jsonl import JsonlWriter


class RotatingJsonlWriter:
    """带分卷的 JSONL writer。

    分卷文件名：`<base_path>.0001.jsonl` / `.0002.jsonl` ...
    超过 rotate_max_records 或 rotate_max_bytes 任一阈值即切下一卷。
    track 每卷的 first_ms / last_ms / count 供 manifest 使用。
    """

    def __init__(
        self,
        base_path: Path,
        rotate_max_records: int,
        rotate_max_bytes: int,
        flush_max_records: int = 1000,
    ):
        self.base_path = Path(base_path)
        self.rotate_max_records = rotate_max_records
        self.rotate_max_bytes = rotate_max_bytes
        self.flush_max_records = flush_max_records
        self._current_index = 0
        self._current_writer: JsonlWriter | None = None
        self._current_meta: dict[str, Any] = {}
        self._closed_files: list[dict[str, Any]] = []
        self._open_next_volume()

    def __enter__(self) -> "RotatingJsonlWriter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _open_next_volume(self) -> None:
        if self._current_writer is not None:
            self._finalize_current()
        self._current_index += 1
        name = f"{self.base_path.name}.{self._current_index:04d}.jsonl"
        path = self.base_path.parent / name
        self._current_writer = JsonlWriter(path, flush_max_records=self.flush_max_records)
        self._current_meta = {
            "name": name,
            "count": 0,
            "first_ms": None,
            "last_ms": None,
        }

    def _finalize_current(self) -> None:
        assert self._current_writer is not None
        self._current_writer.close()
        self._closed_files.append(dict(self._current_meta))

    def write(self, obj: dict[str, Any]) -> None:
        assert self._current_writer is not None
        # Lazy rotation: check threshold BEFORE writing so we don't leave
        # an empty trailing volume after close().
        if (
            self._current_writer.records_written >= self.rotate_max_records
            or self._current_writer.bytes_written >= self.rotate_max_bytes
        ):
            self._open_next_volume()
            assert self._current_writer is not None
        ts = obj.get("recv_ts_ms")
        if self._current_meta["first_ms"] is None and ts is not None:
            self._current_meta["first_ms"] = ts
        if ts is not None:
            self._current_meta["last_ms"] = ts
        self._current_writer.write(obj)
        self._current_meta["count"] += 1

    def flush(self) -> None:
        """把当前卷 writer 内部 buffer 推到磁盘 fd（不关闭、不切卷）。
        TopicWriteQueue 每个 flush_interval_ms 调一次，保证用户能实时看到
        文件增长，并把进程崩溃时的数据丢失上限限制到 flush_interval_ms。
        """
        if self._current_writer is not None:
            self._current_writer.flush()

    def close(self) -> None:
        if self._current_writer is not None:
            self._finalize_current()
            self._current_writer = None

    def files_metadata(self) -> list[dict[str, Any]]:
        return [dict(f) for f in self._closed_files]
