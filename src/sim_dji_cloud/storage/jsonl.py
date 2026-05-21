import json
import os
from pathlib import Path
from typing import Any, Iterator


class JsonlWriter:
    """Append-only JSONL writer，批量刷盘减少 syscall。"""

    def __init__(self, path: Path, flush_max_records: int = 1000):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fp = self.path.open("ab")
        self._buffer: list[bytes] = []
        self._flush_max_records = flush_max_records
        self.records_written = 0
        self.bytes_written = 0

    def write(self, obj: dict[str, Any]) -> None:
        line = (json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
        self._buffer.append(line)
        self.records_written += 1
        self.bytes_written += len(line)
        if len(self._buffer) >= self._flush_max_records:
            self.flush()

    def flush(self) -> None:
        if not self._buffer or self._fp.closed:
            return
        self._fp.write(b"".join(self._buffer))
        self._fp.flush()
        self._buffer.clear()

    def close(self) -> None:
        if self._fp.closed:
            return
        self.flush()
        os.fsync(self._fp.fileno())
        self._fp.close()

    def __enter__(self) -> "JsonlWriter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


class JsonlReader:
    """逐行读取 JSONL；跳过空行。"""

    def __init__(self, path: Path):
        self.path = Path(path)

    def __iter__(self) -> Iterator[dict[str, Any]]:
        if not self.path.exists():
            return
        with self.path.open("r", encoding="utf-8") as fp:
            for lineno, line in enumerate(fp, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as e:
                    raise json.JSONDecodeError(
                        f"{self.path}:{lineno}: {e.msg}", e.doc, e.pos
                    ) from None
