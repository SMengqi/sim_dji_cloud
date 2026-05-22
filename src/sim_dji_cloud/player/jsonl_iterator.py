import json
from pathlib import Path
from typing import Iterator


class JsonlIterator:
    """按给定顺序流式读多卷 jsonl，跳过空行；可在第一个文件指定起始字节偏移。"""

    def __init__(
        self,
        files: list[Path],
        start_offset_first_file: int = 0,
    ):
        self._files = [Path(f) for f in files]
        self._start_offset_first_file = start_offset_first_file

    def __iter__(self) -> Iterator[dict]:
        for idx, path in enumerate(self._files):
            if not path.exists():
                continue
            with path.open("rb") as fp:
                if idx == 0 and self._start_offset_first_file > 0:
                    fp.seek(self._start_offset_first_file)
                for raw in fp:
                    line = raw.strip()
                    if not line:
                        continue
                    yield json.loads(line)
