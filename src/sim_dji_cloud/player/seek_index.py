import json
from bisect import bisect_right
from pathlib import Path


class SeekIndex:
    """稀疏 (recv_ts_ms, byte_offset) 锚点索引，支持按时间二分查找。

    每 anchor_every 条记录在 jsonl 中保存一个锚点；seek 时找到 <= target_ms
    的最大锚点，返回其字节偏移，Player 从该偏移开始顺序前进至目标时间。
    """

    def __init__(self, anchors: list[tuple[int, int]]):
        self._anchors = anchors
        self._ts_list = [a[0] for a in anchors]

    @classmethod
    def from_jsonl(cls, path: Path, anchor_every: int = 100) -> "SeekIndex":
        anchors: list[tuple[int, int]] = []
        p = Path(path)
        if not p.exists() or p.stat().st_size == 0:
            return cls(anchors)
        i = 0
        with p.open("rb") as f:
            while True:
                line_start = f.tell()
                line = f.readline()
                if not line:
                    break
                stripped = line.strip()
                if not stripped:
                    continue
                if i % anchor_every == 0:
                    try:
                        rec = json.loads(stripped)
                        ts = rec.get("recv_ts_ms")
                        if isinstance(ts, int):
                            anchors.append((ts, line_start))
                    except json.JSONDecodeError:
                        pass
                i += 1
        return cls(anchors)

    def anchors(self) -> list[tuple[int, int]]:
        return list(self._anchors)

    def seek_offset_for_time(self, target_ms: int) -> int:
        if not self._anchors:
            return 0
        i = bisect_right(self._ts_list, target_ms)
        if i == 0:
            return 0
        return self._anchors[i - 1][1]
