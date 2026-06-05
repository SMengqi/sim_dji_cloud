"""崩溃安全的文件覆盖写入。

write_text 直接覆盖会在中途崩溃（kill -9 / 断电）时留下截断文件。
manifest.json 一旦被截断，所有下游工具（inspect/list/play/selfcheck/repair）
都读不出来。这里走 tmp + fsync + os.replace 的 POSIX 原子模式，
要么旧版本完好、要么新版本完整，永远不会半截。
"""
from __future__ import annotations

import os
from pathlib import Path


def atomic_write_text(path: Path, content: str, encoding: str = "utf-8") -> None:
    """覆盖式原子写入。同一文件系统下 os.replace 是原子的。"""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    data = content.encode(encoding)
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        os.write(fd, data)
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp, path)
