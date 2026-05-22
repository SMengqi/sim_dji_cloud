import asyncio
import time
from typing import Optional


class VirtTimeScheduler:
    """虚拟时钟：virt_ms = virt_zero_ms + (wall_now - play_start_wall) * speed * 1000。

    阶段二只支持启动时设置 speed/start_offset；Pause/Seek/SetSpeed 留阶段三。
    """

    def __init__(self):
        self._play_start_wall: Optional[float] = None
        self._virt_zero_ms: int = 0
        self._speed: float = 1.0

    def start(self, virt_zero_ms: int = 0, speed: float = 1.0) -> None:
        self._virt_zero_ms = virt_zero_ms
        self._speed = float(speed)
        self._play_start_wall = time.monotonic()

    def virt_now_ms(self) -> int:
        if self._play_start_wall is None:
            return self._virt_zero_ms
        elapsed_wall = time.monotonic() - self._play_start_wall
        return int(self._virt_zero_ms + elapsed_wall * 1000 * self._speed)

    async def wait_until_virt(self, target_ms: int) -> None:
        if self._play_start_wall is None:
            self.start()
        while True:
            now = self.virt_now_ms()
            if now >= target_ms:
                return
            virt_remaining_ms = target_ms - now
            wall_sleep_s = (virt_remaining_ms / 1000.0) / max(self._speed, 1e-6)
            # 单次 sleep 限上限 0.5s，方便阶段三接入 pause/seek 中断
            await asyncio.sleep(min(wall_sleep_s, 0.5))
