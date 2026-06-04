import asyncio
import time
from typing import Optional


class VirtTimeScheduler:
    """虚拟时钟：virt_ms = virt_zero_ms + (wall_now - play_start_wall) * speed * 1000。

    阶段三加 pause / resume / set_virt 三个方法，配合 paused_event 让 wait_until_virt
    可被 asyncio 层卡住。pause 期间 virt 冻结；resume 时把暂停期间的 wall 时长
    叠加到 play_start_wall 上，保证 virt 不"跳"。set_virt 重置 zero + 锚定新 wall
    起点。
    """

    def __init__(self):
        self._play_start_wall: Optional[float] = None
        self._virt_zero_ms: int = 0
        self._speed: float = 1.0
        self._paused: bool = False
        self._paused_at_wall: Optional[float] = None
        self._paused_event = asyncio.Event()  # set() = running; clear() = paused
        self._paused_event.set()

    def start(self, virt_zero_ms: int = 0, speed: float = 1.0) -> None:
        self._virt_zero_ms = virt_zero_ms
        self._speed = float(speed)
        self._play_start_wall = time.monotonic()
        self._paused = False
        self._paused_at_wall = None
        self._paused_event.set()

    def pause(self) -> None:
        if self._paused:
            return
        self._paused = True
        self._paused_at_wall = time.monotonic()
        self._paused_event.clear()

    def resume(self) -> None:
        if not self._paused:
            return
        if self._paused_at_wall is None:
            # Unreachable in practice; treat as zero-duration pause.
            paused_duration = 0.0
        else:
            paused_duration = time.monotonic() - self._paused_at_wall
        if self._play_start_wall is not None:
            self._play_start_wall += paused_duration
        self._paused = False
        self._paused_at_wall = None
        self._paused_event.set()

    def set_virt(self, virt_ms: int) -> None:
        """跳到任意 virt_ms。重置 zero + 锚定 wall_start = monotonic()。
        保持 paused 状态不变（seek 时暂停的播放仍暂停）。
        若 paused，同步把 _paused_at_wall 锚到新 wall_start，
        确保 virt_now_ms 在 paused 下也返回 virt_ms（而非过去的负 elapsed 漂移）。
        """
        self._virt_zero_ms = int(virt_ms)
        self._play_start_wall = time.monotonic()
        if self._paused:
            self._paused_at_wall = self._play_start_wall

    def virt_now_ms(self) -> int:
        if self._play_start_wall is None:
            return self._virt_zero_ms
        if self._paused:
            if self._paused_at_wall is None:
                # Unreachable in practice; treat as zero elapsed while paused.
                elapsed = 0.0
            else:
                elapsed = self._paused_at_wall - self._play_start_wall
        else:
            elapsed = time.monotonic() - self._play_start_wall
        return int(self._virt_zero_ms + elapsed * 1000 * self._speed)

    async def wait_until_virt(self, target_ms: int) -> None:
        if self._play_start_wall is None:
            self.start()
        while True:
            await self._paused_event.wait()
            now = self.virt_now_ms()
            if now >= target_ms:
                return
            virt_remaining_ms = target_ms - now
            wall_sleep_s = (virt_remaining_ms / 1000.0) / max(self._speed, 1e-6)
            await asyncio.sleep(min(wall_sleep_s, 0.5))

    @property
    def paused(self) -> bool:
        return self._paused

    @property
    def speed(self) -> float:
        return self._speed
