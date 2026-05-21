import time

def now_ms() -> int:
    """Wall-clock 毫秒整数（受 NTP 调整影响）。落盘时间戳用这个。"""
    return int(time.time() * 1000)

def monotonic_ms() -> int:
    """单调毫秒整数（不受 NTP 影响）。调度器/超时判定用这个。"""
    return int(time.monotonic() * 1000)
