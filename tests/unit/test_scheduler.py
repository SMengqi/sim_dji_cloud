import asyncio
import time
import pytest
from sim_dji_cloud.player.scheduler import VirtTimeScheduler


@pytest.mark.asyncio
async def test_virt_time_advances_with_wall_time():
    s = VirtTimeScheduler()
    s.start(virt_zero_ms=0, speed=1.0)
    await asyncio.sleep(0.1)
    assert 90 <= s.virt_now_ms() <= 150


@pytest.mark.asyncio
async def test_double_speed_advances_twice_as_fast():
    s = VirtTimeScheduler()
    s.start(virt_zero_ms=0, speed=2.0)
    await asyncio.sleep(0.1)
    assert 180 <= s.virt_now_ms() <= 280


@pytest.mark.asyncio
async def test_wait_until_virt_sleeps_appropriately():
    s = VirtTimeScheduler()
    s.start(virt_zero_ms=0, speed=1.0)
    wall_before = time.monotonic()
    await s.wait_until_virt(200)
    elapsed_wall = (time.monotonic() - wall_before) * 1000
    assert 180 <= elapsed_wall <= 280


@pytest.mark.asyncio
async def test_wait_until_virt_returns_immediately_if_past():
    s = VirtTimeScheduler()
    s.start(virt_zero_ms=0, speed=1.0)
    await asyncio.sleep(0.05)
    wall_before = time.monotonic()
    await s.wait_until_virt(10)
    elapsed = (time.monotonic() - wall_before) * 1000
    assert elapsed < 20


@pytest.mark.asyncio
async def test_virt_origin_offset():
    s = VirtTimeScheduler()
    s.start(virt_zero_ms=5000, speed=1.0)
    assert 5000 <= s.virt_now_ms() <= 5100
