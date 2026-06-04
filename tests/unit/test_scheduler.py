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


def test_pause_freezes_virt_time():
    """pause 后 virt_now_ms 冻结在 pause 时刻；wall 继续走也不变。"""
    s = VirtTimeScheduler()
    s.start(virt_zero_ms=0, speed=1.0)
    time.sleep(0.1)
    before = s.virt_now_ms()
    s.pause()
    time.sleep(0.2)
    after = s.virt_now_ms()
    assert abs(after - before) < 5, f"paused virt drifted: {before}->{after}"


def test_resume_continues_virt():
    """pause -> wall sleep -> resume 后 virt 不"跳"——pause 期间的 wall 被吃掉。"""
    s = VirtTimeScheduler()
    s.start(virt_zero_ms=0, speed=1.0)
    time.sleep(0.1)
    paused_at = s.virt_now_ms()
    s.pause()
    time.sleep(0.2)
    s.resume()
    after_resume = s.virt_now_ms()
    assert abs(after_resume - paused_at) < 10, (
        f"resume jumped: paused_at={paused_at} after_resume={after_resume}"
    )


def test_pause_when_already_paused_is_noop():
    s = VirtTimeScheduler()
    s.start()
    time.sleep(0.05)
    s.pause()
    first_paused_virt = s.virt_now_ms()
    time.sleep(0.05)
    s.pause()
    time.sleep(0.05)
    second_paused_virt = s.virt_now_ms()
    assert first_paused_virt == second_paused_virt


def test_resume_when_not_paused_is_noop():
    s = VirtTimeScheduler()
    s.start()
    time.sleep(0.05)
    before = s.virt_now_ms()
    s.resume()
    time.sleep(0.05)
    after = s.virt_now_ms()
    assert after - before >= 40, f"resume changed clock: {before}->{after}"


def test_set_virt_jumps_forward():
    s = VirtTimeScheduler()
    s.start()
    time.sleep(0.05)
    s.set_virt(5000)
    after = s.virt_now_ms()
    assert 4990 <= after <= 5050, f"set_virt drift: {after}"


@pytest.mark.asyncio
async def test_wait_until_virt_blocks_on_pause():
    s = VirtTimeScheduler()
    s.start(virt_zero_ms=0, speed=10.0)
    s.pause()
    wait_task = asyncio.create_task(s.wait_until_virt(100))
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(asyncio.shield(wait_task), timeout=0.2)
    s.resume()
    await asyncio.wait_for(wait_task, timeout=1.0)


def test_set_virt_while_paused_returns_target_virt():
    """seek (set_virt) 在 paused 状态下：virt_now_ms 应返回 target，
    不应因 _paused_at_wall 没同步锚到新 wall_start 而漂负值。
    回归保护 code-reviewer 2026-06-03 发现的 bug。"""
    s = VirtTimeScheduler()
    s.start(virt_zero_ms=0, speed=1.0)
    time.sleep(0.05)
    s.pause()
    paused_virt_before = s.virt_now_ms()   # 比如 ~50
    time.sleep(0.05)                        # 让 wall 前进
    s.set_virt(5000)
    after = s.virt_now_ms()
    assert 4995 <= after <= 5005, (
        f"set_virt while paused should anchor virt to target; got {after}"
    )
    # 再等一会儿仍冻结在 5000（paused 还没 resume）
    time.sleep(0.05)
    assert s.virt_now_ms() == after, "paused virt drifted after set_virt"


def test_set_virt_while_paused_then_resume_continues_from_target():
    """seek paused 后 resume，virt 应从 target 继续前进，不跳。"""
    s = VirtTimeScheduler()
    s.start(virt_zero_ms=0, speed=1.0)
    s.pause()
    s.set_virt(2000)
    assert 1990 <= s.virt_now_ms() <= 2010
    time.sleep(0.05)        # paused, 不动
    s.resume()
    time.sleep(0.05)        # ~50ms 前进
    after = s.virt_now_ms()
    assert 2030 <= after <= 2080, (
        f"resume after set_virt-while-paused should continue from target; got {after}"
    )
