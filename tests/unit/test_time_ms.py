import time
from sim_dji_cloud.utils.time_ms import now_ms, monotonic_ms

def test_now_ms_returns_int_close_to_wall_clock():
    t = now_ms()
    assert isinstance(t, int)
    assert abs(t - int(time.time() * 1000)) < 100

def test_monotonic_ms_is_monotonic():
    a = monotonic_ms()
    b = monotonic_ms()
    assert b >= a
    assert isinstance(a, int)
