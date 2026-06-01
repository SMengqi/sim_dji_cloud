from sim_dji_cloud.player.video_pusher import plan_video_push

M = {"started_at_recv_ms": 1000, "video": {"started_at_recv_ms": 4000}}


def test_plan_ok():
    assert plan_video_push(M, "rtmp://srs/live/x", 1.0, True) == {
        "wait_virt_ms": 3000, "ss_seconds": 0.0}


def test_no_url():
    assert plan_video_push(M, None, 1.0, True) is None
    assert plan_video_push(M, "", 1.0, True) is None


def test_speed_not_1():
    assert plan_video_push(M, "rtmp://srs/live/x", 2.0, True) is None


def test_no_video():
    assert plan_video_push({"started_at_recv_ms": 0, "video": None},
                           "rtmp://srs/live/x", 1.0, True) is None
    assert plan_video_push({"started_at_recv_ms": 0},
                           "rtmp://srs/live/x", 1.0, True) is None


def test_file_missing():
    assert plan_video_push(M, "rtmp://srs/live/x", 1.0, False) is None


def test_negative_offset_floored():
    m = {"started_at_recv_ms": 5000, "video": {"started_at_recv_ms": 4000}}
    assert plan_video_push(m, "rtmp://srs/live/x", 1.0, True)["wait_virt_ms"] == 0


# ---------------------------------------------------------------------------
# anchor_offset_ms：补偿回放端 RTMP push → SRS GOP cache → player buffer
# 这段管道延迟。负数 = 让视频比 first-frame 时刻早推（追上 trajectory）。
# ---------------------------------------------------------------------------

def test_anchor_offset_negative_advances_push():
    """负 offset 把视频推流提前。
    base wait = 4000 - 1000 = 3000ms；offset = -1500 → wait = 1500。"""
    assert plan_video_push(
        M, "rtmp://srs/live/x", 1.0, True, anchor_offset_ms=-1500,
    )["wait_virt_ms"] == 1500


def test_anchor_offset_positive_delays_push():
    """正 offset 把视频推流推后。
    base wait = 3000；offset = +500 → wait = 3500。"""
    assert plan_video_push(
        M, "rtmp://srs/live/x", 1.0, True, anchor_offset_ms=500,
    )["wait_virt_ms"] == 3500


def test_anchor_offset_floored_at_zero():
    """offset 大到把 wait 推到负值 → max(0, ...) 兜底到 0。
    base wait = 3000；offset = -5000 → 理论 -2000 → 落地 0。"""
    assert plan_video_push(
        M, "rtmp://srs/live/x", 1.0, True, anchor_offset_ms=-5000,
    )["wait_virt_ms"] == 0


def test_anchor_offset_default_unchanged():
    """不传 anchor_offset_ms 时维持原行为（向后兼容）。"""
    assert plan_video_push(M, "rtmp://srs/live/x", 1.0, True) == {
        "wait_virt_ms": 3000, "ss_seconds": 0.0,
    }
