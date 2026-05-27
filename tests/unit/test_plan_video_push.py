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
