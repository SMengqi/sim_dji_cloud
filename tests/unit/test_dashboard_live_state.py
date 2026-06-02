import pytest

from sim_dji_cloud.dashboard.live_state import LiveState


def test_initial_state_is_empty():
    s = LiveState(events_ring_size=20)
    snap = s.snapshot()
    assert snap["dock"] == {}
    assert snap["drone"] == {}
    assert snap["events"] == []
    assert snap["topic_counts"] == {}


def test_update_dock_osd_extracts_key_fields():
    s = LiveState()
    s.update("thing/product/SN_DOCK/osd", {
        "data": {
            "mode_code": 4,
            "cover_state": 1,
            "drone_in_dock": 0,
            "flighttask_step_code": 1,
            "drc_state": 2,
            "environment_temperature": 26.4,
            "temperature": 21.7,
            "humidity": 96,
            "wind_speed": 0,
            "rainfall": 0,
            "network_state": {"type": 2, "quality": 0, "rate": 239},
            "latitude": 29.92,
            "longitude": 121.66,
            "height": 40.4,
            "sub_device": {"device_sn": "SN_DRONE"},
        },
        "timestamp": 1779346053697,
    }, recv_ts_ms=1779346054000)

    snap = s.snapshot()
    assert snap["dock"]["mode_code"] == 4
    assert snap["dock"]["cover_state"] == 1
    assert snap["dock"]["drone_in_dock"] == 0
    assert snap["dock"]["flighttask_step_code"] == 1
    assert snap["dock"]["drc_state"] == 2
    assert snap["dock"]["environment_temperature"] == 26.4
    assert snap["dock"]["temperature"] == 21.7
    assert snap["dock"]["humidity"] == 96
    assert snap["dock"]["wind_speed"] == 0
    assert snap["dock"]["rainfall"] == 0
    assert snap["dock"]["network_quality"] == 0
    assert snap["dock"]["latitude"] == 29.92
    assert snap["dock"]["longitude"] == 121.66
    assert snap["dock"]["paired_drone_sn"] == "SN_DRONE"
    assert snap["topic_counts"]["thing/product/SN_DOCK/osd"] == 1


def test_update_drone_osd_extracts_key_fields():
    s = LiveState()
    s.update("thing/product/SN_DRONE/osd", {
        "data": {
            "mode_code": 5,
            "battery": {"capacity_percent": 59},
            "attitude_head": 87.5,
            "attitude_pitch": 1.6,
            "attitude_roll": -6.5,
            "horizontal_speed": 1.16,
            "vertical_speed": 0,
            "height": 41.93,
            "home_distance": 1.81,
            "latitude": 29.9236,
            "longitude": 121.6634,
            "position_state": {"gps_number": 32, "rtk_number": 34, "is_fixed": 2},
            "wind_speed": 0,
        },
    }, recv_ts_ms=1779346055000)

    snap = s.snapshot()
    assert snap["drone"]["mode_code"] == 5
    assert snap["drone"]["battery_pct"] == 59
    assert snap["drone"]["attitude_head"] == 87.5
    assert snap["drone"]["horizontal_speed"] == 1.16
    assert snap["drone"]["height"] == 41.93
    assert snap["drone"]["home_distance"] == 1.81
    assert snap["drone"]["latitude"] == 29.9236
    assert snap["drone"]["longitude"] == 121.6634
    assert snap["drone"]["gps_satellites"] == 32
    assert snap["drone"]["rtk_satellites"] == 34


def test_drone_trail_accumulates_positions():
    s = LiveState()
    for i in range(5):
        s.update("thing/product/SN_DRONE/osd", {
            "data": {"mode_code": 5, "latitude": 30.0 + i * 0.001, "longitude": 121.0 + i * 0.001,
                     "battery": {"capacity_percent": 50}},
        }, recv_ts_ms=1000 + i * 1000)

    snap = s.snapshot()
    assert len(snap["drone_trail"]) == 5
    assert snap["drone_trail"][0] == [30.0, 121.0]
    assert snap["drone_trail"][-1] == [30.004, 121.004]


def test_drone_trail_caps_at_max():
    s = LiveState(trail_max=10)
    for i in range(15):
        s.update("thing/product/SN_DRONE/osd", {
            "data": {"latitude": 30.0 + i * 0.001, "longitude": 121.0,
                     "battery": {"capacity_percent": 50}},
        }, recv_ts_ms=1000 + i * 1000)

    snap = s.snapshot()
    assert len(snap["drone_trail"]) == 10
    assert snap["drone_trail"][-1] == [30.014, 121.0]


def test_events_ring_buffer():
    s = LiveState(events_ring_size=3)
    for i in range(5):
        s.update("thing/product/SN_DOCK/events", {
            "method": f"event_{i}",
            "data": {"flight_id": "T-X"},
        }, recv_ts_ms=1000 + i * 100)

    snap = s.snapshot()
    assert len(snap["events"]) == 3
    assert snap["events"][0]["method"] == "event_2"
    assert snap["events"][-1]["method"] == "event_4"


def test_events_keep_full_payload_for_modal():
    """events 也要保留完整 payload，前端 modal 能展开看 + 复制。"""
    s = LiveState()
    full = {"method": "device_online", "data": {"flight_id": "T-Y", "extra": [1, 2, 3]}}
    s.update("thing/product/SN_DOCK/events", full, recv_ts_ms=1234)
    snap = s.snapshot()
    assert snap["events"][0]["payload"] == full


def test_topic_counts_accumulate():
    s = LiveState()
    for _ in range(7):
        s.update("thing/product/SN_DOCK/osd",
                 {"data": {"sub_device": {"device_sn": "SN_DRONE"}}}, recv_ts_ms=1000)
    for _ in range(3):
        s.update("thing/product/SN_DRONE/osd", {"data": {}}, recv_ts_ms=1000)
    snap = s.snapshot()
    assert snap["topic_counts"]["thing/product/SN_DOCK/osd"] == 7
    assert snap["topic_counts"]["thing/product/SN_DRONE/osd"] == 3


def test_unknown_topic_only_counted_not_decoded():
    """非 osd/events/drc/services 的 topic 也算计数，但不解码字段。"""
    s = LiveState()
    s.update("thing/product/SN_DOCK/requests", {"method": "foo", "data": {}},
             recv_ts_ms=1000)
    snap = s.snapshot()
    assert snap["topic_counts"]["thing/product/SN_DOCK/requests"] == 1
    assert snap["dock"] == {}
    assert snap["controls"] == []


def test_partial_dock_osd_preserves_previous_fields():
    """DJI 部分 OSD 报文不带某些字段（如 flighttask_step_code 只在变化时上报）。
    LiveState 应保留上次值，而不是把字段清空成 None —— 否则前端会闪烁出 null。"""
    s = LiveState()
    s.update("thing/product/SN_DOCK/osd", {
        "data": {
            "mode_code": 4,
            "flighttask_step_code": 1,
            "drc_state": 2,
            "drone_in_dock": 0,
            "cover_state": 1,
            "environment_temperature": 26.4,
            "temperature": 21.5,
            "humidity": 96,
            "rainfall": 0,
            "sub_device": {"device_sn": "SN_DRONE"},
        },
    }, recv_ts_ms=1000)

    s.update("thing/product/SN_DOCK/osd", {
        "data": {
            "mode_code": 4,
            "drone_in_dock": 0,
            "cover_state": 1,
            "environment_temperature": 26.5,
            "humidity": 96,
        },
    }, recv_ts_ms=2000)

    snap = s.snapshot()
    dock = snap["dock"]
    assert dock["mode_code"] == 4
    assert dock["flighttask_step_code"] == 1, "应保留上次值而非清空成 None"
    assert dock["drc_state"] == 2, "应保留上次值而非清空成 None"
    assert dock["rainfall"] == 0, "应保留上次值而非清空成 None"
    assert dock["temperature"] == 21.5, "舱内温度未上报时应保留上次值"
    assert dock["paired_drone_sn"] == "SN_DRONE", "sub_device 缺失时应保留"
    assert dock["environment_temperature"] == 26.5, "新值应覆盖"
    assert dock["last_recv_ts_ms"] == 2000


def test_partial_drone_osd_preserves_previous_fields():
    """飞行器 OSD 部分字段缺失时同样应合并保留。"""
    s = LiveState()
    s.update("thing/product/SN_DRONE/osd", {
        "data": {
            "mode_code": 2,
            "battery": {"capacity_percent": 80},
            "attitude_head": 90.0,
            "latitude": 30.0, "longitude": 121.0,
            "horizontal_speed": 1.5,
            "position_state": {"gps_number": 12, "rtk_number": 24},
        },
    }, recv_ts_ms=1000)

    s.update("thing/product/SN_DRONE/osd", {
        "data": {
            "mode_code": 2,
            "latitude": 30.001, "longitude": 121.001,
            "horizontal_speed": 1.6,
        },
    }, recv_ts_ms=2000)

    snap = s.snapshot()
    drone = snap["drone"]
    assert drone["battery_pct"] == 80, "battery 字段未上报时应保留"
    assert drone["attitude_head"] == 90.0, "attitude 字段未上报时应保留"
    assert drone["gps_satellites"] == 12, "position_state 未上报时应保留"
    assert drone["latitude"] == 30.001, "新值应覆盖"
    assert drone["horizontal_speed"] == 1.6


def test_snapshot_returns_copy():
    """snapshot 应返回深拷贝。"""
    s = LiveState()
    s.update("thing/product/SN_DRONE/osd",
             {"data": {"latitude": 1.0, "longitude": 2.0, "battery": {"capacity_percent": 50}}},
             recv_ts_ms=1000)
    snap1 = s.snapshot()
    snap1["drone"]["mode_code"] = 999
    snap1["drone_trail"].append([0, 0])
    snap2 = s.snapshot()
    assert snap2["drone"].get("mode_code") != 999
    assert len(snap2["drone_trail"]) == 1


# ---------------------------------------------------------------------------
# 飞行任务空闲时清空轨迹 + 后续不再累积
# ---------------------------------------------------------------------------

def test_trail_cleared_when_dock_reports_task_idle():
    """dock OSD 报 flighttask_step_code=5（任务空闲）时，已累积的轨迹必须被清掉。"""
    s = LiveState()
    # 飞行中：dock 报 step=1，drone 喂位置，轨迹累积。
    s.update("thing/product/SN_DOCK/osd", {
        "data": {"flighttask_step_code": 1, "sub_device": {"device_sn": "SN_DRONE"}},
    }, recv_ts_ms=1000)
    for i in range(5):
        s.update("thing/product/SN_DRONE/osd", {
            "data": {"latitude": 30.0 + i * 0.001, "longitude": 121.0,
                     "battery": {"capacity_percent": 50}},
        }, recv_ts_ms=1500 + i * 200)
    assert len(s.snapshot()["drone_trail"]) == 5

    # 任务结束：dock 报 step=5 → 轨迹立即清空。
    s.update("thing/product/SN_DOCK/osd", {
        "data": {"flighttask_step_code": 5, "sub_device": {"device_sn": "SN_DRONE"}},
    }, recv_ts_ms=2500)
    assert s.snapshot()["drone_trail"] == []


def test_trail_does_not_accumulate_while_dock_idle():
    """dock 是 idle 时，后续 drone OSD 不应再把位置追加到轨迹里
    （否则 drone 落地后报的 dock 位置会被反复堆成一团）。"""
    s = LiveState()
    # 先把 dock 钉成 idle。
    s.update("thing/product/SN_DOCK/osd", {
        "data": {"flighttask_step_code": 5, "sub_device": {"device_sn": "SN_DRONE"}},
    }, recv_ts_ms=1000)
    # 然后 drone 发若干 OSD（带经纬度）。
    for i in range(5):
        s.update("thing/product/SN_DRONE/osd", {
            "data": {"latitude": 30.0, "longitude": 121.0,
                     "battery": {"capacity_percent": 50}},
        }, recv_ts_ms=1500 + i * 200)
    assert s.snapshot()["drone_trail"] == [], "idle 时不能再往轨迹里追加"


def test_trail_resumes_when_dock_returns_to_recording():
    """idle 之后 dock 回到 step=1，drone OSD 应该重新开始累积。"""
    s = LiveState()
    s.update("thing/product/SN_DOCK/osd", {
        "data": {"flighttask_step_code": 5, "sub_device": {"device_sn": "SN_DRONE"}},
    }, recv_ts_ms=1000)
    s.update("thing/product/SN_DRONE/osd", {
        "data": {"latitude": 30.0, "longitude": 121.0, "battery": {"capacity_percent": 50}},
    }, recv_ts_ms=1100)
    assert s.snapshot()["drone_trail"] == []

    # 新任务开始：dock 转回 step=1。
    s.update("thing/product/SN_DOCK/osd", {
        "data": {"flighttask_step_code": 1, "sub_device": {"device_sn": "SN_DRONE"}},
    }, recv_ts_ms=2000)
    for i in range(3):
        s.update("thing/product/SN_DRONE/osd", {
            "data": {"latitude": 31.0 + i * 0.001, "longitude": 122.0,
                     "battery": {"capacity_percent": 50}},
        }, recv_ts_ms=2100 + i * 200)
    trail = s.snapshot()["drone_trail"]
    assert len(trail) == 3
    assert trail[0] == [31.0, 122.0]


def test_trail_permissive_before_first_dock_osd():
    """开机瞬间 dock OSD 还没到，但 drone OSD 已经到了——这种情况下
    宽容累积（用户在飞行中刷新页面时不应丢点）。"""
    s = LiveState()
    for i in range(3):
        s.update("thing/product/SN_DRONE/osd", {
            "data": {"latitude": 30.0 + i * 0.001, "longitude": 121.0,
                     "battery": {"capacity_percent": 50}},
        }, recv_ts_ms=1000 + i * 200)
    assert len(s.snapshot()["drone_trail"]) == 3


def test_trail_unchanged_when_dock_osd_omits_step_code():
    """DJI 只在状态变化时下发 flighttask_step_code，缺该字段的 OSD 不应触发清空。"""
    s = LiveState()
    s.update("thing/product/SN_DOCK/osd", {
        "data": {"flighttask_step_code": 1, "sub_device": {"device_sn": "SN_DRONE"}},
    }, recv_ts_ms=1000)
    s.update("thing/product/SN_DRONE/osd", {
        "data": {"latitude": 30.0, "longitude": 121.0, "battery": {"capacity_percent": 50}},
    }, recv_ts_ms=1100)
    # 后续 dock OSD 不带 flighttask_step_code（DJI sticky 行为）。
    s.update("thing/product/SN_DOCK/osd", {
        "data": {"temperature": 28.0, "sub_device": {"device_sn": "SN_DRONE"}},
    }, recv_ts_ms=1200)
    assert s.snapshot()["drone_trail"] == [[30.0, 121.0]], (
        "OSD 没带 flighttask_step_code 时应沿用上次状态，不触发清空"
    )


# ---------------------------------------------------------------------------
# 相机变焦倍数 + 云台角
# ---------------------------------------------------------------------------

# 真实 DJI Dock 3 OSD 里 cameras[] 和 data.<payload_index> 子对象的字段子集；
# 真实报文很大，这里只列我们关心的几个字段。
_DRONE_OSD_WITH_CAMERA = {
    "data": {
        "latitude": 30.0, "longitude": 121.0,
        "battery": {"capacity_percent": 50},
        "99-0-0": {
            "gimbal_pitch": -21.0,
            "gimbal_yaw": 68.4,
            "gimbal_roll": 0.0,
            "zoom_factor": 2.27,        # 单镜头几何倍率，我们不取这个
        },
        "cameras": [
            {
                "payload_index": "99-0-0",
                "zoom_factor": 4.06,    # 综合/活动倍率，我们取这个
            },
        ],
    },
}


def test_drone_zoom_factor_from_cameras_array_not_payload_subobject():
    """变焦倍数明确取 data.cameras[0].zoom_factor，
    不取 data.<payload_index>.zoom_factor（两者数值不同，用户指定取前者）。"""
    s = LiveState()
    s.update("thing/product/SN_DRONE/osd", _DRONE_OSD_WITH_CAMERA, recv_ts_ms=1000)
    drone = s.snapshot()["drone"]
    assert drone["zoom_factor"] == 4.06, (
        "zoom_factor 必须来自 cameras[0].zoom_factor (4.06)，"
        "不是 data.99-0-0.zoom_factor (2.27)"
    )


def test_drone_gimbal_pitch_and_yaw_extracted_via_payload_index():
    """云台角放在 data.<payload_index> 子对象里——
    通过 cameras[0].payload_index 反查到那个子对象，取 gimbal_pitch / gimbal_yaw。"""
    s = LiveState()
    s.update("thing/product/SN_DRONE/osd", _DRONE_OSD_WITH_CAMERA, recv_ts_ms=1000)
    drone = s.snapshot()["drone"]
    assert drone["gimbal_pitch"] == -21.0
    assert drone["gimbal_yaw"] == 68.4


def test_drone_zoom_gimbal_absent_when_no_cameras_field():
    """OSD 没 cameras 字段时（部分上报或老协议），zoom/gimbal 字段不出现，
    不影响其他字段写入。"""
    s = LiveState()
    s.update("thing/product/SN_DRONE/osd", {
        "data": {"latitude": 30.0, "longitude": 121.0, "battery": {"capacity_percent": 50}},
    }, recv_ts_ms=1000)
    drone = s.snapshot()["drone"]
    assert "zoom_factor" not in drone
    assert "gimbal_pitch" not in drone
    assert "gimbal_yaw" not in drone


def test_drone_zoom_gimbal_sticky_across_partial_osds():
    """zoom/gimbal 在后续不带 cameras 的 OSD 里要保留（DJI 部分 OSD 不重复带这些字段）。"""
    s = LiveState()
    s.update("thing/product/SN_DRONE/osd", _DRONE_OSD_WITH_CAMERA, recv_ts_ms=1000)
    # 后续一个不带 cameras 的 OSD。
    s.update("thing/product/SN_DRONE/osd", {
        "data": {"latitude": 30.01, "longitude": 121.0, "battery": {"capacity_percent": 49}},
    }, recv_ts_ms=1200)
    drone = s.snapshot()["drone"]
    assert drone["zoom_factor"] == 4.06
    assert drone["gimbal_pitch"] == -21.0
    assert drone["gimbal_yaw"] == 68.4


# ---------------------------------------------------------------------------
# 控制消息环（drc/down + services）
# ---------------------------------------------------------------------------

def test_drc_down_message_captured_to_controls():
    """thing/product/<sn>/drc/down (飞控指令) 走 controls 环；保留完整 payload。"""
    s = LiveState()
    drc_payload = {
        "method": "stick_control",
        "data": {"x": 0.5, "y": -0.2, "z": 0.0, "yaw": 30.0},
    }
    s.update("thing/product/SN_DOCK/drc/down", drc_payload, recv_ts_ms=1000)
    controls = s.snapshot()["controls"]
    assert len(controls) == 1
    c = controls[0]
    assert c["topic"] == "thing/product/SN_DOCK/drc/down"
    assert c["suffix"] == "drc/down"
    assert c["method"] == "stick_control"
    assert c["recv_ts_ms"] == 1000
    # 完整 payload 必须留下来，前端 modal 展开看
    assert c["payload"] == drc_payload


def test_services_message_captured_to_controls():
    """thing/product/<sn>/services (航线/任务下行) 也走 controls 环。"""
    s = LiveState()
    svc_payload = {
        "method": "flighttask_prepare",
        "data": {"flight_id": "T-X", "file": {"url": "https://...", "fingerprint": "ab"}},
    }
    s.update("thing/product/SN_DOCK/services", svc_payload, recv_ts_ms=2000)
    controls = s.snapshot()["controls"]
    assert len(controls) == 1
    c = controls[0]
    assert c["topic"] == "thing/product/SN_DOCK/services"
    assert c["suffix"] == "services"
    assert c["method"] == "flighttask_prepare"
    assert c["payload"] == svc_payload


def test_controls_ring_caps_at_max():
    """drc/down 在真实飞行里可达 30Hz；环必须封顶不无限增长。"""
    s = LiveState(controls_ring_size=10)
    for i in range(15):
        s.update("thing/product/SN_DOCK/drc/down", {
            "method": "stick_control",
            "data": {"x": i * 0.1},
        }, recv_ts_ms=1000 + i * 33)
    controls = s.snapshot()["controls"]
    assert len(controls) == 10
    # 留下的是最新的 10 条
    assert controls[0]["payload"]["data"]["x"] == pytest.approx(0.5)
    assert controls[-1]["payload"]["data"]["x"] == pytest.approx(1.4)


def test_controls_does_not_touch_dock_or_drone_state():
    """控制消息只进 controls 环，不应该改 dock/drone 任何字段。"""
    s = LiveState()
    s.update("thing/product/SN_DOCK/drc/down",
             {"method": "stick_control", "data": {"x": 1.0}}, recv_ts_ms=1000)
    s.update("thing/product/SN_DOCK/services",
             {"method": "takeoff_to_point", "data": {"latitude": 30.0}}, recv_ts_ms=1100)
    snap = s.snapshot()
    assert snap["dock"] == {}
    assert snap["drone"] == {}


def test_events_and_controls_are_independent_rings():
    """events 与 controls 互不干扰。"""
    s = LiveState()
    s.update("thing/product/SN_DOCK/events",
             {"method": "device_online", "data": {}}, recv_ts_ms=1000)
    s.update("thing/product/SN_DOCK/drc/down",
             {"method": "stick_control", "data": {}}, recv_ts_ms=1100)
    s.update("thing/product/SN_DOCK/services",
             {"method": "takeoff", "data": {}}, recv_ts_ms=1200)
    snap = s.snapshot()
    assert len(snap["events"]) == 1 and snap["events"][0]["method"] == "device_online"
    assert len(snap["controls"]) == 2
    assert [c["method"] for c in snap["controls"]] == ["stick_control", "takeoff"]


def test_initial_snapshot_has_controls_field():
    """空 LiveState 的 snapshot 也要包含 controls 字段（前端总能读到一个数组）。"""
    s = LiveState()
    snap = s.snapshot()
    assert snap["controls"] == []


def test_flight_idle_listener_fires_when_step_code_leaves_recording():
    """收到 dock OSD 带 flighttask_step_code=5 时，所有注册的 listener 都被调用。"""
    calls = {"a": 0, "b": 0}
    state = LiveState(on_flight_idle=[
        lambda: calls.__setitem__("a", calls["a"] + 1),
        lambda: calls.__setitem__("b", calls["b"] + 1),
    ])
    # 先发一条 recording 状态确立 dock_sn
    state.update(
        topic="thing/product/SN_DOCK/osd",
        payload={"data": {"flighttask_step_code": 1,
                          "sub_device": {"device_sn": "SN_DRONE"}}},
        recv_ts_ms=1000,
    )
    assert calls == {"a": 0, "b": 0}, "recording 状态不该触发"
    # 再发一条 idle (=5)
    state.update(
        topic="thing/product/SN_DOCK/osd",
        payload={"data": {"flighttask_step_code": 5}},
        recv_ts_ms=2000,
    )
    assert calls == {"a": 1, "b": 1}


def test_flight_idle_listener_exception_isolated_per_listener():
    """某个 listener 抛错不影响其它 listener、不阻断 trail.clear。"""
    other_called = []

    def boom():
        raise RuntimeError("listener went boom")

    state = LiveState(on_flight_idle=[
        boom,
        lambda: other_called.append(True),
    ])
    state.update(
        topic="thing/product/SN_DOCK/osd",
        payload={"data": {"flighttask_step_code": 1,
                          "sub_device": {"device_sn": "SN_DRONE"}}},
        recv_ts_ms=1000,
    )
    state.update(
        topic="thing/product/SN_DRONE/osd",
        payload={"data": {"latitude": 30.0, "longitude": 120.0}},
        recv_ts_ms=1500,
    )
    snap = state.snapshot()
    assert len(snap["drone_trail"]) == 1
    state.update(
        topic="thing/product/SN_DOCK/osd",
        payload={"data": {"flighttask_step_code": 5}},
        recv_ts_ms=2000,
    )
    # boom listener 抛错被吞，其它 listener 仍被调用，trail 仍被清
    assert other_called == [True]
    assert state.snapshot()["drone_trail"] == []
