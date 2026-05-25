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
    """非 osd/events 的 topic 也算计数，但不解码字段。"""
    s = LiveState()
    s.update("thing/product/SN_DOCK/services", {"method": "foo", "data": {}},
             recv_ts_ms=1000)
    snap = s.snapshot()
    assert snap["topic_counts"]["thing/product/SN_DOCK/services"] == 1
    assert snap["dock"] == {}


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
