from sim_dji_cloud.recorder.flight_detector import (
    FlightDetector, FlightState, RuleEvaluator,
)
from sim_dji_cloud.recorder.topic_router import Source


def test_rule_match_field_not_equals():
    rules = [{"source": "dock_osd", "field": "wayline_mission_state", "not_equals": "idle"}]
    ev = RuleEvaluator(rules)
    matched, _ = ev.evaluate(Source.DOCK_OSD, {"wayline_mission_state": "executing"}, now_ms=1000)
    assert matched
    matched, _ = ev.evaluate(Source.DOCK_OSD, {"wayline_mission_state": "idle"}, now_ms=1000)
    assert not matched


def test_rule_match_payload_method():
    rules = [{"source": "dock_services", "payload_match": {"method": "wayline_prepare"}}]
    ev = RuleEvaluator(rules)
    matched, _ = ev.evaluate(Source.DOCK_SERVICES, {"method": "wayline_prepare", "tid": "t1"}, 1000)
    assert matched
    matched, _ = ev.evaluate(Source.DOCK_SERVICES, {"method": "other"}, 1000)
    assert not matched


def test_rule_sustain_seconds_requires_continuous_match():
    rules = [{
        "source": "dock_osd", "field": "wayline_mission_state",
        "equals": "idle", "sustain_seconds": 30,
    }]
    ev = RuleEvaluator(rules)
    m1, _ = ev.evaluate(Source.DOCK_OSD, {"wayline_mission_state": "idle"}, 0)
    assert not m1
    m2, _ = ev.evaluate(Source.DOCK_OSD, {"wayline_mission_state": "idle"}, 10_000)
    assert not m2
    m3, _ = ev.evaluate(Source.DOCK_OSD, {"wayline_mission_state": "idle"}, 31_000)
    assert m3


def test_rule_sustain_resets_when_condition_breaks():
    rules = [{
        "source": "dock_osd", "field": "wayline_mission_state",
        "equals": "idle", "sustain_seconds": 30,
    }]
    ev = RuleEvaluator(rules)
    ev.evaluate(Source.DOCK_OSD, {"wayline_mission_state": "idle"}, 0)
    ev.evaluate(Source.DOCK_OSD, {"wayline_mission_state": "executing"}, 5_000)
    m, _ = ev.evaluate(Source.DOCK_OSD, {"wayline_mission_state": "idle"}, 6_000)
    assert not m
    m, _ = ev.evaluate(Source.DOCK_OSD, {"wayline_mission_state": "idle"}, 36_000)
    assert m


def test_flight_detector_transitions_idle_to_recording():
    start_rules = [{"source": "dock_services", "payload_match": {"method": "wayline_prepare"}}]
    d = FlightDetector(start_rules=start_rules, end_rules=[])
    assert d.state == FlightState.IDLE
    d.feed(Source.DOCK_SERVICES, {"method": "wayline_prepare"}, now_ms=1000)
    assert d.state == FlightState.RECORDING


def test_flight_detector_extracts_task_id_from_payload():
    start_rules = [{"source": "dock_services", "payload_match": {"method": "wayline_prepare"}}]
    d = FlightDetector(start_rules=start_rules, end_rules=[])
    d.feed(Source.DOCK_SERVICES,
           {"method": "wayline_prepare", "data": {"flight_id": "T-2026-001"}},
           now_ms=1000)
    assert d.task_id == "T-2026-001"


def test_flight_detector_end_rule_transitions_to_finalizing():
    start_rules = [{"source": "dock_services", "payload_match": {"method": "wayline_prepare"}}]
    end_rules = [{"source": "drone_osd", "field": "mode_code", "in": ["standby", "landed"]}]
    d = FlightDetector(start_rules=start_rules, end_rules=end_rules)
    d.feed(Source.DOCK_SERVICES, {"method": "wayline_prepare", "data": {"flight_id": "T1"}}, now_ms=0)
    assert d.state == FlightState.RECORDING
    d.feed(Source.DRONE_OSD, {"mode_code": "manual_flight"}, now_ms=1000)
    assert d.state == FlightState.RECORDING  # not a terminal mode
    d.feed(Source.DRONE_OSD, {"mode_code": "landed"}, now_ms=2000)
    assert d.state == FlightState.FINALIZING
    assert d.end_reason == "auto_idle"
    assert d.task_ended_ms == 2000


def test_force_stop_sets_state_and_reason():
    d = FlightDetector(start_rules=[], end_rules=[])
    d.force_stop(now_ms=5000, reason="manual_stop")
    assert d.state == FlightState.FINALIZING
    assert d.end_reason == "manual_stop"
    assert d.task_ended_ms == 5000


def test_rule_skips_when_source_does_not_match():
    rules = [{"source": "dock_osd", "field": "x", "equals": 1}]
    ev = RuleEvaluator(rules)
    # event source != rule source → no match regardless of payload
    matched, _ = ev.evaluate(Source.DRONE_OSD, {"x": 1}, now_ms=0)
    assert not matched
    # event source == rule source → match
    matched, _ = ev.evaluate(Source.DOCK_OSD, {"x": 1}, now_ms=0)
    assert matched


def test_extract_task_id_fallback_chain():
    start_rules = [{"source": "dock_services", "payload_match": {"method": "wayline_prepare"}}]
    # Case 1: only top-level payload.flight_id
    d = FlightDetector(start_rules=start_rules, end_rules=[])
    d.feed(Source.DOCK_SERVICES,
           {"method": "wayline_prepare", "flight_id": "TOP_LEVEL_T"},
           now_ms=0)
    assert d.task_id == "TOP_LEVEL_T"

    # Case 2: only data.task_id (no data.flight_id)
    d2 = FlightDetector(start_rules=start_rules, end_rules=[])
    d2.feed(Source.DOCK_SERVICES,
            {"method": "wayline_prepare", "data": {"task_id": "DATA_TASK_T"}},
            now_ms=0)
    assert d2.task_id == "DATA_TASK_T"

    # Case 3: data.flight_id wins over data.task_id when both present
    d3 = FlightDetector(start_rules=start_rules, end_rules=[])
    d3.feed(Source.DOCK_SERVICES,
            {"method": "wayline_prepare",
             "data": {"flight_id": "FLIGHT_T", "task_id": "TASK_T"},
             "flight_id": "TOP_T"},
            now_ms=0)
    assert d3.task_id == "FLIGHT_T"


def test_multiple_rules_any_match_returns_true():
    """If multiple rules in list, any match should win."""
    rules = [
        {"source": "dock_osd", "field": "x", "equals": "no_match"},
        {"source": "dock_osd", "field": "y", "equals": "yes"},
    ]
    ev = RuleEvaluator(rules)
    matched, rule = ev.evaluate(Source.DOCK_OSD, {"y": "yes"}, now_ms=0)
    assert matched
    assert rule["field"] == "y"


def test_dot_path_field_access():
    """规则的 field 支持 'a.b.c' 点路径取值，匹配真实 DJI payload 结构
    （mode_code 在 payload.data.mode_code，drone_in_dock 在 payload.data.drone_in_dock）。"""
    rules = [{"source": "drone_osd", "field": "data.mode_code", "not_equals": 0}]
    ev = RuleEvaluator(rules)

    # drone 在飞（mode_code=5）→ 匹配
    matched, _ = ev.evaluate(Source.DRONE_OSD, {"data": {"mode_code": 5}}, now_ms=0)
    assert matched

    # drone standby（mode_code=0）→ 不匹配
    matched, _ = ev.evaluate(Source.DRONE_OSD, {"data": {"mode_code": 0}}, now_ms=0)
    assert not matched

    # 字段缺失（任一层非 dict）→ 视为 None，not_equals 0 比较 → 匹配（None != 0）
    matched, _ = ev.evaluate(Source.DRONE_OSD, {"foo": "bar"}, now_ms=0)
    assert matched
    matched, _ = ev.evaluate(Source.DRONE_OSD, {"data": "not-a-dict"}, now_ms=0)
    assert matched


def test_real_drone_osd_flight_lifecycle():
    """模拟真实抓取数据中观察到的 mode_code 切换：
    5(wayline) → 9(RTH) → 10(landing) → 0(standby) 持续 30s
    验证 start 立即触发、end 正确判定。"""
    start_rules = [{"source": "drone_osd", "field": "data.mode_code", "not_equals": 0}]
    end_rules = [
        {"source": "drone_osd", "field": "data.mode_code",
         "equals": 0, "sustain_seconds": 30}
    ]
    d = FlightDetector(start_rules=start_rules, end_rules=end_rules)

    # 第一条 drone_osd: 已经在飞 (mode_code=5)
    d.feed(Source.DRONE_OSD, {"data": {"mode_code": 5}}, now_ms=0)
    assert d.state == FlightState.RECORDING

    # RTH (mode 9)
    d.feed(Source.DRONE_OSD, {"data": {"mode_code": 9}}, now_ms=10_000)
    assert d.state == FlightState.RECORDING

    # 自动降落 (mode 10)
    d.feed(Source.DRONE_OSD, {"data": {"mode_code": 10}}, now_ms=20_000)
    assert d.state == FlightState.RECORDING

    # 落地，回到 standby (mode 0)
    d.feed(Source.DRONE_OSD, {"data": {"mode_code": 0}}, now_ms=30_000)
    assert d.state == FlightState.RECORDING  # sustain 还没到 30s

    # standby 持续 < 30s
    d.feed(Source.DRONE_OSD, {"data": {"mode_code": 0}}, now_ms=50_000)
    assert d.state == FlightState.RECORDING

    # standby 持续 ≥ 30s → 触发 end
    d.feed(Source.DRONE_OSD, {"data": {"mode_code": 0}}, now_ms=60_500)
    assert d.state == FlightState.FINALIZING
    assert d.end_reason == "auto_idle"


def test_real_services_method_triggers_start():
    """services 方法名（基于真实抓包）应能触发 start。"""
    start_rules = [{
        "source": "dock_services",
        "field": "method",
        "in": ["takeoff_to_point", "return_home", "wayline_prepare"],
    }]
    d = FlightDetector(start_rules=start_rules, end_rules=[])
    d.feed(Source.DOCK_SERVICES,
           {"method": "takeoff_to_point", "data": {"flight_id": "T-REAL"}},
           now_ms=0)
    assert d.state == FlightState.RECORDING
    assert d.task_id == "T-REAL"


def test_drone_in_dock_end_signal():
    """兜底 end 规则：drone_in_dock=1 持续 30s。"""
    start_rules = [{"source": "drone_osd", "field": "data.mode_code", "not_equals": 0}]
    end_rules = [{
        "source": "dock_osd",
        "field": "data.drone_in_dock",
        "equals": 1,
        "sustain_seconds": 30,
    }]
    d = FlightDetector(start_rules=start_rules, end_rules=end_rules)

    # 飞行中
    d.feed(Source.DRONE_OSD, {"data": {"mode_code": 5}}, now_ms=0)
    assert d.state == FlightState.RECORDING

    # 飞行器入坞
    d.feed(Source.DOCK_OSD, {"data": {"drone_in_dock": 1}}, now_ms=10_000)
    assert d.state == FlightState.RECORDING

    # 持续 30s 后入坞
    d.feed(Source.DOCK_OSD, {"data": {"drone_in_dock": 1}}, now_ms=40_500)
    assert d.state == FlightState.FINALIZING


def test_extract_task_id_does_not_crash_when_data_is_list():
    """Real-world dock_events payloads (ADS-B etc.) carry `data` as a LIST, not a dict.
    _extract_task_id must return None instead of raising AttributeError."""
    start_rules = [{"source": "dock_osd", "field": "wayline_mission_state",
                    "not_equals": "idle"}]
    d = FlightDetector(start_rules=start_rules, end_rules=[])
    d.feed(Source.DOCK_OSD, {"wayline_mission_state": "executing"}, now_ms=0)
    assert d.state == FlightState.RECORDING
    assert d.task_id is None

    events_payload = {
        "bid": "synthetic-bid",
        "data": [{"icao": "ABCDEF", "altitude": 3000, "heading": 90.0}],
    }
    d.feed(Source.DOCK_EVENTS, events_payload, now_ms=1000)
    assert d.task_id is None
    assert d.state == FlightState.RECORDING


def test_extract_task_id_handles_non_dict_data_variants():
    """Defensively handle data being None, string, int, list, etc."""
    for bad_data in (None, "string-not-dict", 42, [], [1, 2, 3]):
        payload = {"method": "wayline_prepare", "data": bad_data}
        assert FlightDetector._extract_task_id(payload) is None
        payload_with_top = dict(payload, flight_id="TOP")
        assert FlightDetector._extract_task_id(payload_with_top) == "TOP"
