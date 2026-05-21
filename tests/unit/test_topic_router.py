from sim_dji_cloud.recorder.topic_router import (
    route_topic, RoutedTopic, Source, file_name_for_topic, is_denied,
)


def test_route_dock_osd_when_sn_matches_dock():
    r = route_topic("thing/product/SN_DOCK/osd", dock_sn="SN_DOCK", drone_sn="SN_DRONE")
    assert r == RoutedTopic(device_sn="SN_DOCK", source=Source.DOCK_OSD, direction="up")


def test_route_drone_osd_when_sn_matches_drone():
    r = route_topic("thing/product/SN_DRONE/osd", dock_sn="SN_DOCK", drone_sn="SN_DRONE")
    assert r == RoutedTopic(device_sn="SN_DRONE", source=Source.DRONE_OSD, direction="up")


def test_route_unknown_osd_when_neither_dock_nor_drone():
    """多机场共享 broker 环境：drone_sn 未抵达 + device_sn != dock_sn 的 osd
    应返回 UNKNOWN，由 Recorder 决定是否丢弃。
    避免被其他机场的 osd 抢占 drone_sn 位置。"""
    r = route_topic("thing/product/SN_X/osd", dock_sn="SN_DOCK", drone_sn=None)
    assert r.device_sn == "SN_X"
    assert r.source == Source.UNKNOWN

    # drone_sn 已设但不匹配，仍 UNKNOWN
    r2 = route_topic("thing/product/SN_OTHER/osd", dock_sn="SN_DOCK", drone_sn="SN_DRONE")
    assert r2.source == Source.UNKNOWN


def test_route_dock_services_and_reply():
    assert route_topic("thing/product/SN_DOCK/services", "SN_DOCK", "SN_DRONE").source == Source.DOCK_SERVICES
    r = route_topic("thing/product/SN_DOCK/services_reply", "SN_DOCK", "SN_DRONE")
    assert r.source == Source.DOCK_SERVICES_REPLY
    assert r.direction == "svc_rsp"


def test_route_dock_drc():
    assert route_topic("thing/product/SN_DOCK/drc/up", "SN_DOCK", "SN_DRONE").source == Source.DOCK_DRC_UP
    assert route_topic("thing/product/SN_DOCK/drc/down", "SN_DOCK", "SN_DRONE").source == Source.DOCK_DRC_DOWN


def test_route_property_set_direction_is_down():
    r = route_topic("thing/product/SN_DOCK/property/set", "SN_DOCK", "SN_DRONE")
    assert r.source == Source.DOCK_PROPERTY_SET
    assert r.direction == "down"


def test_file_name_translates_slashes():
    assert file_name_for_topic("thing/product/SN_DOCK/drc/up") == "thing__product__SN_DOCK__drc__up.jsonl"


def test_is_denied_exact_and_regex():
    rules = [
        {"exact": "thing/product/SN_DOCK/firmware_upgrade"},
        {"regex": r"thing/product/.+/token.*"},
    ]
    assert is_denied("thing/product/SN_DOCK/firmware_upgrade", rules)
    assert is_denied("thing/product/SN_DOCK/token_refresh", rules)
    assert not is_denied("thing/product/SN_DOCK/osd", rules)


def test_is_denied_bad_regex_falls_back_silently():
    rules = [{"regex": r"["}]
    assert not is_denied("thing/product/SN/osd", rules)
