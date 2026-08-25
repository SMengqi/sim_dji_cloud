from sim_dji_cloud.recorder.pilot_topic_router import (
    route_topic, RoutedTopic, Source,
)


def test_route_rc_osd_when_sn_matches_rc():
    r = route_topic("thing/product/SN_RC/osd", rc_sn="SN_RC", aircraft_sn="SN_AIRCRAFT")
    assert r == RoutedTopic(device_sn="SN_RC", source=Source.RC_OSD, direction="up")


def test_route_aircraft_osd_when_sn_matches_aircraft():
    r = route_topic("thing/product/SN_AIRCRAFT/osd", rc_sn="SN_RC", aircraft_sn="SN_AIRCRAFT")
    assert r == RoutedTopic(device_sn="SN_AIRCRAFT", source=Source.AIRCRAFT_OSD, direction="up")


def test_route_unknown_osd_when_neither_rc_nor_aircraft():
    r = route_topic("thing/product/SN_X/osd", rc_sn="SN_RC", aircraft_sn=None)
    assert r.device_sn == "SN_X"
    assert r.source == Source.UNKNOWN

    r2 = route_topic("thing/product/SN_OTHER/osd", rc_sn="SN_RC", aircraft_sn="SN_AIRCRAFT")
    assert r2.source == Source.UNKNOWN


def test_route_rc_state_and_aircraft_state():
    r1 = route_topic("thing/product/SN_RC/state", rc_sn="SN_RC", aircraft_sn="SN_AIRCRAFT")
    assert r1.source == Source.RC_STATE
    r2 = route_topic("thing/product/SN_AIRCRAFT/state", rc_sn="SN_RC", aircraft_sn="SN_AIRCRAFT")
    assert r2.source == Source.AIRCRAFT_STATE


def test_route_rc_services_and_reply():
    assert route_topic("thing/product/SN_RC/services", "SN_RC", "SN_AIRCRAFT").source == Source.RC_SERVICES
    r = route_topic("thing/product/SN_RC/services_reply", "SN_RC", "SN_AIRCRAFT")
    assert r.source == Source.RC_SERVICES_REPLY
    assert r.direction == "svc_rsp"


def test_route_rc_drc_up_down():
    assert route_topic("thing/product/SN_RC/drc/up", "SN_RC", "SN_AIRCRAFT").source == Source.RC_DRC_UP
    assert route_topic("thing/product/SN_RC/drc/down", "SN_RC", "SN_AIRCRAFT").source == Source.RC_DRC_DOWN


def test_route_property_set_direction_is_down():
    r = route_topic("thing/product/SN_RC/property/set", "SN_RC", "SN_AIRCRAFT")
    assert r.source == Source.RC_PROPERTY_SET
    assert r.direction == "down"


def test_route_rc_status_topology():
    r = route_topic("sys/product/SN_RC/status", rc_sn="SN_RC", aircraft_sn=None)
    assert r.source == Source.RC_STATUS
    assert r.device_sn == "SN_RC"
