from dataclasses import dataclass
from enum import Enum
from typing import Optional


class Source(str, Enum):
    RC_OSD = "rc_osd"
    AIRCRAFT_OSD = "aircraft_osd"
    RC_STATE = "rc_state"
    AIRCRAFT_STATE = "aircraft_state"
    RC_EVENTS = "rc_events"
    RC_SERVICES = "rc_services"
    RC_SERVICES_REPLY = "rc_services_reply"
    RC_REQUESTS = "rc_requests"
    RC_REQUESTS_REPLY = "rc_requests_reply"
    RC_PROPERTY_SET = "rc_property_set"
    RC_PROPERTY_SET_REPLY = "rc_property_set_reply"
    RC_DRC_UP = "rc_drc_up"
    RC_DRC_DOWN = "rc_drc_down"
    RC_STATUS = "rc_status"               # sys/product/<rc_sn>/status（拓扑）
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class RoutedTopic:
    device_sn: str
    source: Source
    direction: str   # up | down | svc_req | svc_rsp


_SUFFIX_DIRECTION = {
    "services": "svc_req",
    "services_reply": "svc_rsp",
    "property/set": "down",
    "property/set_reply": "down",
    "requests": "up",
    "requests_reply": "svc_rsp",
}

_SUFFIX_TO_RC_SOURCE = {
    "events": Source.RC_EVENTS,
    "services": Source.RC_SERVICES,
    "services_reply": Source.RC_SERVICES_REPLY,
    "requests": Source.RC_REQUESTS,
    "requests_reply": Source.RC_REQUESTS_REPLY,
    "property/set": Source.RC_PROPERTY_SET,
    "property/set_reply": Source.RC_PROPERTY_SET_REPLY,
    "drc/up": Source.RC_DRC_UP,
    "drc/down": Source.RC_DRC_DOWN,
}


def route_topic(topic: str, rc_sn: str, aircraft_sn: Optional[str]) -> RoutedTopic:
    parts = topic.split("/")
    if len(parts) < 4:
        return RoutedTopic(device_sn="", source=Source.UNKNOWN, direction="up")

    namespace = parts[0]              # "thing" or "sys"
    device_sn = parts[2]
    suffix = "/".join(parts[3:])

    is_rc = device_sn == rc_sn
    is_aircraft = aircraft_sn is not None and device_sn == aircraft_sn

    direction = _SUFFIX_DIRECTION.get(suffix, "up")

    if namespace == "sys" and suffix == "status":
        return RoutedTopic(device_sn, Source.RC_STATUS, direction)

    if suffix == "osd":
        if is_rc:
            src = Source.RC_OSD
        elif is_aircraft:
            src = Source.AIRCRAFT_OSD
        else:
            src = Source.UNKNOWN
        return RoutedTopic(device_sn, src, direction)

    if suffix == "state":
        if is_rc:
            src = Source.RC_STATE
        elif is_aircraft:
            src = Source.AIRCRAFT_STATE
        else:
            src = Source.UNKNOWN
        return RoutedTopic(device_sn, src, direction)

    src = _SUFFIX_TO_RC_SOURCE.get(suffix, Source.UNKNOWN)
    return RoutedTopic(device_sn, src, direction)
