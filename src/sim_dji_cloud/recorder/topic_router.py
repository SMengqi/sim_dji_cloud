import re
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class Source(str, Enum):
    DOCK_OSD = "dock_osd"
    DRONE_OSD = "drone_osd"
    DOCK_STATE = "dock_state"
    DOCK_EVENTS = "dock_events"
    DOCK_SERVICES = "dock_services"
    DOCK_SERVICES_REPLY = "dock_services_reply"
    DOCK_REQUESTS = "dock_requests"
    DOCK_REQUESTS_REPLY = "dock_requests_reply"
    DOCK_PROPERTY_SET = "dock_property_set"
    DOCK_PROPERTY_SET_REPLY = "dock_property_set_reply"
    DOCK_DRC_UP = "dock_drc_up"
    DOCK_DRC_DOWN = "dock_drc_down"
    DOCK_STATUS = "dock_status"               # sys/product/<dock_sn>/status
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

_SUFFIX_TO_DOCK_SOURCE = {
    "state": Source.DOCK_STATE,
    "events": Source.DOCK_EVENTS,
    "services": Source.DOCK_SERVICES,
    "services_reply": Source.DOCK_SERVICES_REPLY,
    "requests": Source.DOCK_REQUESTS,
    "requests_reply": Source.DOCK_REQUESTS_REPLY,
    "property/set": Source.DOCK_PROPERTY_SET,
    "property/set_reply": Source.DOCK_PROPERTY_SET_REPLY,
    "drc/up": Source.DOCK_DRC_UP,
    "drc/down": Source.DOCK_DRC_DOWN,
}


def route_topic(topic: str, dock_sn: str, drone_sn: Optional[str]) -> RoutedTopic:
    parts = topic.split("/")
    if len(parts) < 4:
        return RoutedTopic(device_sn="", source=Source.UNKNOWN, direction="up")

    namespace = parts[0]              # "thing" or "sys"
    device_sn = parts[2]
    # suffix is the entire remainder — extra trailing segments fall through to UNKNOWN
    suffix = "/".join(parts[3:])

    is_dock = device_sn == dock_sn
    is_drone = drone_sn is not None and device_sn == drone_sn

    direction = _SUFFIX_DIRECTION.get(suffix, "up")

    if namespace == "sys" and suffix == "status":
        return RoutedTopic(device_sn, Source.DOCK_STATUS, direction)

    if suffix == "osd":
        if is_dock:
            src = Source.DOCK_OSD
        elif is_drone:
            src = Source.DRONE_OSD
        else:
            # drone_sn unknown or unmatched: a non-dock device publishing osd is the drone
            src = Source.DRONE_OSD
        return RoutedTopic(device_sn, src, direction)

    src = _SUFFIX_TO_DOCK_SOURCE.get(suffix, Source.UNKNOWN)
    return RoutedTopic(device_sn, src, direction)


def file_name_for_topic(topic: str) -> str:
    return topic.replace("/", "__") + ".jsonl"


def is_denied(topic: str, deny_rules: list[dict]) -> bool:
    for rule in deny_rules:
        if "exact" in rule:
            if topic == rule["exact"]:
                return True
        elif "regex" in rule:
            try:
                if re.search(rule["regex"], topic):
                    return True
            except re.error:
                continue
    return False
