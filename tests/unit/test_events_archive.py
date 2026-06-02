import pytest
from sim_dji_cloud.dashboard.events_archive import EventsArchive


_DOCK = "SN_DOCK_FIXTURE"
_EVENTS_TOPIC   = f"thing/product/{_DOCK}/events"
_DRC_DOWN_TOPIC = f"thing/product/{_DOCK}/drc/down"
_SERVICES_TOPIC = f"thing/product/{_DOCK}/services"
_OSD_TOPIC      = f"thing/product/{_DOCK}/osd"


def _payload(method="x", **extra):
    p = {"method": method}
    p.update(extra)
    return p


def test_append_routes_events_topic_to_events_lane():
    a = EventsArchive()
    a.append(_EVENTS_TOPIC, _payload(method="alarm"), recv_ts_ms=1000)
    entries, _ = a.query()
    assert len(entries) == 1
    assert entries[0]["kind"] == "event"
    assert entries[0]["topic"] == _EVENTS_TOPIC
    assert entries[0]["method"] == "alarm"


def test_append_routes_drc_down_to_controls_lane():
    a = EventsArchive()
    a.append(_DRC_DOWN_TOPIC, _payload(method="stick_control"), recv_ts_ms=1500)
    entries, _ = a.query()
    assert entries[0]["kind"] == "control"


def test_append_routes_services_to_controls_lane():
    a = EventsArchive()
    a.append(_SERVICES_TOPIC, _payload(method="flighttask_create"), recv_ts_ms=2000)
    entries, _ = a.query()
    assert entries[0]["kind"] == "control"


def test_append_skips_osd_and_unknown_topics():
    a = EventsArchive()
    a.append(_OSD_TOPIC, _payload(), recv_ts_ms=1000)
    a.append("thing/product/X/totally/unknown", _payload(), recv_ts_ms=1000)
    entries, _ = a.query()
    assert entries == []


def test_session_started_at_ms_set_on_first_append():
    a = EventsArchive()
    assert a.session_started_at_ms is None
    a.append(_EVENTS_TOPIC, _payload(), recv_ts_ms=12345)
    assert a.session_started_at_ms == 12345
    a.append(_EVENTS_TOPIC, _payload(), recv_ts_ms=99999)
    assert a.session_started_at_ms == 12345


def test_reset_clears_both_lanes_and_session_start():
    a = EventsArchive()
    a.append(_EVENTS_TOPIC, _payload(), recv_ts_ms=1)
    a.append(_DRC_DOWN_TOPIC, _payload(), recv_ts_ms=2)
    a.reset()
    assert a.query()[0] == []
    assert a.session_started_at_ms is None


def test_reset_then_append_relights_session_start():
    a = EventsArchive()
    a.append(_EVENTS_TOPIC, _payload(), recv_ts_ms=1)
    a.reset()
    a.append(_EVENTS_TOPIC, _payload(), recv_ts_ms=2)
    assert a.session_started_at_ms == 2


def test_soft_cap_lru_drops_oldest_event():
    a = EventsArchive(soft_cap=3)
    for i in range(5):
        a.append(_EVENTS_TOPIC, _payload(method=str(i)), recv_ts_ms=i)
    entries, _ = a.query()
    methods = [e["method"] for e in entries]
    assert methods == ["2", "3", "4"]


def test_append_non_dict_payload_skips_silently():
    a = EventsArchive()
    a.append(_EVENTS_TOPIC, "not a dict", recv_ts_ms=1)
    a.append(_EVENTS_TOPIC, ["list"], recv_ts_ms=1)
    a.append(_EVENTS_TOPIC, None, recv_ts_ms=1)
    assert a.query()[0] == []


def test_query_filters_kinds():
    a = EventsArchive()
    a.append(_EVENTS_TOPIC,   _payload(method="e"), recv_ts_ms=1)
    a.append(_DRC_DOWN_TOPIC, _payload(method="c"), recv_ts_ms=2)
    entries, _ = a.query(kinds=("event",))
    assert [e["method"] for e in entries] == ["e"]
    entries, _ = a.query(kinds=("control",))
    assert [e["method"] for e in entries] == ["c"]


def test_query_time_window_filters_inclusive():
    a = EventsArchive()
    for ts in [100, 200, 300, 400, 500]:
        a.append(_EVENTS_TOPIC, _payload(method=str(ts)), recv_ts_ms=ts)
    entries, _ = a.query(since_ms=200, until_ms=400)
    methods = [e["method"] for e in entries]
    assert methods == ["200", "300", "400"]


def test_query_limit_truncates_keeping_most_recent_and_flags():
    a = EventsArchive()
    for ts in range(10):
        a.append(_EVENTS_TOPIC, _payload(method=str(ts)), recv_ts_ms=ts)
    entries, truncated = a.query(limit=3)
    assert truncated is True
    assert [e["method"] for e in entries] == ["7", "8", "9"]


def test_query_returns_sorted_merged_lanes():
    a = EventsArchive()
    a.append(_EVENTS_TOPIC,   _payload(method="e1"), recv_ts_ms=100)
    a.append(_DRC_DOWN_TOPIC, _payload(method="c1"), recv_ts_ms=50)
    a.append(_EVENTS_TOPIC,   _payload(method="e2"), recv_ts_ms=200)
    a.append(_SERVICES_TOPIC, _payload(method="c2"), recv_ts_ms=150)
    entries, _ = a.query()
    assert [e["method"] for e in entries] == ["c1", "e1", "c2", "e2"]


def test_query_returns_snapshot_immune_to_subsequent_append():
    a = EventsArchive()
    a.append(_EVENTS_TOPIC, _payload(method="first"), recv_ts_ms=1)
    entries, _ = a.query()
    a.append(_EVENTS_TOPIC, _payload(method="second"), recv_ts_ms=2)
    assert [e["method"] for e in entries] == ["first"]
