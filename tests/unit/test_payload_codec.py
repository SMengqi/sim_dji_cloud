import base64

from sim_dji_cloud.storage.payload_codec import decode_mqtt_payload, record_payload_bytes


def test_decode_json_bytes():
    assert decode_mqtt_payload(b'{"a":1}') == ({"a": 1}, None)


def test_decode_json_str():
    assert decode_mqtt_payload('{"a":1}') == ({"a": 1}, None)


def test_decode_empty_is_empty_dict():
    assert decode_mqtt_payload(b"") == ({}, None)


def test_decode_non_json_returns_raw_bytes():
    raw = b"\x08\x01\x12\x03abc\xff"
    assert decode_mqtt_payload(raw) == (None, raw)


def test_decode_non_json_str_returns_utf8_bytes():
    assert decode_mqtt_payload("not json") == (None, b"not json")


def test_record_payload_bytes_json():
    assert record_payload_bytes({"payload": {"a": "中"}}) == '{"a":"中"}'.encode()


def test_record_payload_bytes_raw_b64_roundtrip():
    raw = b"\x00\xffbinary"
    rec = {"payload": None, "payload_b64": base64.b64encode(raw).decode("ascii")}
    assert record_payload_bytes(rec) == raw


def test_record_payload_bytes_missing_payload_is_empty_object():
    assert record_payload_bytes({}) == b"{}"
