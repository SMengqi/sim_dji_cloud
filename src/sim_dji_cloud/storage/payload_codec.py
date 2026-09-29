"""MQTT 负载 ↔ JSONL record 的编解码。

JSON 负载照旧存 ``payload``；非 JSON 负载（DJI 的 drc/up、飞行器 state 偶发
二进制 / 非 UTF-8，2026-09-28 生产）存 ``payload: null`` + ``payload_b64``
（原始字节 base64），回放时原样重发。对旧 record 纯增字段，读方不受影响。
"""
import base64
import json
from typing import Any, Optional, Union


def decode_mqtt_payload(payload: Union[bytes, str]) -> tuple[Any, Optional[bytes]]:
    """返回 ``(payload_obj, raw)``：JSON → ``(obj, None)``；非 JSON → ``(None, 原始字节)``。

    空负载视为 ``{}``。非 bytes/str 抛 ``TypeError``。
    """
    if not payload:
        return {}, None
    if isinstance(payload, str):
        raw = payload.encode("utf-8")
    elif isinstance(payload, (bytes, bytearray)):
        raw = bytes(payload)
    else:
        raise TypeError(f"unexpected payload type {type(payload).__name__}")
    try:
        return json.loads(raw.decode("utf-8")), None
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None, raw


def encode_raw_b64(raw: bytes) -> str:
    return base64.b64encode(raw).decode("ascii")


def describe_raw(raw: bytes, head: int = 64) -> str:
    """诊断用：``len=N head=<hex>``。"""
    return f"len={len(raw)} head={raw[:head].hex()}"


def record_payload_bytes(record: dict) -> bytes:
    """回放用：record → 要发布的字节。有 ``payload_b64`` 则原样还原。"""
    b64 = record.get("payload_b64")
    if b64 is not None:
        return base64.b64decode(b64)
    return json.dumps(record.get("payload", {}), ensure_ascii=False,
                      separators=(",", ":")).encode("utf-8")
