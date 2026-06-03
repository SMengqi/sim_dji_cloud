"""FastAPI dependency：Bearer-token 鉴权 for write API。

DASHBOARD_TOKEN 环境变量是开关 + 密钥。未设 / 空 → 写 API 全 503。
secure-by-default 设计详见 http-control-design.md §2 D4。
"""
import os
import secrets

from fastapi import Header, HTTPException


def require_token(authorization: str | None = Header(None)) -> None:
    """检查 Authorization: Bearer <DASHBOARD_TOKEN>。

    DASHBOARD_TOKEN 未设 / 空 → 503 (write API disabled)
    Authorization header 缺 / 格式错 → 401
    token 不匹配 → 401
    匹配 → 放行 (return None)

    用 secrets.compare_digest 比对，防时序攻击。
    """
    expected = os.environ.get("DASHBOARD_TOKEN")
    if not expected:
        raise HTTPException(503, "write API disabled: set DASHBOARD_TOKEN")
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "missing or malformed Authorization header")
    token = authorization[7:]
    if not secrets.compare_digest(token, expected):
        raise HTTPException(401, "invalid token")
