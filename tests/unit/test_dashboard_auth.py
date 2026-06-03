import pytest
from fastapi import HTTPException

from sim_dji_cloud.dashboard.auth import require_token


def test_require_token_503_when_env_not_set(monkeypatch):
    monkeypatch.delenv("DASHBOARD_TOKEN", raising=False)
    with pytest.raises(HTTPException) as exc_info:
        require_token(authorization="Bearer anything")
    assert exc_info.value.status_code == 503
    assert "DASHBOARD_TOKEN" in str(exc_info.value.detail)


def test_require_token_503_when_env_empty_string(monkeypatch):
    """空字符串也按未设处理，避免 secrets.compare_digest("", "") 误放行。"""
    monkeypatch.setenv("DASHBOARD_TOKEN", "")
    with pytest.raises(HTTPException) as exc_info:
        require_token(authorization="Bearer anything")
    assert exc_info.value.status_code == 503


def test_require_token_401_when_missing_header(monkeypatch):
    monkeypatch.setenv("DASHBOARD_TOKEN", "secret123")
    with pytest.raises(HTTPException) as exc_info:
        require_token(authorization=None)
    assert exc_info.value.status_code == 401


def test_require_token_401_when_malformed_header(monkeypatch):
    """Authorization 不以 'Bearer ' 开头 → 401。"""
    monkeypatch.setenv("DASHBOARD_TOKEN", "secret123")
    with pytest.raises(HTTPException) as exc_info:
        require_token(authorization="Basic xxx")
    assert exc_info.value.status_code == 401


def test_require_token_401_when_token_mismatch(monkeypatch):
    monkeypatch.setenv("DASHBOARD_TOKEN", "secret123")
    with pytest.raises(HTTPException) as exc_info:
        require_token(authorization="Bearer wrong_token")
    assert exc_info.value.status_code == 401


def test_require_token_passes_when_match(monkeypatch):
    monkeypatch.setenv("DASHBOARD_TOKEN", "secret123")
    assert require_token(authorization="Bearer secret123") is None
