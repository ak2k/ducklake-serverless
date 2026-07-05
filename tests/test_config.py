"""Canonical settings test pattern."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from myproject.config import get_settings


def test_required_env_loads(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MYPROJECT_HEALTH_CHECK_URL", "https://svc.example")
    settings = get_settings()
    assert settings.health_check_url == "https://svc.example"
    assert settings.log_level == "info"
    assert settings.log_json is False


def test_missing_required_env_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MYPROJECT_HEALTH_CHECK_URL", raising=False)
    with pytest.raises(ValidationError):
        get_settings()


def test_log_level_validates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Bad LogLevel literal value fails at construction, not at use."""
    monkeypatch.setenv("MYPROJECT_HEALTH_CHECK_URL", "https://svc.example")
    monkeypatch.setenv("MYPROJECT_LOG_LEVEL", "verbose")
    with pytest.raises(ValidationError):
        get_settings()
