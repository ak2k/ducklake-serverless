"""Shared test fixtures."""

import pytest


@pytest.fixture
def anyio_backend() -> str:
    """Default anyio backend for async tests. Override per-test if needed."""
    return "asyncio"
