"""Canonical test pattern. Copy this shape for new tests.

Demonstrates:
- httpx.MockTransport for hermetic HTTP tests (no network).
- @pytest.mark.anyio for async tests via anyio's pytest plugin.
- Asserting on domain errors, not on HTTP status codes in caller code.
- hypothesis for property tests at the boundary (JSON round-trip).
"""

from __future__ import annotations

from collections.abc import Callable

import httpx
import pytest
from hypothesis import given
from hypothesis import strategies as st

from myproject.errors import ExternalServiceError, NotFoundError
from myproject.example_service import HealthResponse, check_service_health

pytestmark = pytest.mark.anyio

Handler = Callable[[httpx.Request], httpx.Response]


def _client_with_handler(handler: Handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def test_ok_response_parses() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"status": "ok", "version": "1.2.3", "uptime_seconds": 42.0},
        )

    async with _client_with_handler(handler) as client:
        result = await check_service_health(client, "https://svc.example")

    assert result == HealthResponse(status="ok", version="1.2.3", uptime_seconds=42.0)


async def test_404_raises_not_found() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    async with _client_with_handler(handler) as client:
        with pytest.raises(NotFoundError):
            await check_service_health(client, "https://svc.example")


async def test_500_raises_external_service_error() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="boom")

    async with _client_with_handler(handler) as client:
        with pytest.raises(ExternalServiceError):
            await check_service_health(client, "https://svc.example")


async def test_transport_error_wraps_as_external_service_error() -> None:
    """httpx.RequestError is wrapped at the boundary; never leaks."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    async with _client_with_handler(handler) as client:
        with pytest.raises(ExternalServiceError):
            await check_service_health(client, "https://svc.example")


async def test_malformed_response_wraps_as_external_service_error() -> None:
    """Pydantic shape-drift surfaces as a domain error at the boundary, not raw ValidationError."""

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "status": "ok",
                "version": "1",
                "uptime_seconds": 0,
                "new_field": "surprise",
            },
        )

    async with _client_with_handler(handler) as client:
        with pytest.raises(ExternalServiceError):
            await check_service_health(client, "https://svc.example")


@given(uptime=st.floats(min_value=0, max_value=1e9, allow_nan=False))
def test_health_response_json_round_trips(uptime: float) -> None:
    """Boundary model serializes and parses back losslessly."""
    original = HealthResponse(status="ok", version="1", uptime_seconds=uptime)
    parsed = HealthResponse.model_validate_json(original.model_dump_json())
    assert parsed == original
