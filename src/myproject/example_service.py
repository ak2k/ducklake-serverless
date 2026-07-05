"""Canonical service pattern. Copy this shape for new services.

Demonstrates:
- Pydantic boundary model with `extra="forbid", frozen=True` so upstream
  shape drift fails loudly. (For external APIs you don't control, see
  AGENTS.md "Appropriate divergence — Profile B (scraping)" — `extra="ignore"`
  is correct there.)
- Dependency injection of the httpx client — testable, no hidden globals.
- Domain errors raised at every boundary; httpx and pydantic exceptions
  never leak to callers.
- structlog for structured logging.
"""

from __future__ import annotations

from http import HTTPStatus
from typing import TYPE_CHECKING, Literal

import httpx
import structlog
from pydantic import BaseModel, ConfigDict, ValidationError

from myproject.errors import ExternalServiceError, NotFoundError

if TYPE_CHECKING:
    from structlog.stdlib import BoundLogger

# structlog.get_logger() returns Any; the annotation narrows it for callers.
log: BoundLogger = structlog.get_logger(__name__)  # pyright: ignore[reportAny]


class HealthResponse(BaseModel):
    """Health-check response from an internal service we control."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    status: Literal["ok", "degraded", "down"]
    version: str
    uptime_seconds: float


async def check_service_health(client: httpx.AsyncClient, base_url: str) -> HealthResponse:
    """Fetch /health from an internal service.

    Args:
        client: Injected httpx async client. Tests pass a MockTransport-backed one.
        base_url: Service base URL, without trailing slash.

    Returns:
        Parsed health response.

    Raises:
        NotFoundError: if the service has no /health endpoint.
        ExternalServiceError: for transport failures, non-404 HTTP errors,
            or upstream responses that fail to parse against the schema.

    """
    log.debug("check_service_health.start", base_url=base_url)
    try:
        response = await client.get(f"{base_url}/health")
    except httpx.RequestError as exc:
        raise ExternalServiceError(f"transport error contacting {base_url}: {exc}") from exc

    if response.status_code == HTTPStatus.NOT_FOUND:
        raise NotFoundError(f"no /health endpoint on {base_url}")
    if response.is_error:
        raise ExternalServiceError(
            f"{base_url} returned {response.status_code}: {response.content[:200]!r}"
        )

    try:
        return HealthResponse.model_validate_json(response.content)
    except ValidationError as exc:
        raise ExternalServiceError(f"{base_url} returned malformed health response: {exc}") from exc
