from collections.abc import Generator
from contextlib import contextmanager

from dagster import ConfigurableResource
from livetennisapi import LiveTennisAPI
from pydantic import Field


class LiveTennisApiResource(ConfigurableResource):
    """A Dagster resource wrapping the official ``livetennisapi`` Python SDK.

    Yields a fully configured :class:`livetennisapi.LiveTennisAPI` client via
    :meth:`get_client`, so assets and ops can call any SDK method directly.

    Example:

        .. code-block:: python

            from dagster import EnvVar, asset
            from dagster_livetennisapi import LiveTennisApiResource

            @asset
            def upcoming_fixtures(livetennisapi: LiveTennisApiResource):
                with livetennisapi.get_client() as client:
                    return [f.to_dict() for f in client.list_fixtures(limit=200)]

            livetennisapi = LiveTennisApiResource(
                api_key=EnvVar("LIVETENNISAPI_KEY"),
            )

    Access to the Live Tennis API is tiered (FREE / BASIC / PRO / ULTRA); the
    free tier is keyed and allows 30 requests/minute and 100 requests/day. A
    call above the key's tier fails with ``livetennisapi.UpgradeRequired``
    (HTTP 403), never with silently degraded data.
    """

    api_key: str | None = Field(
        default=None,
        description=(
            "Live Tennis API key. Prefer passing dagster.EnvVar('LIVETENNISAPI_KEY'). "
            "If unset, the SDK falls back to the LIVETENNISAPI_KEY environment "
            "variable."
        ),
    )
    base_url: str | None = Field(
        default=None,
        description=(
            "Override the API base URL. Defaults to the SDK's production URL "
            "(or the LIVETENNISAPI_BASE_URL environment variable)."
        ),
    )
    timeout: float = Field(
        default=30.0,
        description="Per-request timeout in seconds.",
    )
    max_retries: int = Field(
        default=2,
        description="Automatic retries for transient failures (matches the SDK default).",
    )

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return False

    @contextmanager
    def get_client(self) -> Generator[LiveTennisAPI, None, None]:
        """Yield a configured ``LiveTennisAPI`` client, closing it afterwards."""
        client = LiveTennisAPI(
            api_key=self.api_key,
            base_url=self.base_url,
            timeout=self.timeout,
            max_retries=self.max_retries,
        )
        try:
            yield client
        finally:
            client.close()
