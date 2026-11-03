from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from adanos import AdanosClient
from dagster import ConfigurableResource, InitResourceContext
from dagster._annotations import public
from pydantic import Field, PrivateAttr


@public
class AdanosResource(ConfigurableResource):
    """Dagster resource for the Adanos Market Sentiment API.

    The resource owns an official :class:`adanos.AdanosClient` for the duration
    of a Dagster run. Use ``get_client`` inside assets and ops to access the
    SDK's Reddit, X, News, Polymarket, and crypto namespaces.
    """

    api_key: str = Field(
        description="Adanos API key. Create one at https://adanos.org/register."
    )
    base_url: str = Field(
        default="https://api.adanos.org",
        description="Adanos API base URL.",
    )
    timeout: float = Field(
        default=30.0,
        gt=0,
        description="HTTP request timeout in seconds.",
    )

    _client: AdanosClient = PrivateAttr()

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return False

    def setup_for_execution(self, _context: InitResourceContext) -> None:
        self._client = AdanosClient(
            api_key=self.api_key,
            base_url=self.base_url,
            timeout=self.timeout,
        )
        self._client.__enter__()

    def teardown_after_execution(self, _context: InitResourceContext) -> None:
        self._client.close()

    @public
    def get_client(self) -> AdanosClient:
        """Return the configured Adanos SDK client for this Dagster run."""

        return self._client

    @public
    @asynccontextmanager
    async def get_async_client(self) -> AsyncIterator[AdanosClient]:
        """Yield an async SDK client managed on the caller's event loop."""

        client = AdanosClient(
            api_key=self.api_key,
            base_url=self.base_url,
            timeout=self.timeout,
        )
        async with client:
            yield client
