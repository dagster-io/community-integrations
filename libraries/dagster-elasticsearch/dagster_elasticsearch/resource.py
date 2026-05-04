from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from dagster import ConfigurableResource
from elasticsearch import Elasticsearch
from pydantic import Field

from .config import BaseConnectionConfig


class ElasticsearchResource(ConfigurableResource):
    """Resource for interacting with an Elasticsearch 9.x cluster.

    Examples:
        .. code-block:: python

            from dagster import Definitions, asset
            from dagster_elasticsearch import ElasticsearchResource, HostsConfig

            @asset
            def index_docs(es: ElasticsearchResource):
                with es.get_client() as client:
                    client.index(index="my-index", document={"title": "hello"})
                    client.indices.refresh(index="my-index")

            defs = Definitions(
                assets=[index_docs],
                resources={
                    "es": ElasticsearchResource(
                        connection_config=HostsConfig(hosts=["http://localhost:9200"]),
                    ),
                },
            )
    """

    connection_config: BaseConnectionConfig = Field(
        description="Connection configuration. Use HostsConfig or CloudConfig.",
    )
    request_timeout: float | None = Field(
        default=None,
        description="Per-request timeout in seconds. Defaults to elasticsearch-py default.",
    )
    additional_client_kwargs: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional kwargs forwarded to the Elasticsearch client constructor.",
    )

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return False

    def _build_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = self.connection_config.to_client_kwargs()
        if self.request_timeout is not None:
            kwargs["request_timeout"] = self.request_timeout
        kwargs.update(self.additional_client_kwargs)
        return kwargs

    @contextmanager
    def get_client(self) -> Generator[Elasticsearch, None, None]:
        client = Elasticsearch(**self._build_kwargs())
        try:
            yield client
        finally:
            client.close()
