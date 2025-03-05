from contextlib import contextmanager
from typing import Generator

from dagster import ConfigurableResource
from pydantic import Field
from qdrant_client import QdrantClient

from .config import QdrantConfig


class QdrantResource(ConfigurableResource):
    config: QdrantConfig = Field(
        description=(
            """Parameters to set up connection to a Qdrant service.
            """
        ),
    )

    @classmethod
    def _is_dagster_mainatained(cls) -> bool:
        return False

    @contextmanager
    def get_client(self) -> Generator[QdrantClient, None, None]:
        conn = QdrantClient(**self.config.model_dump())
        yield conn

        conn.close()
