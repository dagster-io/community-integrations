from contextlib import contextmanager
from typing import Any
from collections.abc import Generator
from pydantic import Field

import chromadb
import chromadb.api
import chromadb.config

from dagster import ConfigurableResource
from dagster._utils.backoff import backoff

from .config import LocalConfig, HttpConfig, BaseConnectionConfig


class ChromaResource(ConfigurableResource):
    """Resource for interacting with a Chroma database.

    Examples:
        .. code-block:: python

        import os
        from dagster import Definitions, asset
        from dagster_chroma import ChromaResource, LocalConfig, HttpConfig

        @asset
        def my_table(chroma: ChromaResource):
            with chroma.get_client() as chroma_client:
                collection = chroma_client.get_collection("fruits")

                results = collection.query(
                    query_texts=["hawaii"],
                    n_results=1,
                )

        defs = Definitions(
            assets=[my_table],
            resources={
                "chroma": ChromaResource(
                    connection_config=
                        LocalConfig(persistence_path="./chroma") if os.getenv("DEV") else
                            HttpConfig(host="192.168.0.10", port=8000)
                ),
            }
        )
    """

    connection_config: BaseConnectionConfig = Field(
        description=(
            """Specified whether to connect to Chroma via HTTP, or to use a Local database
            (i.e. the library will directly access a persistence-directory on this machine)
            """
        ),
    )

    tenant: str = Field(
        description=(
            "The tenant to use for this client. Defaults to the default tenant."
        ),
        default=chromadb.config.DEFAULT_TENANT,
    )

    database: str = Field(
        description="The database to use for this client. Defaults to the default database.",
        default=chromadb.config.DEFAULT_DATABASE,
    )

    additional_settings: dict[str, Any] = Field(
        description=(
            "A dictionary of additional settings for the client. See the chromadb Settings"
            " class for a full list of allowed options:"
            " https://github.com/chroma-core/chroma/blob/088e4cca0adc5bb8fda6ade48af3cce9044dd6e6/chromadb/config.py#L100"
        ),
        default={},
    )

    @classmethod
    def _is_dagster_mainatained(cls) -> bool:
        return False

    @contextmanager
    def get_client(self) -> Generator[chromadb.api.ClientAPI, None, None]:
        chromadb_settings = chromadb.config.Settings(**self.additional_settings)

        if isinstance(self.connection_config, LocalConfig):  # LocalConfig
            if self.connection_config.persistence_path is None:
                conn = chromadb.EphemeralClient(
                    settings=chromadb_settings,
                    tenant=self.tenant,
                    database=self.database,
                )
            else:
                conn = chromadb.PersistentClient(
                    path=self.connection_config.persistence_path,
                    settings=chromadb_settings,
                    tenant=self.tenant,
                    database=self.database,
                )
        elif isinstance(self.connection_config, HttpConfig):  # HttpConfig
            conn = backoff(
                fn=chromadb.HttpClient,
                retry_on=(ValueError,),
                kwargs={
                    "host": self.connection_config.host,
                    "port": self.connection_config.port,
                    "ssl": self.connection_config.ssl,
                    "headers": self.connection_config.headers,
                    "settings": chromadb_settings,
                    "tenant": self.tenant,
                    "database": self.database,
                },
                max_retries=10,
            )
        else:
            raise Exception(
                "Invalid connection_config type - use of one LocalConfig/HttpConfig"
            )

        yield conn

        # the chromadb client doesn't support closing. we should still
        # follow the "contextmanager" convention, for consistency with other dagster resources.
