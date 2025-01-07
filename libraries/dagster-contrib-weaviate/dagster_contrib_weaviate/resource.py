from contextlib import contextmanager
from typing import Any, Dict, Optional, Generator

import weaviate

from dagster import ConfigurableResource
from dagster._utils.backoff import backoff
from pydantic import Field


class WeaviateBaseResource(ConfigurableResource):
    headers: Optional[Dict[str, str]] = Field(
        description=(
            "Additional headers to include in the requests,"
            " e.g. API keys for Cloud vectorization"
        ),
        default=None,
    )

    skip_init_checks: bool = Field(
        description=(
            "Whether to skip the initialization checks when connecting to Weaviate"
        ),
        default=False,
    )

    auth_credentials: Dict[str, Any] = Field(
        description=(
            "A dictionary containing the credentials to use for authentication with your"
            " Weaviate instance. You may provide any of the following options:"
            " api_key / access_token (bearer_token) / username+password / client_secret"
            " See https://weaviate.io/developers/weaviate/configuration/authentication for more info."
        ),
        default={},
    )

    def _weaviate_auth_credentials(self) -> Optional[weaviate.auth.AuthCredentials]:
        """Converts the auth_credentials config dict from the user, to the Weaviate AuthCredentials
        class that can be passed to the WeaviateClient constructor."""

        if len(self.auth_credentials) == 0:
            return None

        if "api_key" in self.auth_credentials:
            return weaviate.classes.init.Auth.api_key(**self.auth_credentials)

        if "access_token" in self.auth_credentials:
            return weaviate.classes.init.Auth.bearer_token(**self.auth_credentials)

        if "username" in self.auth_credentials:
            return weaviate.classes.init.Auth.client_password(**self.auth_credentials)

        if "client_secret" in self.client_credentials:
            return weaviate.classes.init.Auth.client_credentials(
                **self.auth_credentials
            )

        raise Exception(
            "One of the following must be provided in auth_credentials configuration:"
            " api_key / access_token / username+password / client_secret"
        )


class WeaviateCloudResource(WeaviateBaseResource):
    """Resource for interacting with a Weaviate Cloud database.

    Use WeaviateLocalResource to interact with a self hosted Weaviate resource.

    Examples:
        .. code-block:: python

            from dagster import Definitions, asset
            from dagster_contrib_weaviate import WeaviateCloudResource

            @asset
            def my_table(weaviate: WeaviateCloudResource):
                with weaviate.get_client() as weaviate_client:
                    questions = weaviate_client.collections.get("Question")
                    questions.query.near_text(query="biology", limit=2)

            defs = Definitions(
                assets=[my_table],
                resources={
                    "weaviate": WeaviateCloudResource(
                        cluster_url=wcd_url,
                        auth_credentials={
                            "api_key": wcd_apikey
                        },
                        headers={
                            "X-Cohere-Api-Key": cohere_apikey,
                        }
                    ),
                },
            )

    """

    cluster_url: str = Field(
        description=(
            "The WCD cluster URL or hostname to connect to. Usually in the form: rAnD0mD1g1t5.something.weaviate.cloud"
        )
    )

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return False

    @contextmanager
    def get_client(self) -> Generator[weaviate.WeaviateClient, None, None]:
        conn = backoff(
            fn=weaviate.connect_to_weaviate_cloud,
            retry_on=(weaviate.exceptions.WeaviateConnectionError,),
            kwargs={
                "cluster_url": self.cluster_url,
                "headers": self.headers,
                "skip_init_checks": self.skip_init_checks,
                "auth_credentials": self._weaviate_auth_credentials(),
            },
            max_retries=10,
        )

        try:
            yield conn
        finally:
            conn.close()


class WeaviateLocalResource(WeaviateBaseResource):
    """Resource for interacting with a local (self-hosted) Weaviate database.

    Use WeaviateCloudResource to interact with a Weaviate Cloud instance.

    Examples:
        .. code-block:: python

            from dagster import Definitions, asset
            from dagster_contrib_weaviate import WeaviateLocalResource

            @asset
            def my_table(weaviate: WeaviateLocalResource):
                with weaviate.get_client() as weaviate_client:
                    questions = weaviate_client.collections.get("Question")
                    questions.query.near_text(query="biology", limit=2)

            defs = Definitions(
                assets=[my_table],
                resources={"weaviate": WeaviateLocalResource(host="192.168.0.10")}
            )

    """

    host: str = Field(
        description=("The host to use for the underlying REST and GraphQL API calls"),
        default="localhost",
    )

    port: int = Field(
        description=("The port to use for the underlying REST and GraphQL API calls"),
        default=8080,
    )

    grpc_port: int = Field(
        description=("The port to use for the underlying gRPC API"), default=50051
    )

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return False

    @contextmanager
    def get_client(self) -> Generator[weaviate.WeaviateClient, None, None]:
        conn = backoff(
            fn=weaviate.connect_to_local,
            retry_on=(weaviate.exceptions.WeaviateConnectionError,),
            kwargs={
                "host": self.host,
                "port": self.port,
                "grpc_port": self.grpc_port,
                "headers": self.headers,
                "skip_init_checks": self.skip_init_checks,
                "auth_credentials": self._weaviate_auth_credentials(),
            },
            max_retries=10,
        )

        try:
            yield conn
        finally:
            conn.close()
