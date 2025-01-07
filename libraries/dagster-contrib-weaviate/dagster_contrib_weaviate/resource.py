from contextlib import contextmanager
from typing import Any, Dict, Optional, Generator, Union
from pydantic import Field

import weaviate

from dagster import ConfigurableResource
from dagster._utils.backoff import backoff

from .config import LocalConfig, CloudConfig


class WeaviateResource(ConfigurableResource):
    """Resource for interacting with a Weaviate database.
    
    Examples:
        .. code-block:: python
            from dagster import Definitions, asset
            from dagster_contrib_weaviate import WeaviateResource, LocalConfig

            @asset
            def my_table(weaviate: WeaviateResource):
                with weaviate.get_client() as weaviate_client:
                    questions = weaviate_client.collections.get("Question")
                    questions.query.near_text(query="biology", limit=2)

            defs = Definitions(
                assets=[my_table],
                resources={
                    "weaviate": WeaviateResource(
                        connection_config=LocalConfig(
                            host="192.168.0.10", 
                            port=8080, 
                        )
                    ),
                },
            )

        .. code-block:: python
            from dagster import Definitions, asset
            from dagster_contrib_weaviate import WeaviateResource, CloudConfig

            @asset
            def my_table(weaviate: WeaviateResource):
                with weaviate.get_client() as weaviate_client:
                    questions = weaviate_client.collections.get("Question")
                    questions.query.near_text(query="biology", limit=2)

            defs = Definitions(
                assets=[my_table],
                resources={
                    "weaviate": WeaviateResource(
                        connection_config=CloudConfig(
                            cluster_url=wcd_url
                        ),
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

    connection_config: Union[LocalConfig, CloudConfig] = Field(
        discriminator="provider",
        description=("Specifies whether to connect to a local (self-hosted) instance,"
                     " or a Weaviate cloud instance. Use LocalConfig or CloudConfig, respectively")
    )

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
    
    @contextmanager
    def get_client(self) -> Generator[weaviate.WeaviateClient, None, None]:
        if self.connection_config.provider == "local": # LocalConfig
            conn = backoff(
                fn=weaviate.connect_to_local,
                retry_on=(weaviate.exceptions.WeaviateConnectionError,),
                kwargs={
                    "host": self.connection_config.host,
                    "port": self.connection_config.port,
                    "grpc_port": self.connection_config.grpc_port,
                    "headers": self.headers,
                    "skip_init_checks": self.skip_init_checks,
                    "auth_credentials": self._weaviate_auth_credentials(),
                },
                max_retries=10,
            )
        elif self.connection_config.provider == "cloud": # CloudConfig
            conn = backoff(
                fn=weaviate.connect_to_weaviate_cloud,
                retry_on=(weaviate.exceptions.WeaviateConnectionError,),
                kwargs={
                    "cluster_url": self.connection_config.cluster_url,
                    "headers": self.headers,
                    "skip_init_checks": self.skip_init_checks,
                    "auth_credentials": self._weaviate_auth_credentials(),
                },
                max_retries=10,
            )
        else:
            raise Exception("Invalid connection_config type - use one of LocalConfig/CloudConfig")

        try:
            yield conn
        finally:
            conn.close()
