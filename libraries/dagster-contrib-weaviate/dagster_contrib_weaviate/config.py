from typing import Literal

from dagster import Config

# These are the default ports for a local Weaviate instance. see:
# https://weaviate.io/developers/weaviate/quickstart/local#11-create-a-weaviate-database
# https://github.com/weaviate/weaviate-python-client/blob/975291de89b02de60dc0f9143dc74b38f116c5c2/weaviate/connect/helpers.py#L150
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 8080
DEFAULT_GRPC_PORT = 50051

class LocalConfig(Config):
    """Connection parameters for a local (self-hosted) database."""
    
    provider: Literal["local"] = "local"

    host: str = DEFAULT_HOST
    """The host to use for the underlying REST and GraphQL API calls. Default to localhost."""

    port: int = DEFAULT_PORT
    """The port to use for the underlying REST and GraphQL API calls.
     Defaults to 8080 (the default port a weaviate instance runs on)"""

    grpc_port: int = DEFAULT_GRPC_PORT
    """The port to use for the underlying gRPC API. 
     Defaults to 50051 (the default grpc-port a weaviate instance runs on)"""

class CloudConfig(Config):
    """Connection parameters for a cloud Weaviate database."""

    provider: Literal["cloud"] = "cloud"

    cluster_url: str 
    """The WCD cluster URL or hostname to connect to. 
    Usually in the form: rAnD0mD1g1t5.something.weaviate.cloud
    """