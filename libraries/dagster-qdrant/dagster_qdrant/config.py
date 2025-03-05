from typing import Optional

from dagster import Config


class QdrantConfig(Config):
    """Parameters to set up connection to a Qdrant service."""

    location: Optional[str] = None
    """ If `":memory:"` - use in-memory Qdrant instance.
        If `str` - use it as a `url` parameter.
        If `None` - use default values for `host` and `port`.
    """
    url: Optional[str] = None
    """Either host or str of "Optional[scheme], host, Optional[port], Optional[prefix]".
    """
    port: Optional[int] = 6333
    """Port of the REST API interface.
    """
    grpc_port: int = 6334
    """Port of the gRPC interface.
    """
    prefer_grpc: bool = False
    """If `true` - use gPRC interface whenever possible in custom methods.
    """
    https: Optional[bool] = None
    """If `true` - use HTTPS(SSL) protocol. Default: `None`
    """
    api_key: Optional[str] = None
    """API key for authentication in Qdrant Cloud.
    """
    prefix: Optional[str] = None
    """ If not `None` - add `prefix` to the REST URL path.
        Example: `service/v1` will result in `http://localhost:6333/service/v1/{qdrant-endpoint}` for REST API.
        Default: `None`
    """
    timeout: Optional[int] = None
    """ Timeout for REST and gRPC API requests.
        Default: 5 seconds for REST and unlimited for gRPC
    """
    host: Optional[str] = None
    """Host name of Qdrant service. If url and host are None, set to 'localhost'.
    """
    path: Optional[str] = None
    """Persistence path for `QdrantLocal`.
    """
    force_disable_check_same_thread: bool = False
    """ For QdrantLocal, force disable check_same_thread. Default: `False`
        Only use this if you can guarantee that you can resolve the thread safety outside QdrantClient.
    """
    check_compatibility: bool = True
    """If `true` - check compatibility with the server version.
    """
