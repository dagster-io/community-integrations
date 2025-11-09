from dagster import Config


class QdrantConfig(Config):
    """Parameters to set up connection to a Qdrant service."""

    location: str | None = None
    """ If `":memory:"` - use in-memory Qdrant instance.
        If `str` - use it as a `url` parameter.
        If `None` - use default values for `host` and `port`.
    """
    url: str | None = None
    """Either host or str of "Optional[scheme], host, Optional[port], Optional[prefix]".
    """
    port: int | None = 6333
    """Port of the REST API interface.
    """
    grpc_port: int = 6334
    """Port of the gRPC interface.
    """
    prefer_grpc: bool = False
    """If `true` - use gPRC interface whenever possible in custom methods.
    """
    https: bool | None = None
    """If `true` - use HTTPS(SSL) protocol. Default: `None`
    """
    api_key: str | None = None
    """API key for authentication in Qdrant Cloud.
    """
    prefix: str | None = None
    """ If not `None` - add `prefix` to the REST URL path.
        Example: `service/v1` will result in `http://localhost:6333/service/v1/{qdrant-endpoint}` for REST API.
        Default: `None`
    """
    timeout: int | None = None
    """ Timeout for REST and gRPC API requests.
        Default: 5 seconds for REST and unlimited for gRPC
    """
    host: str | None = None
    """Host name of Qdrant service. If url and host are None, set to 'localhost'.
    """
    path: str | None = None
    """Persistence path for `QdrantLocal`.
    """
    force_disable_check_same_thread: bool = False
    """ For QdrantLocal, force disable check_same_thread. Default: `False`
        Only use this if you can guarantee that you can resolve the thread safety outside QdrantClient.
    """
    check_compatibility: bool = True
    """If `true` - check compatibility with the server version.
    """
