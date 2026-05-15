import abc


from dagster import ConfigurableResource

# By default, the chroma CLI runs on port 8000: https://docs.trychroma.com/cli/install-and-run
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 8000


class BaseConnectionConfig(ConfigurableResource, abc.ABC):
    pass


class LocalConfig(BaseConnectionConfig):
    """Connection parameters for a local (filesystem) database."""

    persistence_path: str | None = "./chroma"
    """The directory to save Chroma's data to. Defaults to './chroma'.
     for an in-memory database, set to None"
    """


class HttpConfig(BaseConnectionConfig):
    """Connection parameters for a Chroma HTTP server."""

    host: str = DEFAULT_HOST
    """The host of the Chroma HTTP server. Defaults to 'localhost'"""

    port: int = DEFAULT_PORT
    """The port of the Chroma server. Defaults to '8000'"""

    ssl: bool = False
    """Whether to use SSL to connect to the Chroma server. Defaults to False"""

    headers: dict[str, str] = {}
    """A dictionary of headers to send to the Chroma server. Defaults to {}"""
