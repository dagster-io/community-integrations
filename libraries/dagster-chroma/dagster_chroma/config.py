from typing import Literal, Optional, Dict


from dagster import Config

# By default, the chroma CLI runs on port 8000: https://docs.trychroma.com/cli/install-and-run
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 8000


class LocalConfig(Config):
    """Connection parameters for a local (filesystem) database."""

    provider: Literal["local"] = "local"

    persistence_path: Optional[str] = "./chroma"
    """The directory to save Chroma's data to. Defaults to './chroma'.
     for an in-memory database, set to None"
    """


class HttpConfig(Config):
    """Connection parameters for a Chroma HTTP server."""

    provider: Literal["http"] = "http"

    host: str = DEFAULT_HOST
    """The host of the Chroma HTTP server. Defaults to 'localhost'"""

    port: int = DEFAULT_PORT
    """The port of the Chroma server. Defaults to '8000'"""

    ssl: bool = False
    """Whether to use SSL to connect to the Chroma server. Defaults to False"""

    headers: Dict[str, str] = {}
    """A dictionary of headers to send to the Chroma server. Defaults to {}"""
