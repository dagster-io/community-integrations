from dagster._core.libraries import DagsterLibraryRegistry

from dagster_qdrant.config import QdrantConfig
from dagster_qdrant.resource import QdrantResource

__all__ = ["QdrantConfig", "QdrantResource"]
__version__ = "0.0.1"

DagsterLibraryRegistry.register("dagster-qdrant", __version__)
