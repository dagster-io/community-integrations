from dagster._core.libraries import DagsterLibraryRegistry

from dagster_qdrant.config import QdrantConfig
from dagster_qdrant.resource import QdrantResource

__all__ = ["QdrantConfig", "QdrantResource"]
__version__ = "0.0.3"

DagsterLibraryRegistry.register("dagster-qdrant", __version__, is_dagster_package=False)
