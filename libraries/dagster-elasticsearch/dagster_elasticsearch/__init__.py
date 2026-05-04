from dagster._core.libraries import DagsterLibraryRegistry

from dagster_elasticsearch.config import (
    BaseConnectionConfig,
    CloudConfig,
    HostsConfig,
)
from dagster_elasticsearch.io_manager import ElasticsearchIOManager
from dagster_elasticsearch.resource import ElasticsearchResource

__all__ = [
    "BaseConnectionConfig",
    "CloudConfig",
    "ElasticsearchIOManager",
    "ElasticsearchResource",
    "HostsConfig",
]
__version__ = "0.0.1"

DagsterLibraryRegistry.register("dagster-elasticsearch", __version__, is_dagster_package=False)
