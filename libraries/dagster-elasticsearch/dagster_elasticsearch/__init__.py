from dagster._core.libraries import DagsterLibraryRegistry

from dagster_elasticsearch.config import (
    BaseConnectionConfig,
    CloudConfig,
    ElasticsearchIndexConfig,
    HostsConfig,
)
from dagster_elasticsearch.errors import ElasticsearchBulkIndexError
from dagster_elasticsearch.io_manager import ElasticsearchIOManager
from dagster_elasticsearch.resource import ElasticsearchResource

__all__ = [
    "BaseConnectionConfig",
    "CloudConfig",
    "ElasticsearchBulkIndexError",
    "ElasticsearchIOManager",
    "ElasticsearchIndexConfig",
    "ElasticsearchResource",
    "HostsConfig",
]
__version__ = "0.0.1"

DagsterLibraryRegistry.register("dagster-elasticsearch", __version__, is_dagster_package=False)
