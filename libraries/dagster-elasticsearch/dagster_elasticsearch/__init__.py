from dagster._core.libraries import DagsterLibraryRegistry

from dagster_elasticsearch.checks import build_indexed_asset_check
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
    "build_indexed_asset_check",
]
__version__ = "0.0.2"

DagsterLibraryRegistry.register("dagster-elasticsearch", __version__, is_dagster_package=False)
