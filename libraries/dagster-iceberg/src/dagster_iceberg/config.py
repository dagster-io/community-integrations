from typing import Any

from dagster import Config
from dagster._annotations import public


@public
class IcebergCatalogConfig(Config):
    """Configuration for Iceberg Catalogs. See <https://py.iceberg.apache.org/configuration/#catalogs>
    for configuration options.

    You can configure the Iceberg IO manager:

        1. Using a `.pyiceberg.yaml` configuration file. <see: https://py.iceberg.apache.org/configuration/#setting-configuration-values>.
        2. Through environment variables.
        3. Using the `IcebergCatalogConfig` configuration object.

    For more information about the first two configuration options, see
    <https://py.iceberg.apache.org/configuration/#setting-configuration-values>

    Example:

    ```python
    from dagster_iceberg.config import IcebergCatalogConfig
    from dagster_iceberg.io_manager.arrow import PyArrowIcebergIOManager

    warehouse_path = "/path/to/warehouse

    io_manager = PyArrowIcebergIOManager(
        name=catalog_name,
        config=IcebergCatalogConfig(properties={
            "uri": f"sqlite:///{warehouse_path}/iceberg_catalog.db",
            "warehouse": f"file://{warehouse_path}",
        }),
        namespace=namespace,
    )
    ```
    """

    properties: dict[str, Any]
