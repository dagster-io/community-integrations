from typing import Any

from dagster import Config
from dagster._annotations import public

from dagster_iceberg._utils import preview


@public
@preview
class IcebergCatalogConfig(Config):
    """Configuration for Iceberg Catalogs.

    See the `Catalogs section <https://py.iceberg.apache.org/configuration/#catalogs>`_
    for configuration options.

    You can configure the Iceberg IO manager:

        1. Using a ``.pyiceberg.yaml`` configuration file.
        2. Through environment variables.
        3. Using the ``IcebergCatalogConfig`` configuration object.

    For more information about the first two configuration options, see
    `Setting Configuration Values <https://py.iceberg.apache.org/configuration/#setting-configuration-values>`_.

    Example:
        .. code-block:: python

            from dagster_iceberg.config import IcebergCatalogConfig
            from dagster_iceberg.io_manager.arrow import PyArrowIcebergIOManager

            warehouse_path = "/path/to/warehouse"

            io_manager = PyArrowIcebergIOManager(
                name="my_catalog",
                config=IcebergCatalogConfig(
                    properties={
                        "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
                        "warehouse": f"file://{warehouse_path}",
                    }
                ),
                namespace="my_namespace",
            )

    """

    properties: dict[str, Any]
