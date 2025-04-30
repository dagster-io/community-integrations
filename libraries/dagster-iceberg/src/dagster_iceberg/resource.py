from dagster import ConfigurableResource
from dagster._annotations import public
from pydantic import Field
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table

from dagster_iceberg._utils import preview
from dagster_iceberg.config import IcebergCatalogConfig


@public
@preview
class IcebergTableResource(ConfigurableResource):
    """Resource for interacting with a PyIceberg table.

    Example:
        .. code-block:: python

            from dagster import Definitions, asset
            from dagster_iceberg import IcebergTableResource


            @asset
            def my_table(iceberg_table: IcebergTableResource):
                df = iceberg_table.load().to_pandas()


            warehouse_path = "/path/to/warehouse"

            defs = Definitions(
                assets=[my_table],
                resources={
                    "iceberg_table": IcebergTableResource(
                        name="my_catalog",
                        config=IcebergCatalogConfig(
                            properties={
                                "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
                                "warehouse": f"file://{warehouse_path}",
                            }
                        ),
                        table="my_table",
                        namespace="my_namespace",
                    )
                },
            )

    """

    name: str = Field(description="The name of the Iceberg catalog.")
    config: IcebergCatalogConfig = Field(
        description="Additional configuration properties for the Iceberg catalog.",
    )
    table: str = Field(
        description="Name of the Iceberg table to interact with.",
    )
    schema_: str | None = Field(
        default=None,
        alias="namespace",
        description="Name of the Iceberg catalog namespace to use.",
    )  # schema is a reserved word for pydantic
    snapshot_id: int | None = Field(
        default=None,
        description="Snapshot ID that you would like to load. Default is latest.",
    )

    def load(self) -> Table:
        config_ = self.config.model_dump()
        catalog = load_catalog(name=self.name, **config_["properties"])
        return catalog.load_table(identifier=f"{self.schema_}.{self.table}")
