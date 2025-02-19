from typing import Optional

from dagster import ConfigurableResource
from dagster._annotations import experimental, public
from pydantic import Field
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table

from dagster_iceberg.config import IcebergCatalogConfig


@public
@experimental
class IcebergTableResource(ConfigurableResource):
    """Resource for interacting with a PyIceberg table.

    Examples:

    ```python
    from dagster import Definitions, asset
    from dagster_iceberg import IcebergTableResource, LocalConfig

    @asset
    def my_table(iceberg_table: IcebergTableResource):
        df = pyiceberg_table.load().to_pandas()

    defs = Definitions(
        assets=[my_table],
        resources={
            "iceberg_table,
            IcebergTableResource(
                name="mycatalog",
                namespace="mynamespace",
                table="mytable",
                config=IcebergCatalogConfig(properties={
                    "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
                    "warehouse": f"file://{warehouse_path}",
                }),
            )
        }
    )
    ```
    """

    name: str = Field(description="The name of the iceberg catalog.")
    config: IcebergCatalogConfig = Field(
        description="Additional configuration properties for the iceberg catalog.",
    )
    table: str = Field(
        description="Name of the iceberg table to interact with.",
    )
    schema_: Optional[str] = Field(
        default=None,
        alias="namespace",
        description="Name of the iceberg catalog namespace to use.",
    )  # schema is a reserved word for pydantic
    snapshot_id: Optional[int] = Field(
        default=None,
        description="Snapshot ID that you would like to load. Default is latest.",
    )

    def load(self) -> Table:
        config_ = self.config.model_dump()
        catalog = load_catalog(name=self.name, **config_["properties"])
        return catalog.load_table(identifier="%s.%s" % (self.schema_, self.table))
