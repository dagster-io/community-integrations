"""
NB: This snippet assumes that an iceberg table called 'ingested_data' exists.
"""

import pandas as pd
from dagster import Definitions, asset

from dagster_iceberg.config import IcebergCatalogConfig
from dagster_iceberg.resource import IcebergTableResource

CATALOG_URI = "sqlite:////home/vscode/workspace/.tmp/examples/catalog.db"
CATALOG_WAREHOUSE = "file:///home/vscode/workspace/.tmp/examples/warehouse"


@asset
def small_petals(iceberg: IcebergTableResource) -> pd.DataFrame:
    return iceberg.load().scan().to_pandas()


defs = Definitions(
    assets=[small_petals],
    resources={
        "iceberg": IcebergTableResource(
            name="test",
            config=IcebergCatalogConfig(
                properties={"uri": CATALOG_URI, "warehouse": CATALOG_WAREHOUSE}
            ),
            namespace="dagster",
            table="ingested_data",
        )
    },
)
