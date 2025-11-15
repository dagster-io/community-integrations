import pandas as pd
from dagster import Definitions, asset

from dagster_iceberg.config import IcebergBranchConfig, IcebergCatalogConfig
from dagster_iceberg.io_manager.pandas import PandasIcebergIOManager

CATALOG_URI = "sqlite:////home/vscode/workspace/.tmp/examples/branching/catalog.db"
CATALOG_WAREHOUSE = (
    "file:///home/vscode/workspace/.tmp/examples/branching/warehouse"
)


# Configure the IO manager to create a branch from a specific snapshot
# The branch will be based on snapshot ID 123456789
resources = {
    "io_manager": PandasIcebergIOManager(
        name="test",
        config=IcebergCatalogConfig(
            properties={"uri": CATALOG_URI, "warehouse": CATALOG_WAREHOUSE},
            branch_config=IcebergBranchConfig(
                branch_name="experimental",
                ref_snapshot_id=123456789,
            ),
        ),
        namespace="dagster",
    )
}


@asset
def iris_dataset() -> pd.DataFrame:
    return pd.read_csv(
        "https://docs.dagster.io/assets/iris.csv",
        names=[
            "sepal_length_cm",
            "sepal_width_cm",
            "petal_length_cm",
            "petal_width_cm",
            "species",
        ],
    )


defs = Definitions(assets=[iris_dataset], resources=resources)

