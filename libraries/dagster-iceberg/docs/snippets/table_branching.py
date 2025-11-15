import pandas as pd
from dagster import Definitions, asset

from dagster_iceberg.config import IcebergBranchConfig, IcebergCatalogConfig
from dagster_iceberg.io_manager.pandas import PandasIcebergIOManager

CATALOG_URI = "sqlite:////home/vscode/workspace/.tmp/examples/branching/catalog.db"
CATALOG_WAREHOUSE = (
    "file:///home/vscode/workspace/.tmp/examples/branching/warehouse"
)


# Configure the IO manager to use a branch named "dev"
resources = {
    "io_manager": PandasIcebergIOManager(
        name="test",
        config=IcebergCatalogConfig(
            properties={"uri": CATALOG_URI, "warehouse": CATALOG_WAREHOUSE},
            branch_config=IcebergBranchConfig(branch_name="dev"),
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


@asset
def processed_iris(iris_dataset: pd.DataFrame) -> pd.DataFrame:
    # This asset reads from iris_dataset on the "dev" branch
    # and writes back to the "dev" branch
    return iris_dataset[iris_dataset["species"] == "Iris-setosa"]


defs = Definitions(assets=[iris_dataset, processed_iris], resources=resources)

