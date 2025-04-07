# Quickstart

!!! warning "Iceberg catalog"

    PyIceberg requires a catalog backend. A SQLite catalog is used here for illustrative purposes. Do not use this in a production setting.

## Step 1: Defining the I/O manager

To use dagster-iceberg as an I/O manager, you add it to your `Definition`:

```py linenums="1"
from dagster import Definitions
from dagster_iceberg.config import IcebergCatalogConfig
from dagster_iceberg.io_manager.arrow import PyArrowIcebergIOManager

CATALOG_URI = "sqlite:////home/vscode/workspace/.tmp/dag/warehouse/catalog.db"
CATALOG_WAREHOUSE = "file:///home/vscode/workspace/.tmp/dag/warehouse"


resources = {
    "io_manager": PyArrowIcebergIOManager(
        name="test",
        config=IcebergCatalogConfig(
            properties={"uri": CATALOG_URI, "warehouse": CATALOG_WAREHOUSE}
        ),
        namespace="dagster",
    )
}


defs = Definitions(
    assets=[iris_dataset],
    resources=resources
)
```

## Step 2: Store a Dagster asset as an Iceberg table

```py linenums="1"
import pandas as pd

from dagster import asset


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
```

## Step 3: Load Iceberg tables in downstream assets

Dagster and the I/O manager allow you to load the data stored in Iceberg tables into downstream assets:

```py linenums="1"
import pandas as pd

from dagster import asset

# this example uses the iris_dataset asset from Step 2

@asset
def iris_cleaned(iris_dataset: pd.DataFrame) -> pd.DataFrame:
    return iris_dataset.dropna().drop_duplicates()
```
