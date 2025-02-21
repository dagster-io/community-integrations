import polars as pl
from dagster import (
    AllPartitionMapping,
    AssetIn,
    AssetSpec,
    Definitions,
    StaticPartitionsDefinition,
    asset,
)
from dagster_iceberg.config import IcebergCatalogConfig
from dagster_iceberg.io_manager.polars import IcebergPolarsIOManager
from dagster_polars import PolarsParquetIOManager


NUM_PARTS = 16  # 0

CATALOG_NAME = "default"
WAREHOUSE_PATH = "/tmp/warehouse"
NAMESPACE = "default"

parts = StaticPartitionsDefinition([f"part.{i}" for i in range(NUM_PARTS)])
raw_nyc_taxi_data = AssetSpec(
    key="nyc.parquet", partitions_def=parts
).with_io_manager_key("polars_parquet_io_manager")


@asset(
    ins={
        "raw_nyc_taxi_data": AssetIn(
            "nyc.parquet", partition_mapping=AllPartitionMapping()
        )
    },
    io_manager_key="iceberg_polars_io_manager",
)
def clean_nyc_taxi_data(raw_nyc_taxi_data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    return pl.concat(raw_nyc_taxi_data.values())


catalog_config = IcebergCatalogConfig(
    properties={
        "uri": f"sqlite:///{WAREHOUSE_PATH}/iceberg_catalog.db",
        "warehouse": f"file://{WAREHOUSE_PATH}",
    }
)


defs = Definitions(
    assets=[raw_nyc_taxi_data, clean_nyc_taxi_data],
    resources={
        "polars_parquet_io_manager": PolarsParquetIOManager(
            base_dir="https://storage.googleapis.com/anaconda-public-data/nyc-taxi/"
        ),
        "iceberg_polars_io_manager": IcebergPolarsIOManager(
            name=CATALOG_NAME, config=catalog_config, namespace=NAMESPACE
        ),
    },
)
