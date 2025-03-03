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

CATALOG_NAME = "local"
NAMESPACE = "demo.nyc"

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
def combined_nyc_taxi_data(raw_nyc_taxi_data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    return pl.concat(raw_nyc_taxi_data.values())


@asset(io_manager_key="iceberg_polars_io_manager")
def reloaded_nyc_taxi_data(combined_nyc_taxi_data: pl.LazyFrame) -> None:
    print(combined_nyc_taxi_data.describe())


catalog_config = IcebergCatalogConfig(
    properties={
        "type": "rest",
        "uri": "http://localhost:8181",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
    }
)


defs = Definitions(
    assets=[raw_nyc_taxi_data, combined_nyc_taxi_data, reloaded_nyc_taxi_data],
    resources={
        "polars_parquet_io_manager": PolarsParquetIOManager(
            base_dir="https://storage.googleapis.com/anaconda-public-data/nyc-taxi/"
        ),
        "iceberg_polars_io_manager": IcebergPolarsIOManager(
            name=CATALOG_NAME, config=catalog_config, namespace=NAMESPACE
        ),
    },
)
