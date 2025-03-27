import polars as pl
from dagster import (
    AllPartitionMapping,
    AssetIn,
    AssetSpec,
    Definitions,
    StaticPartitionsDefinition,
    asset,
)
from dagster_polars import PolarsParquetIOManager
from dagster_pyspark import PySparkResource
from pyspark.sql.connect.dataframe import DataFrame

from dagster_iceberg.config import IcebergCatalogConfig
from dagster_iceberg.io_manager.polars import IcebergPolarsIOManager
from dagster_iceberg.io_manager.spark import SparkIcebergIOManager

NUM_PARTS = 2  # TODO(deepyaman): Make this configurable.

CATALOG_NAME = "rest"
NAMESPACE = "nyc"

parts = StaticPartitionsDefinition([f"part.{i}" for i in range(NUM_PARTS)])
raw_nyc_taxi_data = AssetSpec(
    key="nyc.parquet",
    partitions_def=parts,
).with_io_manager_key("polars_parquet_io_manager")


@asset(
    ins={
        "raw_nyc_taxi_data": AssetIn(
            "nyc.parquet",
            partition_mapping=AllPartitionMapping(),
        ),
    },
    io_manager_key="iceberg_polars_io_manager",
)
def combined_nyc_taxi_data(raw_nyc_taxi_data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    return pl.concat(raw_nyc_taxi_data.values())


@asset(io_manager_key="iceberg_polars_io_manager")
def reloaded_nyc_taxi_data(combined_nyc_taxi_data: pl.LazyFrame) -> None:
    print(combined_nyc_taxi_data.describe())  # noqa: T201


@asset(deps=["combined_nyc_taxi_data"], io_manager_key="spark_iceberg_io_manager")
def combined_nyc_taxi_data_spark(pyspark: PySparkResource) -> DataFrame:
    spark = pyspark.spark_session
    return spark.table(f"{CATALOG_NAME}.{NAMESPACE}.combined_nyc_taxi_data")


@asset(io_manager_key="spark_iceberg_io_manager")
def reloaded_nyc_taxi_data_spark(combined_nyc_taxi_data_spark: DataFrame) -> None:
    combined_nyc_taxi_data_spark.describe().show()


catalog_config = IcebergCatalogConfig(
    properties={
        "type": "rest",
        "uri": "http://localhost:8181",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
    },
)


defs = Definitions(
    assets=[
        raw_nyc_taxi_data,
        combined_nyc_taxi_data,
        reloaded_nyc_taxi_data,
        combined_nyc_taxi_data_spark,
        reloaded_nyc_taxi_data_spark,
    ],
    resources={
        "polars_parquet_io_manager": PolarsParquetIOManager(
            base_dir="https://storage.googleapis.com/anaconda-public-data/nyc-taxi/",
        ),
        "iceberg_polars_io_manager": IcebergPolarsIOManager(
            name=CATALOG_NAME,
            config=catalog_config,
            namespace=NAMESPACE,
        ),
        "pyspark": PySparkResource(spark_config={"spark.remote": "sc://localhost"}),
        "spark_iceberg_io_manager": SparkIcebergIOManager(
            catalog_name=CATALOG_NAME,
            namespace=NAMESPACE,
            remote_url="sc://localhost",
        ),
    },
)
