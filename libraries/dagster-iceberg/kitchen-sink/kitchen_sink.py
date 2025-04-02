from datetime import date

import dagster as dg
import polars as pl
from dagster_polars import PolarsParquetIOManager
from dagster_pyspark import PySparkResource
from pyspark.sql.connect.dataframe import DataFrame

from dagster_iceberg.config import IcebergCatalogConfig
from dagster_iceberg.io_manager.polars import IcebergPolarsIOManager
from dagster_iceberg.io_manager.spark import SparkIcebergIOManager

NUM_PARTS = 2  # TODO(deepyaman): Make this configurable.

CATALOG_NAME = "rest"
NAMESPACE = "nyc"

STATIC_PARTITION_KEY = "part"
DAILY_PARTITION_KEY = "date"

static_partitions = dg.StaticPartitionsDefinition(
    [f"part.{i}" for i in range(NUM_PARTS)]
)
raw_nyc_taxi_data = dg.AssetSpec(
    key="nyc.parquet",
    partitions_def=static_partitions,
).with_io_manager_key("polars_parquet_io_manager")

daily_partitions = dg.DailyPartitionsDefinition(start_date="2015-01-01")
multi_partitions = dg.MultiPartitionsDefinition(
    {"daily": daily_partitions, "static": static_partitions}
)


@dg.asset(
    ins={
        "raw_nyc_taxi_data": dg.AssetIn(
            "nyc.parquet",
            partition_mapping=dg.AllPartitionMapping(),
        ),
    },
    io_manager_key="iceberg_polars_io_manager",
    group_name="polars",
)
def combined_nyc_taxi_data(raw_nyc_taxi_data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    return pl.concat(raw_nyc_taxi_data.values())


@dg.asset(io_manager_key="iceberg_polars_io_manager", group_name="polars")
def reloaded_nyc_taxi_data(combined_nyc_taxi_data: pl.LazyFrame) -> None:
    print(combined_nyc_taxi_data.describe())  # noqa: T201


@dg.asset(
    ins={"raw_nyc_taxi_data": dg.AssetIn("nyc.parquet")},
    metadata={"partition_expr": STATIC_PARTITION_KEY},
    io_manager_key="iceberg_polars_io_manager",
    partitions_def=static_partitions,
    group_name="polars",
)
def static_partitioned_nyc_taxi_data(
    context: dg.AssetExecutionContext, raw_nyc_taxi_data: pl.LazyFrame
) -> pl.LazyFrame:
    return raw_nyc_taxi_data.with_columns(
        pl.lit(context.partition_key).alias(STATIC_PARTITION_KEY)
    )


@dg.asset(
    metadata={"partition_expr": STATIC_PARTITION_KEY},
    io_manager_key="iceberg_polars_io_manager",
    partitions_def=static_partitions,
    group_name="polars",
)
def reloaded_static_partitioned_nyc_taxi_data(
    static_partitioned_nyc_taxi_data: pl.LazyFrame,
) -> None:
    print(static_partitioned_nyc_taxi_data.describe())  # noqa: T201


@dg.asset(
    ins={"raw_nyc_taxi_data": dg.AssetIn("nyc.parquet")},
    metadata={
        "partition_expr": {"daily": DAILY_PARTITION_KEY, "static": STATIC_PARTITION_KEY}
    },
    io_manager_key="iceberg_polars_io_manager",
    partitions_def=multi_partitions,
    group_name="polars",
)
def multi_partitioned_nyc_taxi_data(
    context: dg.AssetExecutionContext, raw_nyc_taxi_data: pl.LazyFrame
) -> pl.LazyFrame:
    keys_by_dimension: dg.MultiPartitionKey = context.partition_key.keys_by_dimension
    return raw_nyc_taxi_data.with_columns(
        pl.col("tpep_pickup_datetime").dt.date().alias(DAILY_PARTITION_KEY),
        pl.lit(keys_by_dimension["static"]).alias(STATIC_PARTITION_KEY),
    ).filter(
        pl.col(DAILY_PARTITION_KEY) == date.fromisoformat(keys_by_dimension["daily"])
    )


@dg.asset(
    metadata={
        "partition_expr": {"daily": DAILY_PARTITION_KEY, "static": STATIC_PARTITION_KEY}
    },
    io_manager_key="iceberg_polars_io_manager",
    partitions_def=multi_partitions,
    group_name="polars",
)
def reloaded_multi_partitioned_nyc_taxi_data(
    multi_partitioned_nyc_taxi_data: pl.LazyFrame,
) -> None:
    print(multi_partitioned_nyc_taxi_data.describe())  # noqa: T201


@dg.asset(
    deps=["combined_nyc_taxi_data"],
    io_manager_key="spark_iceberg_io_manager",
    group_name="spark",
)
def combined_nyc_taxi_data_spark(pyspark: PySparkResource) -> DataFrame:
    spark = pyspark.spark_session
    return spark.table(f"{CATALOG_NAME}.{NAMESPACE}.combined_nyc_taxi_data")


@dg.asset(io_manager_key="spark_iceberg_io_manager", group_name="spark")
def reloaded_nyc_taxi_data_spark(combined_nyc_taxi_data_spark: DataFrame) -> None:
    combined_nyc_taxi_data_spark.describe().show()


@dg.asset(
    deps=["static_partitioned_nyc_taxi_data"],
    metadata={"partition_expr": STATIC_PARTITION_KEY},
    io_manager_key="spark_iceberg_io_manager",
    partitions_def=static_partitions,
    group_name="spark",
)
def static_partitioned_nyc_taxi_data_spark(
    context: dg.AssetExecutionContext, pyspark: PySparkResource
) -> DataFrame:
    spark = pyspark.spark_session
    df = spark.table(f"{CATALOG_NAME}.{NAMESPACE}.static_partitioned_nyc_taxi_data")
    return df.filter(df[STATIC_PARTITION_KEY] == context.partition_key)


@dg.asset(
    metadata={"partition_expr": STATIC_PARTITION_KEY},
    io_manager_key="spark_iceberg_io_manager",
    partitions_def=static_partitions,
    group_name="spark",
)
def reloaded_static_partitioned_nyc_taxi_data_spark(
    static_partitioned_nyc_taxi_data_spark: DataFrame,
) -> None:
    static_partitioned_nyc_taxi_data_spark.describe().show()


catalog_config = IcebergCatalogConfig(
    properties={
        "type": "rest",
        "uri": "http://localhost:8181",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
    },
)


defs = dg.Definitions(
    assets=[
        raw_nyc_taxi_data,
        combined_nyc_taxi_data,
        reloaded_nyc_taxi_data,
        static_partitioned_nyc_taxi_data,
        reloaded_static_partitioned_nyc_taxi_data,
        multi_partitioned_nyc_taxi_data,
        reloaded_multi_partitioned_nyc_taxi_data,
        combined_nyc_taxi_data_spark,
        reloaded_nyc_taxi_data_spark,
        static_partitioned_nyc_taxi_data_spark,
        reloaded_static_partitioned_nyc_taxi_data_spark,
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
