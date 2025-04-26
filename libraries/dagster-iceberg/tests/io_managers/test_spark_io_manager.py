import datetime

import docker
import pyarrow as pa
import pytest
from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    HourlyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    asset,
    materialize,
)
from pyiceberg.catalog import Catalog
from pyspark.sql import SparkSession
from pyspark.sql.connect.dataframe import DataFrame

from dagster_iceberg.io_manager.spark import SparkIcebergIOManager

SPARK_CONFIG = {
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.postgres": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.postgres.type": "jdbc",
    "spark.sql.catalog.postgres.uri": "jdbc:postgresql://postgres:5432/test",
    "spark.sql.catalog.postgres.jdbc.user": "test",
    "spark.sql.catalog.postgres.jdbc.password": "test",
    "spark.sql.catalog.postgres.warehouse": "/home/iceberg/warehouse",
    "spark.sql.defaultCatalog": "postgres",
    "spark.eventLog.enabled": "true",
    "spark.eventLog.dir": "/home/iceberg/spark-events",
    "spark.history.fs.logDirectory": "/home/iceberg/spark-events",
    "spark.sql.catalogImplementation": "in-memory",
    "spark.sql.execution.arrow.pyspark.enabled": "true",
}

pytest.skip(
    "Spark fails to create Parquet file when running on GitHub Actions",
    allow_module_level=True,
)


@pytest.fixture
def io_manager(
    catalog_name: str,
    namespace: str,
) -> SparkIcebergIOManager:
    return SparkIcebergIOManager(
        catalog_name=catalog_name,
        namespace=namespace,
        spark_config=SPARK_CONFIG,
        remote_url="sc://localhost",
    )


# NB: iceberg table identifiers are namespace + asset names (see below)
@pytest.fixture
def asset_b_df_table_identifier(namespace: str) -> str:
    return f"{namespace}.b_df"


@pytest.fixture
def asset_b_plus_one_table_identifier(namespace: str) -> str:
    return f"{namespace}.b_plus_one"


@pytest.fixture
def asset_daily_partitioned_table_identifier(namespace: str) -> str:
    return f"{namespace}.daily_partitioned"


@pytest.fixture
def asset_hourly_partitioned_table_identifier(namespace: str) -> str:
    return f"{namespace}.hourly_partitioned"


@pytest.fixture
def asset_multi_partitioned_table_identifier(namespace: str) -> str:
    return f"{namespace}.multi_partitioned"


@asset(
    key_prefix=["my_schema"],
    metadata={"partition_spec_update_mode": "update", "schema_update_mode": "update"},
)
def b_df() -> DataFrame:
    spark = (
        SparkSession.builder.remote("sc://localhost")
        .config(map=SPARK_CONFIG)
        .getOrCreate()
    )
    return spark.createDataFrame(
        pa.Table.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]}).to_pandas()
    )


@asset(key_prefix=["my_schema"])
def b_plus_one(b_df: DataFrame) -> DataFrame:
    return b_df.withColumn("a", b_df.a + 1)


@asset(
    key_prefix=["my_schema"],
    partitions_def=DailyPartitionsDefinition(start_date="2022-01-01"),
    config_schema={"value": str},
    metadata={"partition_expr": "partition"},
)
def daily_partitioned(context: AssetExecutionContext) -> DataFrame:
    partition = datetime.datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    value = context.op_execution_context.op_config["value"]

    spark = (
        SparkSession.builder.remote("sc://localhost")
        .config(map=SPARK_CONFIG)
        .getOrCreate()
    )
    return spark.createDataFrame(
        [(partition, value, 1)], "partition: date, value: string, b: int"
    )


@asset(
    key_prefix=["my_schema"],
    partitions_def=HourlyPartitionsDefinition(
        start_date=datetime.datetime(2022, 1, 1, 0)
    ),
    config_schema={"value": str},
    metadata={"partition_expr": "partition"},
)
def hourly_partitioned(context: AssetExecutionContext) -> DataFrame:
    partition = datetime.datetime.strptime(context.partition_key, "%Y-%m-%d-%H:%M")
    value = context.op_execution_context.op_config["value"]

    spark = (
        SparkSession.builder.remote("sc://localhost")
        .config(map=SPARK_CONFIG)
        .getOrCreate()
    )
    return spark.createDataFrame(
        [(partition, value, 1)], "partition: timestamp_ntz, value: string, b: int"
    )


@asset(
    key_prefix=["my_schema"],
    partitions_def=MultiPartitionsDefinition(
        partitions_defs={
            "date": DailyPartitionsDefinition(
                start_date="2022-01-01",
                end_date="2022-01-10",
            ),
            "category": StaticPartitionsDefinition(["a", "b", "c"]),
        },
    ),
    config_schema={"value": str},
    metadata={
        "partition_expr": {
            "date": "date_this",
            "category": "category_this",
        },
    },
)
def multi_partitioned(context: AssetExecutionContext) -> DataFrame:
    category, date = context.partition_key.split("|")
    date_parsed = datetime.datetime.strptime(date, "%Y-%m-%d").date()
    value = context.op_execution_context.op_config["value"]

    spark = (
        SparkSession.builder.remote("sc://localhost")
        .config(map=SPARK_CONFIG)
        .getOrCreate()
    )
    return spark.createDataFrame(
        [(date_parsed, value, 1, category)],
        "date_this: date, value: string, b: int, category_this: string",
    )


def _exec_in_container(container: docker.models.containers.Container, *statements: str):
    """Execute a series of statements in the specified Docker container.

    This is a workaround for the fact that files created from within the
    Docker Compose context are inaccessible from the host context.

    Raises an exception if an error occurs while running the statements.
    """
    setup_statements = [
        "import datetime",
        "from pyiceberg.catalog import load_catalog",
        'catalog = load_catalog("postgres", **{"uri": "postgresql+psycopg2://test:test@postgres:5432/test", "warehouse": "file:///home/iceberg/warehouse"})',
    ]
    exit_code, output = container.exec_run(
        f"""python -c '{"; ".join([*setup_statements, *statements])}'"""
    )
    if exit_code:
        raise Exception(output)


def test_iceberg_io_manager_with_assets(
    asset_b_df_table_identifier: str,
    asset_b_plus_one_table_identifier: str,
    catalog: Catalog,
    io_manager: SparkIcebergIOManager,
):
    resource_defs = {"io_manager": io_manager}

    client = docker.from_env()
    container = client.containers.get("pyiceberg-spark")

    for _ in range(2):
        res = materialize([b_df, b_plus_one], resources=resource_defs)
        assert res.success

        _exec_in_container(
            container,
            f'table = catalog.load_table("{asset_b_df_table_identifier}")',
            "out_df = table.scan().to_arrow()",
            'assert out_df["a"].to_pylist() == [1, 2, 3]',
        )

        _exec_in_container(
            container,
            f'table = catalog.load_table("{asset_b_plus_one_table_identifier}")',
            "out_df = table.scan().to_arrow()",
            'assert out_df["a"].to_pylist() == [2, 3, 4]',
        )


def test_iceberg_io_manager_with_daily_partitioned_assets(
    asset_daily_partitioned_table_identifier: str,
    catalog: Catalog,
    io_manager: SparkIcebergIOManager,
):
    resource_defs = {"io_manager": io_manager}

    for date in ["2022-01-01", "2022-01-02", "2022-01-03"]:
        res = materialize(
            [daily_partitioned],
            partition_key=date,
            resources=resource_defs,
            run_config={
                "ops": {"my_schema__daily_partitioned": {"config": {"value": "1"}}},
            },
        )
        assert res.success

    client = docker.from_env()
    container = client.containers.get("pyiceberg-spark")

    _exec_in_container(
        container,
        f'table = catalog.load_table("{asset_daily_partitioned_table_identifier}")',
        "assert len(table.spec().fields) == 1",
        'assert table.spec().fields[0].name == "partition_day"',
        "out_df = table.scan().to_arrow()",
        'assert out_df["partition"].to_pylist() == [datetime.date(2022, 1, 3), datetime.date(2022, 1, 2), datetime.date(2022, 1, 1)]',
    )


def test_iceberg_io_manager_with_hourly_partitioned_assets(
    asset_hourly_partitioned_table_identifier: str,
    catalog: Catalog,
    io_manager: SparkIcebergIOManager,
):
    resource_defs = {"io_manager": io_manager}

    for date in ["2022-01-01-01:00", "2022-01-01-02:00", "2022-01-01-03:00"]:
        res = materialize(
            [hourly_partitioned],
            partition_key=date,
            resources=resource_defs,
            run_config={
                "ops": {"my_schema__hourly_partitioned": {"config": {"value": "1"}}},
            },
        )
        assert res.success

    client = docker.from_env()
    container = client.containers.get("pyiceberg-spark")

    _exec_in_container(
        container,
        f'table = catalog.load_table("{asset_hourly_partitioned_table_identifier}")',
        "assert len(table.spec().fields) == 1",
        'assert table.spec().fields[0].name == "partition_hour"',
        "out_df = table.scan().to_arrow()",
        'assert out_df["partition"].to_pylist() == [datetime.datetime(2022, 1, 1, 3, 0), datetime.datetime(2022, 1, 1, 2, 0), datetime.datetime(2022, 1, 1, 1, 0)]',
    )


def test_iceberg_io_manager_with_multipartitioned_assets(
    asset_multi_partitioned_table_identifier: str,
    catalog: Catalog,
    io_manager: SparkIcebergIOManager,
):
    resource_defs = {"io_manager": io_manager}

    for key in [
        "a|2022-01-01",
        "b|2022-01-01",
        "c|2022-01-01",
        "a|2022-01-02",
        "b|2022-01-02",
        "c|2022-01-02",
    ]:
        res = materialize(
            [multi_partitioned],
            partition_key=key,
            resources=resource_defs,
            run_config={
                "ops": {"my_schema__multi_partitioned": {"config": {"value": "1"}}},
            },
        )
        assert res.success

    client = docker.from_env()
    container = client.containers.get("pyiceberg-spark")

    _exec_in_container(
        container,
        f'table = catalog.load_table("{asset_multi_partitioned_table_identifier}")',
        "assert len(table.spec().fields) == 2",
        'assert [f.name for f in table.spec().fields] == ["category_this", "date_this_day"]',
        "out_df = table.scan().to_arrow()",
        'assert out_df["date_this"].to_pylist() == [datetime.date(2022, 1, 2), datetime.date(2022, 1, 2), datetime.date(2022, 1, 2), datetime.date(2022, 1, 1), datetime.date(2022, 1, 1), datetime.date(2022, 1, 1)]',
        'assert out_df["category_this"].to_pylist() == ["c", "b", "a", "c", "b", "a"]',
    )
