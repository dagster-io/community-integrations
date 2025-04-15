import pyarrow as pa
import pytest
from dagster import asset, materialize
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
    spark.catalog.listCatalogs()  # Force Spark to initialize the catalog
    df = spark.createDataFrame(
        pa.Table.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]}).to_pandas()
    )
    df.show()
    return df


@asset(key_prefix=["my_schema"])
def b_plus_one(b_df: DataFrame) -> DataFrame:
    b_df.show()
    return b_df.withColumn("a", b_df.a + 1)


def test_spark_io_manager():
    spark = (
        SparkSession.builder.remote("sc://localhost")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.postgres", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.postgres.type", "jdbc")
        .config(
            "spark.sql.catalog.postgres.uri",
            "jdbc:postgresql://postgres:5432/test",
        )
        .config("spark.sql.catalog.postgres.jdbc.user", "test")
        .config("spark.sql.catalog.postgres.jdbc.password", "test")
        .config("spark.sql.catalog.postgres.warehouse", "/home/iceberg/warehouse")
        .config("spark.sql.defaultCatalog", "postgres")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "/home/iceberg/spark-events")
        .config("spark.history.fs.logDirectory", "/home/iceberg/spark-events")
        .config("spark.sql.catalogImplementation", "in-memory")
        .getOrCreate()
    )
    assert len(spark.catalog.listCatalogs()) == 1  # No idea why two calls are necessary
    assert len(spark.catalog.listCatalogs()) == 2
    assert spark.catalog.currentCatalog() == "postgres"


def test_iceberg_io_manager_with_assets(
    asset_b_df_table_identifier: str,
    asset_b_plus_one_table_identifier: str,
    catalog: Catalog,
    io_manager: SparkIcebergIOManager,
):
    resource_defs = {"io_manager": io_manager}

    for _ in range(2):
        res = materialize([b_df, b_plus_one], resources=resource_defs)
        assert res.success

        table = catalog.load_table(asset_b_df_table_identifier)
        out_df = table.scan().to_arrow()
        assert out_df["a"].to_pylist() == [1, 2, 3]

        dt = catalog.load_table(asset_b_plus_one_table_identifier)
        out_dt = dt.scan().to_arrow()
        assert out_dt["a"].to_pylist() == [2, 3, 4]
