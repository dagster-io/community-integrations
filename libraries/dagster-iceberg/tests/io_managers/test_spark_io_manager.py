from pyspark.sql import SparkSession


def test_spark_io_manager():
    spark = (
        SparkSession.builder.remote("sc://localhost:15002")
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
