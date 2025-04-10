import subprocess

import pytest
from pyspark.sql import SparkSession

MAKEFILE_DIR = "kitchen-sink"


@pytest.fixture(scope="module", autouse=True)
def start_connect_server():
    subprocess.run(["make", "up"], cwd=MAKEFILE_DIR, check=True)
    subprocess.run(["sleep", "10"], check=True)
    yield
    subprocess.run(["make", "down"], cwd=MAKEFILE_DIR, check=True)


def test_spark_io_manager():
    spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
    assert not spark.catalog.listTables()
