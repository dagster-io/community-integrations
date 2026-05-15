"""Integration tests for S3 storage with PolarsParquetIOManager using moto server."""

import os

import boto3
import polars as pl
import polars.testing as pl_testing
import pytest
import s3fs
from dagster import asset, materialize
from moto.server import ThreadedMotoServer

from dagster_polars import PolarsParquetIOManager

S3_BUCKET = "test-bucket"
S3_PORT = 5555
S3_ENDPOINT = f"http://127.0.0.1:{S3_PORT}"


@pytest.fixture(scope="module")
def moto_server():
    """Start moto server for the test module."""
    # Clear s3fs cache to avoid using stale connections
    s3fs.S3FileSystem.clear_instance_cache()
    server = ThreadedMotoServer(port=S3_PORT)
    server.start()
    yield server
    server.stop()


@pytest.fixture(scope="module")
def s3_client(moto_server):
    """Create S3 client and bucket."""
    client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        region_name="us-east-1",
    )
    client.create_bucket(Bucket=S3_BUCKET)
    return client


@pytest.fixture
def s3_env(moto_server):
    """Set up S3 environment variables for s3fs/boto3."""
    old_env = os.environ.copy()
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ["AWS_ENDPOINT_URL"] = S3_ENDPOINT
    # Clear s3fs cache so it picks up new environment
    s3fs.S3FileSystem.clear_instance_cache()
    yield
    os.environ.clear()
    os.environ.update(old_env)
    s3fs.S3FileSystem.clear_instance_cache()


def test_polars_parquet_io_manager_s3_write_read(s3_env, s3_client):
    """Test writing and reading a DataFrame to/from S3 using environment credentials."""
    io_manager = PolarsParquetIOManager(base_dir=f"s3://{S3_BUCKET}/eager")

    test_df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})

    @asset(io_manager_def=io_manager)
    def upstream_eager() -> pl.DataFrame:
        return test_df

    @asset(io_manager_def=io_manager)
    def downstream_eager(upstream_eager: pl.DataFrame) -> pl.DataFrame:
        return upstream_eager

    result = materialize([upstream_eager, downstream_eager])
    assert result.success

    # Verify the data was written to S3
    objects = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix="eager/")
    assert "Contents" in objects
    assert len(objects["Contents"]) > 0

    # Verify we can read the data back correctly
    output = result.output_for_node("downstream_eager")
    pl_testing.assert_frame_equal(output, test_df)


def test_polars_parquet_io_manager_s3_lazy_write_read(s3_env, s3_client):
    """Test writing and reading a LazyFrame to/from S3 using environment credentials."""
    io_manager = PolarsParquetIOManager(base_dir=f"s3://{S3_BUCKET}/lazy")

    test_df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})

    @asset(io_manager_def=io_manager)
    def upstream_lazy() -> pl.LazyFrame:
        return test_df.lazy()

    @asset(io_manager_def=io_manager)
    def downstream_lazy(upstream_lazy: pl.LazyFrame) -> pl.DataFrame:
        return upstream_lazy.collect()

    result = materialize([upstream_lazy, downstream_lazy])
    assert result.success

    # Verify the data was written to S3
    objects = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix="lazy/")
    assert "Contents" in objects
    assert len(objects["Contents"]) > 0

    # Verify we can read the data back correctly
    output = result.output_for_node("downstream_lazy")
    pl_testing.assert_frame_equal(output, test_df)
