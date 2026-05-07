"""Integration tests for S3 storage with PolarsParquetIOManager using moto server.

The fixture deliberately does NOT set AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY /
AWS_ENDPOINT_URL as environment variables. Polars' Rust ``object_store`` reads
those env vars directly, which would bypass the ``storage_options`` plumbing
this test suite is here to exercise (see issue #257). Credentials and endpoint
are passed in fsspec shape via the IO manager's ``storage_options`` field.
"""

import os
import shutil
import tempfile
from pathlib import Path

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

# fsspec-shaped storage_options. Mirrors the real-world configuration users
# supply: keys/secrets are fsspec keys (``key`` / ``secret``), endpoint and
# region live under ``client_kwargs``. The IO manager remaps these to
# ``object_store`` keys (``aws_access_key_id`` etc.) before handing them to
# Polars.
S3_STORAGE_OPTIONS = {
    "key": "testing",
    "secret": "testing",
    "client_kwargs": {
        "endpoint_url": S3_ENDPOINT,
        "region_name": "us-east-1",
    },
}


@pytest.fixture(scope="module")
def moto_server():
    """Start moto server for the test module."""
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
def s3_env(moto_server, tmp_path):
    """Isolate the AWS credential chain so Polars/object_store has nothing to
    fall back on outside the explicit ``storage_options`` we hand to the
    IO manager.

    - Strip every ``AWS_*`` env var (no key/secret/endpoint leakage to
      ``object_store``).
    - Point ``HOME`` and the AWS shared/config-file env vars at empty paths
      so neither boto3 nor object_store can read ``~/.aws/credentials``.
    - Clear s3fs caches.
    """
    old_env = os.environ.copy()
    old_home = os.environ.get("HOME")
    fake_home = Path(tempfile.mkdtemp(prefix="dagster-polars-s3-env-"))
    try:
        for k in list(os.environ):
            if k.startswith("AWS_"):
                del os.environ[k]
        os.environ["HOME"] = str(fake_home)
        os.environ["AWS_SHARED_CREDENTIALS_FILE"] = str(fake_home / "no-credentials")
        os.environ["AWS_CONFIG_FILE"] = str(fake_home / "no-config")
        s3fs.S3FileSystem.clear_instance_cache()
        yield
    finally:
        os.environ.clear()
        os.environ.update(old_env)
        if old_home is not None:
            os.environ["HOME"] = old_home
        s3fs.S3FileSystem.clear_instance_cache()
        shutil.rmtree(fake_home, ignore_errors=True)


def test_polars_parquet_io_manager_s3_write_read(s3_env, s3_client):
    """Write and read a DataFrame to/from S3 with explicit fsspec-shaped
    ``storage_options``. The IO manager must remap fsspec keys for both the
    write and read paths."""
    io_manager = PolarsParquetIOManager(
        base_dir=f"s3://{S3_BUCKET}/eager",
        storage_options=S3_STORAGE_OPTIONS,
    )

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
    """Sink and scan a LazyFrame on S3. Exercises ``sink_parquet`` →
    ``scan_parquet``, both of which receive the IO manager's fsspec-shaped
    ``storage_options`` and must remap before handing them to Polars'
    ``object_store`` backend."""
    io_manager = PolarsParquetIOManager(
        base_dir=f"s3://{S3_BUCKET}/lazy",
        storage_options=S3_STORAGE_OPTIONS,
    )

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


def test_polars_parquet_io_manager_s3_fsspec_storage_options(s3_env, s3_client):
    """Regression test for issue #257 / dagster-io/dagster#22079.

    Without the remap, ``scan_parquet`` receives fsspec keys (``key``,
    ``secret``, ``client_kwargs``) and raises
    ``TypeError: failed to extract field Extract.storage_options`` because
    Polars' ``object_store`` backend does not understand them.

    The ``s3_env`` fixture strips every ``AWS_*`` env var and isolates the
    AWS credential files so neither s3fs/boto3 nor ``object_store`` can fall
    back to the ambient environment — credentials must travel through
    ``storage_options``.
    """
    io_manager = PolarsParquetIOManager(
        base_dir=f"s3://{S3_BUCKET}/regression",
        storage_options=S3_STORAGE_OPTIONS,
    )

    test_df = pl.DataFrame({"a": [1, 2, 3]})

    @asset(io_manager_def=io_manager)
    def upstream() -> pl.LazyFrame:
        return test_df.lazy()

    @asset(io_manager_def=io_manager)
    def downstream(upstream: pl.LazyFrame) -> pl.DataFrame:
        return upstream.collect()

    result = materialize([upstream, downstream])
    assert result.success
    pl_testing.assert_frame_equal(result.output_for_node("downstream"), test_df)
