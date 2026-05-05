"""Optional live integration test for S3TablesCatalogConfig.

Skipped unless the following environment variables are set:

* ``S3_TABLES_BUCKET_ARN`` — ARN of an S3 Tables bucket.
* ``S3_TABLES_REGION`` — AWS region (e.g. ``eu-west-2``).
* ``AWS_PROFILE`` (or any other boto3 credential source) — must resolve to
  credentials that can list namespaces in the bucket.

Run::

    AWS_PROFILE=... S3_TABLES_BUCKET_ARN=... S3_TABLES_REGION=... \
        uv run pytest tests/unit/test_s3_tables_live.py -v
"""

import os

import pytest

from dagster_iceberg.config import S3TablesCatalogConfig

ARN = os.environ.get("S3_TABLES_BUCKET_ARN")
REGION = os.environ.get("S3_TABLES_REGION")

pytestmark = pytest.mark.skipif(
    not (ARN and REGION),
    reason="Set S3_TABLES_BUCKET_ARN + S3_TABLES_REGION to run live test.",
)


def test_load_catalog_lists_namespaces() -> None:
    pyiceberg_catalog = pytest.importorskip("pyiceberg.catalog")

    cfg = S3TablesCatalogConfig(region=REGION, table_bucket_arn=ARN)
    catalog = pyiceberg_catalog.load_catalog("s3_tables_test", **cfg.properties)
    namespaces = list(catalog.list_namespaces())
    assert isinstance(namespaces, list)
