"""Unit tests for S3TablesCatalogConfig."""

import pytest

from dagster_iceberg.config import S3TablesCatalogConfig

VALID_ARN = "arn:aws:s3tables:eu-west-2:123456789012:bucket/my-tables"


def test_derives_rest_catalog_properties() -> None:
    cfg = S3TablesCatalogConfig(region="eu-west-2", table_bucket_arn=VALID_ARN)
    assert cfg.properties == {
        "type": "rest",
        "uri": "https://glue.eu-west-2.amazonaws.com/iceberg",
        "warehouse": "123456789012:s3tablescatalog/my-tables",
        "rest.sigv4-enabled": "true",
        "rest.signing-name": "glue",
        "rest.signing-region": "eu-west-2",
    }


def test_user_properties_extend_derived() -> None:
    cfg = S3TablesCatalogConfig(
        region="eu-west-2",
        table_bucket_arn=VALID_ARN,
        properties={"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"},
    )
    assert cfg.properties["py-io-impl"] == "pyiceberg.io.fsspec.FsspecFileIO"
    # Derived defaults still present.
    assert cfg.properties["type"] == "rest"
    assert cfg.properties["rest.sigv4-enabled"] == "true"


def test_user_properties_override_derived() -> None:
    cfg = S3TablesCatalogConfig(
        region="eu-west-2",
        table_bucket_arn=VALID_ARN,
        properties={"rest.signing-region": "us-east-1"},
    )
    assert cfg.properties["rest.signing-region"] == "us-east-1"
    # Other defaults still present.
    assert cfg.properties["type"] == "rest"


def test_invalid_arn_prefix_raises() -> None:
    with pytest.raises(ValueError, match="Not a valid S3 Tables bucket ARN"):
        S3TablesCatalogConfig(
            region="eu-west-2",
            table_bucket_arn="arn:aws:s3:::a-regular-bucket",
        )


def test_invalid_arn_resource_raises() -> None:
    with pytest.raises(ValueError, match="must start with 'bucket/'"):
        S3TablesCatalogConfig(
            region="eu-west-2",
            table_bucket_arn="arn:aws:s3tables:eu-west-2:123:table/foo",
        )


def test_empty_bucket_name_raises() -> None:
    with pytest.raises(ValueError, match="Empty bucket name"):
        S3TablesCatalogConfig(
            region="eu-west-2",
            table_bucket_arn="arn:aws:s3tables:eu-west-2:123:bucket/",
        )


def test_partition_field_name_prefix_inherited() -> None:
    cfg = S3TablesCatalogConfig(
        region="eu-west-2",
        table_bucket_arn=VALID_ARN,
        partition_field_name_prefix="custom",
    )
    assert cfg.partition_field_name_prefix == "custom"
