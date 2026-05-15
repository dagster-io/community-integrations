from typing import Any

from dagster import Config
from dagster._annotations import public
from pydantic import Field, model_validator

from dagster_iceberg._utils import (
    DEFAULT_PARTITION_FIELD_NAME_PREFIX,
    UpsertOptions,  # noqa: F401 - can be used for type validation in asset metadata
    preview,
)


@public
@preview
class IcebergCatalogConfig(Config):
    """Configuration for Iceberg Catalogs.

    See the `Catalogs section <https://py.iceberg.apache.org/configuration/#catalogs>`_
    for configuration options.

    You can configure the Iceberg IO manager:

        1. Using a ``.pyiceberg.yaml`` configuration file.
        2. Through environment variables.
        3. Using the ``IcebergCatalogConfig`` configuration object.

    For more information about the first two configuration options, see
    `Setting Configuration Values <https://py.iceberg.apache.org/configuration/#setting-configuration-values>`_.

    Example:
        .. code-block:: python

            from dagster_iceberg.config import IcebergCatalogConfig
            from dagster_iceberg.io_manager.arrow import PyArrowIcebergIOManager

            warehouse_path = "/path/to/warehouse"

            io_manager = PyArrowIcebergIOManager(
                name="my_catalog",
                config=IcebergCatalogConfig(
                    properties={
                        "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
                        "warehouse": f"file://{warehouse_path}",
                    }
                ),
                namespace="my_namespace",
            )

    """

    properties: dict[str, Any]
    partition_field_name_prefix: str = Field(
        default=DEFAULT_PARTITION_FIELD_NAME_PREFIX,
        description="Prefix to apply to the partition field names. This is required to avoid conflicts with schema field names when defining partitions using non-identity transforms in pyiceberg 0.10.0+. Defaults to 'part'.",
    )

    @model_validator(mode="after")
    def validate_partition_field_name_prefix(self) -> "IcebergCatalogConfig":
        if self.partition_field_name_prefix == "":
            raise ValueError("partition_field_name_prefix cannot be an empty string")
        return self


@public
@preview
class S3TablesCatalogConfig(IcebergCatalogConfig):
    """Configuration helper for AWS S3 Tables catalogs.

    `S3 Tables <https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables.html>`_
    is AWS's managed Apache Iceberg catalog. Tables are exposed via a Glue REST
    Iceberg endpoint that uses SigV4 to sign requests.

    The base :class:`IcebergCatalogConfig` already supports REST catalogs via
    the ``properties`` mapping; this subclass derives the S3 Tables-specific
    properties from a few high-level fields so callers don't have to remember
    the Glue endpoint URL or warehouse string format.

    Example:
        .. code-block:: python

            from dagster_iceberg.config import S3TablesCatalogConfig
            from dagster_iceberg.io_manager.arrow import PyArrowIcebergIOManager

            io_manager = PyArrowIcebergIOManager(
                name="s3_tables",
                config=S3TablesCatalogConfig(
                    region="eu-west-2",
                    table_bucket_arn="arn:aws:s3tables:eu-west-2:123456789012:bucket/my-tables",
                ),
                namespace="analytics",
            )
    """

    region: str = Field(description="AWS region the table bucket lives in.")
    table_bucket_arn: str = Field(
        description=(
            "ARN of the S3 Tables bucket, e.g. "
            "``arn:aws:s3tables:<region>:<account>:bucket/<bucket-name>``."
        ),
    )
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "PyIceberg properties. Defaults are derived from ``region`` and "
            "``table_bucket_arn``; any keys you set here override those "
            "defaults so you can tweak individual REST properties without "
            "redoing the whole derivation."
        ),
    )

    # Use ``mode="before"`` because Dagster's :class:`Config` is a frozen
    # Pydantic model — we cannot mutate ``self.properties`` in an after-validator.
    @model_validator(mode="before")
    @classmethod
    def _populate_properties(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        region = data.get("region")
        arn = data.get("table_bucket_arn")
        if region is None or arn is None:
            # Let Pydantic's required-field validation raise the canonical error.
            return data
        account_id, bucket_name = _parse_s3tables_arn(arn)
        derived: dict[str, Any] = {
            "type": "rest",
            "uri": f"https://glue.{region}.amazonaws.com/iceberg",
            "warehouse": f"{account_id}:s3tablescatalog/{bucket_name}",
            "rest.sigv4-enabled": "true",
            "rest.signing-name": "glue",
            "rest.signing-region": region,
        }
        # User-supplied properties override the derived defaults so callers
        # can still tweak individual REST properties without having to fork
        # the whole derivation.
        derived.update(data.get("properties") or {})
        data["properties"] = derived
        return data


def _parse_s3tables_arn(arn: str) -> tuple[str, str]:
    """Extract (account_id, bucket_name) from an S3 Tables ARN.

    Expected ARN format::

        arn:aws:s3tables:<region>:<account-id>:bucket/<bucket-name>
    """
    parts = arn.split(":")
    if len(parts) < 6 or parts[0] != "arn" or parts[2] != "s3tables":
        raise ValueError(
            f"Not a valid S3 Tables bucket ARN: {arn!r}. Expected "
            "'arn:aws:s3tables:<region>:<account>:bucket/<bucket-name>'."
        )
    account_id = parts[4]
    resource = parts[5]
    if not resource.startswith("bucket/"):
        raise ValueError(
            f"Not a valid S3 Tables bucket ARN: {arn!r}. Resource portion "
            f"{resource!r} must start with 'bucket/'."
        )
    bucket_name = resource.split("/", 1)[1]
    if not bucket_name:
        raise ValueError(f"Empty bucket name in S3 Tables ARN: {arn!r}.")
    return account_id, bucket_name
