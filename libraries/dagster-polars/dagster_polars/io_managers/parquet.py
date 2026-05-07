from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, ClassVar, Optional, cast

import polars as pl
import pyarrow.dataset as ds
from dagster import InputContext, OutputContext
from packaging.version import Version

from dagster_polars.io_managers.base import BasePolarsUPathIOManager

if TYPE_CHECKING:
    from upath import UPath


DAGSTER_POLARS_STORAGE_METADATA_KEY = "dagster_polars_metadata"


def get_pyarrow_dataset(path: "UPath", context: InputContext) -> ds.Dataset:
    context_metadata = context.definition_metadata or {}

    fs = path.fs if hasattr(path, "fs") else None

    if context_metadata.get("partitioning") is not None:
        context.log.warning(
            f'"partitioning" metadata value for PolarsParquetIOManager is deprecated '
            f'in favor of "partition_by" (loading from {path})'
        )

    dataset = ds.dataset(
        str(path),
        filesystem=fs,
        format=context_metadata.get("format", "parquet"),
        partitioning=context_metadata.get("partitioning")
        or context_metadata.get("partition_by"),
        partition_base_dir=context_metadata.get("partition_base_dir"),
        exclude_invalid_files=context_metadata.get("exclude_invalid_files", True),
        ignore_prefixes=context_metadata.get("ignore_prefixes", [".", "_"]),
    )

    return dataset


# fsspec exposes S3 credentials with different key names than the
# ``object_store`` Rust crate that Polars uses under the hood. Without a
# remap, Polars reads from S3 with empty credentials and fails with
# "Generic S3 error: Missing bucket name" or similar — see
# https://github.com/dagster-io/community-integrations/issues/257.

# Top-level fsspec → object_store key map.
_FSSPEC_TO_OBJECT_STORE = {
    "key": "aws_access_key_id",
    "secret": "aws_secret_access_key",
    "token": "aws_session_token",
}

# boto3-style keys nested inside fsspec ``client_kwargs`` → object_store keys.
# fsspec/s3fs accept several aliases here; map all the common ones we have
# seen in the wild.
_CLIENT_KWARGS_TO_OBJECT_STORE = {
    "region_name": "aws_region",
    "endpoint_url": "aws_endpoint_url",
    "aws_access_key_id": "aws_access_key_id",
    "aws_secret_access_key": "aws_secret_access_key",
    "aws_session_token": "aws_session_token",
    "verify": "aws_allow_http",  # fsspec ``verify=False`` ≈ object_store HTTP allowed
}

# fsspec-only keys with no ``object_store`` equivalent — drop on remap.
_FSSPEC_DROP = frozenset(
    {
        "client_options",
        "config_kwargs",
        "s3_additional_kwargs",
        "default_block_size",
        "default_cache_type",
        "version_aware",
        "anon",
        "use_listings_cache",
        "listings_expiry_time",
        "max_paths",
        "skip_instance_cache",
        "asynchronous",
        "loop",
    }
)

# object_store S3 keys we know about and pass through unchanged. Anything not
# on this list, not in ``_FSSPEC_TO_OBJECT_STORE``, and not in
# ``_FSSPEC_DROP`` is unknown — log a debug note and drop rather than forward
# a possibly-misspelled key into the Rust binding (which would raise a less
# helpful error).
_OBJECT_STORE_PASSTHROUGH = frozenset(
    {
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
        "aws_region",
        "aws_endpoint_url",
        "aws_allow_http",
        "aws_virtual_hosted_style_request",
        "aws_skip_signature",
        "aws_unsigned_payload",
        "aws_checksum_algorithm",
        "aws_server_side_encryption",
        "aws_sse_kms_key_id",
        "aws_sse_customer_key_base64",
        "aws_metadata_endpoint",
        "aws_container_credentials_relative_uri",
        "aws_imdsv1_fallback",
        "aws_request_payer",
        "aws_s3_express",
        "aws_disable_tagging",
        # generic object_store keys (work across S3-compatible backends)
        "endpoint",
        "endpoint_url",
        "region",
        "access_key_id",
        "secret_access_key",
        "session_token",
        "bucket",
        "allow_http",
        "allow_invalid_certificates",
        "skip_signature",
        "virtual_hosted_style_request",
        "request_payer",
    }
)


def _remap_fsspec_storage_options(
    opts: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    """Translate fsspec-style storage_options into ``object_store`` keys.

    Polars's ``scan_parquet`` / ``sink_parquet`` use Rust ``object_store`` and
    expect keys like ``aws_access_key_id``. ``UPath.storage_options`` is
    fsspec-shaped (``key``, ``secret``, ``client_kwargs``). Translate the
    common subset, recurse into ``client_kwargs``, drop fsspec-only keys,
    pass known ``object_store`` keys through unchanged. Unknown keys are
    silently dropped to avoid forwarding typos into the Rust binding (which
    surfaces them as opaque errors).
    """
    if not opts:
        return None
    out: dict[str, Any] = {}
    for k, v in opts.items():
        if k in _FSSPEC_TO_OBJECT_STORE:
            out[_FSSPEC_TO_OBJECT_STORE[k]] = v
        elif k == "client_kwargs" and isinstance(v, Mapping):
            for ck, cv in v.items():
                target = _CLIENT_KWARGS_TO_OBJECT_STORE.get(ck)
                if target is None:
                    continue
                if ck == "verify" and isinstance(cv, bool):
                    # ``verify=False`` (fsspec) ↔ ``aws_allow_http=True``.
                    out[target] = "true" if cv is False else "false"
                else:
                    out[target] = cv
        elif k in _FSSPEC_DROP:
            continue
        elif k in _OBJECT_STORE_PASSTHROUGH:
            out[k] = v
        # else: unknown key — drop silently to avoid handing a typo to
        # object_store, which would raise an unhelpful Rust-side error.
    return out or None


def scan_parquet(
    path: "UPath",
    context: InputContext,
    storage_options: Mapping[str, Any] | None = None,
) -> pl.LazyFrame:
    """Scan a parquet file and return a lazy frame (uses polars native reader).

    :param path:
    :param context:
    :param storage_options: Storage options from the IO manager. May be
        fsspec-shaped (``key``, ``secret``, ``client_kwargs``) or already
        ``object_store``-shaped — both are passed through
        :func:`_remap_fsspec_storage_options` before reaching Polars. When
        supplied, takes precedence over ``path.storage_options``.
    :return:
    """
    context_metadata = context.definition_metadata or {}

    kwargs = dict(
        n_rows=context_metadata.get("n_rows", None),
        cache=context_metadata.get("cache", True),
        parallel=context_metadata.get("parallel", "auto"),
        rechunk=context_metadata.get("rechunk", True),
        low_memory=context_metadata.get("low_memory", False),
        use_statistics=context_metadata.get("use_statistics", True),
        hive_partitioning=context_metadata.get("hive_partitioning", True),
        retries=context_metadata.get("retries", 0),
    )
    kwargs["row_index_name"] = context_metadata.get("row_index_name", None)
    kwargs["row_index_offset"] = context_metadata.get("row_index_offset", 0)

    # On Polars >= 1.17 ``object_store`` is the cloud backend; remap fsspec
    # keys so credentials reach the reader. Older Polars used a different
    # backend (no remap helped), so keep the legacy fsspec passthrough for
    # backwards compatibility — see issue #257.
    if Version(pl.__version__) >= Version("1.17.0"):
        # ``self.storage_options`` (passed in via ``storage_options``) and
        # ``path.storage_options`` are both fsspec-shaped because they come
        # from the same UPath kwargs. Remap unconditionally — already-
        # ``object_store``-shaped keys pass through unchanged.
        path_options = cast(
            Optional[dict[str, Any]],
            (path.storage_options if hasattr(path, "storage_options") else None),
        )
        pl_storage_options = _remap_fsspec_storage_options(
            storage_options or path_options
        )
    else:
        legacy_options = cast(
            Optional[dict[str, Any]],
            (path.storage_options if hasattr(path, "storage_options") else None),
        )
        # gh issue [dagster-io/dagster#28633]()
        INCOMPATIBLE_FSSPEC_KEYS = ["client_options"]
        pl_storage_options = (
            None
            if legacy_options is None
            else {
                k: v
                for k, v in legacy_options.items()
                if k not in INCOMPATIBLE_FSSPEC_KEYS
            }
        )

    return pl.scan_parquet(str(path), storage_options=pl_storage_options, **kwargs)  # type: ignore


class PolarsParquetIOManager(BasePolarsUPathIOManager):
    """Implements reading and writing Polars DataFrames in Apache Parquet format.

    Features:
     - All features provided by :py:class:`~dagster_polars.BasePolarsUPathIOManager`.
     - All read/write options can be set via corresponding metadata or config parameters (metadata takes precedence).
     - Supports reading partitioned Parquet datasets (for example, often produced by Spark).
     - Supports reading/writing custom metadata in the Parquet file's schema as json-serialized bytes at `"dagster_polars_metadata"` key.

    Examples:
        .. code-block:: python

            from dagster import asset
            from dagster_polars import PolarsParquetIOManager
            import polars as pl

            @asset(
                io_manager_key="polars_parquet_io_manager",
                key_prefix=["my_dataset"]
            )
            def my_asset() -> pl.DataFrame:  # data will be stored at <base_dir>/my_dataset/my_asset.parquet
                ...

            defs = Definitions(
                assets=[my_table],
                resources={
                    "polars_parquet_io_manager": PolarsParquetIOManager(base_dir="s3://my-bucket/my-dir")
                }
            )

        Reading partitioned Parquet datasets:

        .. code-block:: python

            from dagster import SourceAsset

            my_asset = SourceAsset(
                key=["path", "to", "dataset"],
                io_manager_key="polars_parquet_io_manager",
                metadata={
                    "partition_by": ["year", "month", "day"]
                }
            )

    """

    # ``ClassVar`` so pydantic stops treating ``extension`` as a model field
    # and the parent's annotation (``Optional[str]`` on ``UPathIOManager``) is
    # not reported as shadowed on every import. The parent declares
    # ``extension`` as an instance var, hence the ty override silencer.
    extension: ClassVar[Optional[str]] = ".parquet"  # ty: ignore[invalid-attribute-override]

    def sink_df_to_path(
        self,
        context: OutputContext,
        df: pl.LazyFrame,
        path: "UPath",
    ):
        context_metadata = context.definition_metadata or {}
        compression = context_metadata.get("compression", "zstd")
        compression_level = context_metadata.get("compression_level")
        statistics = context_metadata.get("statistics", False)
        row_group_size = context_metadata.get("row_group_size")

        # sink_parquet gained storage_options in Polars 1.17.0
        if Version(pl.__version__) >= Version("1.17.0"):
            df.sink_parquet(
                str(path),
                compression=compression,
                compression_level=compression_level,
                statistics=statistics,
                row_group_size=row_group_size,
                storage_options=_remap_fsspec_storage_options(self.storage_options),
            )
        else:
            context.log.warning(
                "Cloud sink with storage_options requires Polars >= 1.17.0, "
                "falling back to collecting the LazyFrame first.",
            )
            return self.write_df_to_path(context, df.collect(), path)

    def write_df_to_path(
        self,
        context: OutputContext,
        df: pl.DataFrame,
        path: "UPath",
    ):
        context_metadata = context.definition_metadata or {}
        compression = context_metadata.get("compression", "zstd")
        compression_level = context_metadata.get("compression_level")
        statistics = context_metadata.get("statistics", False)
        row_group_size = context_metadata.get("row_group_size")
        pyarrow_options = context_metadata.get("pyarrow_options", None)

        fs = path.fs if hasattr(path, "fs") else None

        if pyarrow_options is not None:
            pyarrow_options["filesystem"] = fs
            df.write_parquet(
                str(path),
                compression=compression,  # type: ignore
                compression_level=compression_level,
                statistics=statistics,
                row_group_size=row_group_size,
                use_pyarrow=True,
                pyarrow_options=pyarrow_options,
            )
        elif fs is not None:
            with fs.open(str(path), mode="wb") as f:
                df.write_parquet(
                    f,  # type: ignore
                    compression=compression,  # type: ignore
                    compression_level=compression_level,
                    statistics=statistics,
                    row_group_size=row_group_size,
                )
        else:
            # storage_options for write_parquet was added in Polars 1.17.0
            kwargs: dict[str, Any] = dict(
                compression=compression,
                compression_level=compression_level,
                statistics=statistics,
                row_group_size=row_group_size,
            )
            if Version(pl.__version__) >= Version("1.17.0"):
                kwargs["storage_options"] = _remap_fsspec_storage_options(
                    self.storage_options
                )
            df.write_parquet(str(path), **kwargs)  # type: ignore

    def scan_df_from_path(
        self,
        path: "UPath",
        context: InputContext,
        partition_key: str | None = None,
    ) -> pl.LazyFrame:
        return scan_parquet(path, context, storage_options=self.storage_options)
