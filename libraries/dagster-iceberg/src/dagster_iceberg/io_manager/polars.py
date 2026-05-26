from collections.abc import Sequence
from typing import Literal, Union

try:
    import polars as pl
except ImportError as e:
    raise ImportError("Please install dagster-iceberg with the 'polars' extra.") from e
import pyarrow as pa
from dagster import InputContext
from dagster._annotations import public
from dagster._core.storage.db_io_manager import DbTypeHandler, TableSlice
from pydantic import Field, field_validator
from pyiceberg import table as ibt
from pyiceberg.catalog import Catalog

from dagster_iceberg import handler as _handler
from dagster_iceberg import io_manager as _io_manager
from dagster_iceberg._utils import DagsterPartitionToPolarsSqlPredicateMapper, preview

PolarsTypes = Union[pl.LazyFrame, pl.DataFrame]  # noqa: UP007, avoid `autodoc` failure

_VALID_READER_OVERRIDES = frozenset({"native", "pyiceberg"})


def _get_polars_storage_options(table: ibt.Table) -> dict[str, str] | None:
    """Translate common PyIceberg S3 catalog options to Polars object-store options."""
    properties = getattr(table.io, "properties", {})
    storage_options: dict[str, str] = {}
    option_mapping = {
        "s3.access-key-id": "aws_access_key_id",
        "s3.secret-access-key": "aws_secret_access_key",
        "s3.session-token": "aws_session_token",
        "s3.endpoint": "aws_endpoint_url",
        "s3.region": "aws_region",
    }
    for pyiceberg_key, polars_key in option_mapping.items():
        value = properties.get(pyiceberg_key)
        if value is not None:
            storage_options[polars_key] = str(value)

    if "aws_endpoint_url" in storage_options:
        storage_options.setdefault("aws_allow_http", "true")
        storage_options.setdefault("aws_region", "us-east-1")

    return storage_options or None


class _PolarsIcebergTypeHandler(_handler.IcebergBaseTypeHandler[PolarsTypes]):
    """Type handler that converts data between Iceberg tables and Polars DataFrames"""

    def to_data_frame(
        self,
        table: ibt.Table,
        table_slice: TableSlice,
        target_type: type[PolarsTypes],
        reader_override: Literal["native", "pyiceberg"] | None = None,
    ) -> PolarsTypes:
        selected_fields: str = (
            ",".join(table_slice.columns) if table_slice.columns is not None else "*"
        )
        row_filter: str | None = None
        if table_slice.partition_dimensions:
            expressions = DagsterPartitionToPolarsSqlPredicateMapper(
                partition_dimensions=table_slice.partition_dimensions,
                table_schema=table.schema(),
                table_partition_spec=table.spec(),
            ).partition_dimensions_to_filters()
            row_filter = " AND ".join(expressions)

        pdf = pl.scan_iceberg(
            source=table,
            storage_options=_get_polars_storage_options(table),
            reader_override=reader_override,
        )

        stmt = f"SELECT {selected_fields} FROM self"
        if row_filter is not None:
            stmt += f"\nWHERE {row_filter}"
        return pdf.sql(stmt) if target_type == pl.LazyFrame else pdf.sql(stmt).collect()

    def load_input(
        self,
        context: InputContext,
        table_slice: TableSlice,
        connection: Catalog,
    ) -> PolarsTypes:
        """Loads the input as a Polars frame, respecting the configured ``reader_override``."""
        reader_override: Literal["native", "pyiceberg"] | None = None
        if context.resource_config:
            reader_override = context.resource_config.get("reader_override")
        return self.to_data_frame(
            table=connection.load_table(f"{table_slice.schema}.{table_slice.table}"),
            table_slice=table_slice,
            target_type=context.dagster_type.typing_type,
            reader_override=reader_override,
        )

    def to_arrow(self, obj: PolarsTypes) -> pa.Table:
        if isinstance(obj, pl.LazyFrame):
            return obj.collect().to_arrow()
        return obj.to_arrow()

    @property
    def supported_types(self) -> Sequence[type[object]]:
        return (pl.LazyFrame, pl.DataFrame)


@public
@preview
class PolarsIcebergIOManager(_io_manager.IcebergIOManager):
    """An I/O manager definition that reads inputs from and writes outputs to Iceberg tables using Polars.

    Examples:
        .. code-block:: python

            import polars as pl
            from dagster import Definitions, asset
            from dagster_iceberg.config import IcebergCatalogConfig
            from dagster_iceberg.io_manager.polars import PolarsIcebergIOManager

            CATALOG_URI = "sqlite:////home/vscode/workspace/.tmp/examples/select_columns/catalog.db"
            CATALOG_WAREHOUSE = (
                "file:///home/vscode/workspace/.tmp/examples/select_columns/warehouse"
            )

            resources = {
                "io_manager": PolarsIcebergIOManager(
                    name="test",
                    config=IcebergCatalogConfig(
                        properties={"uri": CATALOG_URI, "warehouse": CATALOG_WAREHOUSE}
                    ),
                    namespace="dagster",
                )
            }


            @asset
            def iris_dataset() -> pl.DataFrame:
                return pl.read_csv(
                    "https://docs.dagster.io/assets/iris.csv",
                    has_header=False,
                    new_columns=[
                        "sepal_length_cm",
                        "sepal_width_cm",
                        "petal_length_cm",
                        "petal_width_cm",
                        "species",
                    ],
                )


            defs = Definitions(assets=[iris_dataset], resources=resources)

        If you do not provide a schema, Dagster will determine a schema based on the assets and ops using
        the I/O manager. For assets, the schema will be determined from the asset key, as in the above example.
        For ops, the schema can be specified by including a "schema" entry in output metadata. If none
        of these is provided, the schema will default to "public". The I/O manager will check if the namespace
        exists in the Iceberg catalog. It does not automatically create the namespace if it does not exist.

        .. code-block:: python

            @op(
                out={"my_table": Out(metadata={"schema": "my_schema"})}
            )
            def make_my_table() -> pl.DataFrame:
                ...

        To only use specific columns of a table as input to a downstream op or asset, add the metadata "columns" to the
        ``In`` or ``AssetIn``.

        .. code-block:: python

            @asset(
                ins={"my_table": AssetIn("my_table", metadata={"columns": ["a"]})}
            )
            def my_table_a(my_table: pl.DataFrame):
                # my_table will just contain the data from column "a"
                ...

    """

    reader_override: str | None = Field(
        default=None,
        description=(
            "Override the Polars Iceberg reader implementation used by "
            "``polars.scan_iceberg``. When set to ``'pyiceberg'``, Polars delegates "
            "all I/O to the PyIceberg library instead of its native Rust reader. "
            "Use this on deployments (e.g. Kubernetes) where the native reader "
            "leaves a deadlocked thread open after reading from S3, which prevents "
            "the Dagster run from finalising. When ``None`` (the default), Polars "
            "selects the best reader automatically. "
            f"Valid values: {sorted(_VALID_READER_OVERRIDES)}."
        ),
    )

    @field_validator("reader_override")
    @classmethod
    def _validate_reader_override(cls, v: str | None) -> str | None:
        if v is not None and v not in _VALID_READER_OVERRIDES:
            raise ValueError(
                f"reader_override must be one of {sorted(_VALID_READER_OVERRIDES)} or None, got {v!r}"
            )
        return v

    @staticmethod
    def type_handlers() -> Sequence[DbTypeHandler]:
        return [_PolarsIcebergTypeHandler()]
