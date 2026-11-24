import datetime as dt
from collections.abc import Sequence

try:
    import daft as da
except ImportError as e:
    raise ImportError("Please install dagster-iceberg with the 'daft' extra.") from e
import pyarrow as pa
from dagster._annotations import public
from dagster._core.storage.db_io_manager import DbTypeHandler, TableSlice
from pyiceberg import table as ibt

from dagster_iceberg import handler as _handler
from dagster_iceberg import io_manager as _io_manager
from dagster_iceberg._utils import DagsterPartitionToPredicateMapper, preview

# daft.col("*") refers to every column, so to_data_frame renames a column named "*"
# to this name while it filters and selects, and _column refers to it by this name.
_STAR_COLUMN_NAME = "__dagster_iceberg_star__"


def _column(name: str) -> da.Expression:
    """Refers to the column with exactly this name."""
    return da.col(_STAR_COLUMN_NAME if name == "*" else name)


class DagsterPartitionToDaftExpressionMapper(
    DagsterPartitionToPredicateMapper[da.Expression]
):
    def _partition_filters_to_predicates(
        self,
        partition_expr: str,
        partition_filters: Sequence[str],
    ) -> da.Expression:
        return _column(partition_expr).is_in(list(partition_filters))

    def _time_window_partition_filters_to_predicates(
        self,
        partition_expr: str,
        start_dt: dt.date | dt.datetime,
        end_dt: dt.date | dt.datetime,
    ) -> da.Expression:
        column = _column(partition_expr)
        return (column >= da.lit(start_dt)) & (column < da.lit(end_dt))


class _DaftIcebergTypeHandler(_handler.IcebergBaseTypeHandler[da.DataFrame]):
    """Type handler that converts data between Iceberg tables and Daft DataFrames."""

    def to_data_frame(
        self,
        table: ibt.Table,
        table_slice: TableSlice,
        target_type: type[da.DataFrame],
    ) -> da.DataFrame:
        ddf = table.to_daft()
        has_star_column = "*" in ddf.column_names
        if has_star_column:
            if _STAR_COLUMN_NAME in ddf.column_names:
                raise ValueError(
                    "Daft cannot read a table with columns named both '*' and "
                    f"'{_STAR_COLUMN_NAME}'."
                )
            ddf = ddf.with_column_renamed("*", _STAR_COLUMN_NAME)
        if table_slice.partition_dimensions:
            predicates = DagsterPartitionToDaftExpressionMapper(
                partition_dimensions=table_slice.partition_dimensions,
                table_schema=table.schema(),
                table_partition_spec=table.spec(),
            ).partition_dimensions_to_filters()
            for predicate in predicates:
                ddf = ddf.where(predicate)
        if table_slice.columns is not None:
            ddf = ddf.select(*(_column(column) for column in table_slice.columns))
        if has_star_column:
            ddf = ddf.with_column_renamed(_STAR_COLUMN_NAME, "*")
        return ddf

    def to_arrow(self, obj: da.DataFrame) -> pa.Table:
        return obj.to_arrow()

    @property
    def supported_types(self) -> Sequence[type[object]]:
        return [da.DataFrame]


@public
@preview
class DaftIcebergIOManager(_io_manager.IcebergIOManager):
    """An I/O manager definition that reads inputs from and writes outputs to Iceberg tables using Daft.

    Examples:
        .. code-block:: python

            import daft as da
            import pandas as pd
            from dagster import Definitions, asset
            from dagster_iceberg.config import IcebergCatalogConfig
            from dagster_iceberg.io_manager.daft import DaftIcebergIOManager

            CATALOG_URI = "sqlite:////home/vscode/workspace/.tmp/examples/select_columns/catalog.db"
            CATALOG_WAREHOUSE = (
                "file:///home/vscode/workspace/.tmp/examples/select_columns/warehouse"
            )

            resources = {
                "io_manager": DaftIcebergIOManager(
                    name="test",
                    config=IcebergCatalogConfig(
                        properties={"uri": CATALOG_URI, "warehouse": CATALOG_WAREHOUSE}
                    ),
                    namespace="dagster",
                )
            }


            @asset
            def iris_dataset() -> da.DataFrame:
                return da.from_pandas(
                    pd.read_csv(
                        "https://docs.dagster.io/assets/iris.csv",
                        names=[
                            "sepal_length_cm",
                            "sepal_width_cm",
                            "petal_length_cm",
                            "petal_width_cm",
                            "species",
                        ],
                    )
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
            def make_my_table() -> da.DataFrame:
                ...

        To only use specific columns of a table as input to a downstream op or asset, add the metadata "columns" to the
        ``In`` or ``AssetIn``.

        .. code-block:: python

            @asset(
                ins={"my_table": AssetIn("my_table", metadata={"columns": ["a"]})}
            )
            def my_table_a(my_table: da.DataFrame):
                # my_table will just contain the data from column "a"
                ...

    """

    @staticmethod
    def type_handlers() -> Sequence[DbTypeHandler]:
        return [_DaftIcebergTypeHandler()]
