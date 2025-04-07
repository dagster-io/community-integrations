from collections.abc import Sequence

try:
    import polars as pl
except ImportError as e:
    raise ImportError("Please install dagster-iceberg with the 'polars' extra.") from e
import pyarrow as pa
from dagster._annotations import public
from dagster._core.storage.db_io_manager import DbTypeHandler, TableSlice
from pyiceberg import table as ibt

from dagster_iceberg import handler as _handler
from dagster_iceberg import io_manager as _io_manager
from dagster_iceberg._utils import DagsterPartitionToPolarsSqlPredicateMapper, preview


class _PolarsIcebergTypeHandler(
    _handler.IcebergBaseTypeHandler[pl.LazyFrame | pl.DataFrame],
):
    """Type handler that converts data between Iceberg tables and polars DataFrames"""

    def to_data_frame(
        self,
        table: ibt.Table,
        table_slice: TableSlice,
        target_type: type[pl.LazyFrame | pl.DataFrame],
    ) -> pl.LazyFrame | pl.DataFrame:
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

        pdf = pl.scan_iceberg(source=table)

        stmt = f"SELECT {selected_fields} FROM self"
        if row_filter is not None:
            stmt += f"\nWHERE {row_filter}"
        return pdf.sql(stmt) if target_type == pl.LazyFrame else pdf.sql(stmt).collect()

    def to_arrow(self, obj: pl.LazyFrame | pl.DataFrame) -> pa.Table:
        if isinstance(obj, pl.LazyFrame):
            return obj.collect().to_arrow()
        return obj.to_arrow()

    @property
    def supported_types(self) -> Sequence[type[object]]:
        return (pl.LazyFrame, pl.DataFrame)


@preview
@public
class PolarsIcebergIOManager(_io_manager.IcebergIOManager):
    """An IO manager definition that reads inputs from and writes outputs to Iceberg tables using Polars.

    Examples:

    ```python
    import pandas as pd
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
        return pl.from_pandas(
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
    ```

    If you do not provide a schema, Dagster will determine a schema based on the assets and ops using
    the I/O Manager. For assets, the schema will be determined from the asset key, as in the above example.
    For ops, the schema can be specified by including a "schema" entry in output metadata. If none
    of these is provided, the schema will default to "public". The I/O manager will check if the namespace
    exists in the iceberg catalog. It does not automatically create the namespace if it does not exist.

    ```python
    @op(
        out={"my_table": Out(metadata={"schema": "my_schema"})}
    )
    def make_my_table() -> pd.DataFrame:
        ...
    ```

    To only use specific columns of a table as input to a downstream op or asset, add the metadata "columns" to the
    In or AssetIn.

    ```python
    @asset(
        ins={"my_table": AssetIn("my_table", metadata={"columns": ["a"]})}
    )
    def my_table_a(my_table: pd.DataFrame):
        # my_table will just contain the data from column "a"
        ...
    ```
    """

    @staticmethod
    def type_handlers() -> Sequence[DbTypeHandler]:
        return [_PolarsIcebergTypeHandler()]
