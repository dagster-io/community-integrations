from typing import Sequence, Type, Union

try:
    import polars as pl
except ImportError as e:
    raise ImportError("Please install dagster-iceberg with the 'polars' extra.") from e
import pyarrow as pa
from dagster._core.storage.db_io_manager import TableSlice
from pyiceberg import table as ibt

from dagster_iceberg.type_handlers._base import IcebergBaseTypeHandler
from dagster_iceberg._utils import DagsterPartitionToPolarsSqlPredicateMapper


class _IcebergPolarsTypeHandler(
    IcebergBaseTypeHandler[Union[pl.LazyFrame, pl.DataFrame]]
):
    """Type handler that converts data between Iceberg tables and polars DataFrames"""

    def to_data_frame(
        self,
        table: ibt.Table,
        table_slice: TableSlice,
        target_type: Type[Union[pl.LazyFrame, pl.DataFrame]],
    ) -> Union[pl.LazyFrame, pl.DataFrame]:
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

    def to_arrow(self, obj: Union[pl.LazyFrame, pl.DataFrame]) -> pa.Table:
        if isinstance(obj, pl.LazyFrame):
            return obj.collect().to_arrow()
        else:
            return obj.to_arrow()

    @property
    def supported_types(self) -> Sequence[Type[object]]:
        return (pl.LazyFrame, pl.DataFrame)
