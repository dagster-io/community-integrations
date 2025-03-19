from typing import Sequence, Type

try:
    import daft as da
except ImportError as e:
    raise ImportError("Please install dagster-iceberg with the 'daft' extra.") from e
import pyarrow as pa
from dagster._core.storage.db_io_manager import TableSlice
from pyiceberg import table as ibt

from dagster_iceberg.type_handlers._base import IcebergBaseTypeHandler
from dagster_iceberg._utils import DagsterPartitionToDaftSqlPredicateMapper


class _IcebergDaftTypeHandler(IcebergBaseTypeHandler[da.DataFrame]):
    """Type handler that converts data between Iceberg tables and polars DataFrames"""

    def to_data_frame(
        self, table: ibt.Table, table_slice: TableSlice, target_type: Type[da.DataFrame]
    ) -> da.DataFrame:
        selected_fields: str = (
            ",".join(table_slice.columns) if table_slice.columns is not None else "*"
        )
        row_filter: str | None = None
        if table_slice.partition_dimensions:
            expressions = DagsterPartitionToDaftSqlPredicateMapper(
                partition_dimensions=table_slice.partition_dimensions,
                table_schema=table.schema(),
                table_partition_spec=table.spec(),
            ).partition_dimensions_to_filters()
            row_filter = " AND ".join(expressions)

        ddf = table.to_daft()  # type: ignore # noqa

        stmt = f"SELECT {selected_fields} FROM ddf"
        if row_filter is not None:
            stmt += f"\nWHERE {row_filter}"

        return da.sql(stmt)

    def to_arrow(self, obj: da.DataFrame) -> pa.Table:
        return obj.to_arrow()

    @property
    def supported_types(self) -> Sequence[Type[object]]:
        return [da.DataFrame]
