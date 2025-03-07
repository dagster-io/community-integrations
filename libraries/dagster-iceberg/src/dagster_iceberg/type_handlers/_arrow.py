from typing import Sequence, Tuple, Type, Union

import pyarrow as pa
from dagster._core.storage.db_io_manager import TableSlice
from pyiceberg import expressions as E
from pyiceberg import table as ibt

from dagster_iceberg.type_handlers._base import IcebergBaseTypeHandler
from dagster_iceberg._utils import DagsterPartitionToIcebergExpressionMapper

ArrowTypes = Union[pa.Table, pa.RecordBatchReader]


class _IcebergPyArrowTypeHandler(IcebergBaseTypeHandler[ArrowTypes]):
    """Type handler that converts data between Iceberg tables and pyarrow Tables"""

    def to_data_frame(
        self, table: ibt.Table, table_slice: TableSlice, target_type: Type[ArrowTypes]
    ) -> ArrowTypes:
        selected_fields: Tuple[str, ...] = (
            tuple(table_slice.columns) if table_slice.columns is not None else ("*",)
        )
        row_filter: E.BooleanExpression
        if table_slice.partition_dimensions:
            expressions = DagsterPartitionToIcebergExpressionMapper(
                partition_dimensions=table_slice.partition_dimensions,
                table_schema=table.schema(),
                table_partition_spec=table.spec(),
            ).partition_dimensions_to_filters()
            row_filter = E.And(*expressions) if len(expressions) > 1 else expressions[0]
        else:
            row_filter = ibt.ALWAYS_TRUE

        table_scan = table.scan(row_filter=row_filter, selected_fields=selected_fields)

        return (
            table_scan.to_arrow()
            if target_type == pa.Table
            else table_scan.to_arrow_batch_reader()
        )

    def to_arrow(self, obj: ArrowTypes) -> pa.Table:
        return obj

    @property
    def supported_types(self) -> Sequence[Type[object]]:
        return (pa.Table, pa.RecordBatchReader)
