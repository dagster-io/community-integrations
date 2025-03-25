from typing import Sequence, Type

try:
    import pandas as pd
except ImportError as e:
    raise ImportError("Please install dagster-iceberg with the 'pandas' extra.") from e
import pyarrow as pa
from dagster import InputContext
from dagster._core.storage.db_io_manager import TableSlice
from pyiceberg.catalog import Catalog

from dagster_iceberg.type_handlers._arrow import _IcebergPyArrowTypeHandler


class _IcebergPandasTypeHandler(_IcebergPyArrowTypeHandler):
    """Type handler that converts data between Iceberg tables and pyarrow Tables"""

    def to_arrow(self, obj: pd.DataFrame) -> pa.Table:
        return pa.Table.from_pandas(obj)

    def load_input(
        self,
        context: InputContext,
        table_slice: TableSlice,
        connection: Catalog,
    ) -> pd.DataFrame:
        """Loads the input using a dataframe implmentation"""
        tbl: pa.Table = self.to_data_frame(
            table=connection.load_table(f"{table_slice.schema}.{table_slice.table}"),
            table_slice=table_slice,
            target_type=pa.RecordBatchReader,
        )
        return tbl.read_pandas()

    @property
    def supported_types(self) -> Sequence[Type[object]]:
        return [pd.DataFrame]
