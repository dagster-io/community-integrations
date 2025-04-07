from collections.abc import Sequence

try:
    import pandas as pd
except ImportError as e:
    raise ImportError("Please install dagster-iceberg with the 'pandas' extra.") from e
import pyarrow as pa
from dagster import InputContext
from dagster._annotations import public
from dagster._core.storage.db_io_manager import DbTypeHandler, TableSlice
from pyiceberg.catalog import Catalog

from dagster_iceberg import io_manager as _io_manager
from dagster_iceberg._utils import preview
from dagster_iceberg.io_manager.arrow import _PyArrowIcebergTypeHandler


class _PandasIcebergTypeHandler(_PyArrowIcebergTypeHandler):
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
    def supported_types(self) -> Sequence[type[object]]:
        return [pd.DataFrame]


@preview
@public
class PandasIcebergIOManager(_io_manager.IcebergIOManager):
    """An IO manager definition that reads inputs from and writes outputs to Iceberg tables using Pandas.

    Examples:

    ```python
    import pandas as pd
    from dagster import Definitions, asset

    from dagster_iceberg.config import IcebergCatalogConfig
    from dagster_iceberg.io_manager.pandas import PandasIcebergIOManager

    CATALOG_URI = "sqlite:////home/vscode/workspace/.tmp/examples/select_columns/catalog.db"
    CATALOG_WAREHOUSE = (
        "file:///home/vscode/workspace/.tmp/examples/select_columns/warehouse"
    )


    resources = {
        "io_manager": PandasIcebergIOManager(
            name="test",
            config=IcebergCatalogConfig(
                properties={"uri": CATALOG_URI, "warehouse": CATALOG_WAREHOUSE}
            ),
            namespace="dagster",
        )
    }


    @asset
    def iris_dataset() -> pd.DataFrame:
        return pd.read_csv(
            "https://docs.dagster.io/assets/iris.csv",
            names=[
                "sepal_length_cm",
                "sepal_width_cm",
                "petal_length_cm",
                "petal_width_cm",
                "species",
            ],
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
        return [_PandasIcebergTypeHandler()]
