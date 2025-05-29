import enum
from abc import abstractmethod
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import TypedDict, cast

from dagster import OutputContext
from dagster._annotations import public
from dagster._config.pythonic_config import ConfigurableIOManagerFactory
from dagster._core.definitions.time_window_partitions import TimeWindow
from dagster._core.storage.db_io_manager import (
    DbClient,
    DbIOManager,
    DbTypeHandler,
    TablePartitionDimension,
    TableSlice,
)
from pydantic import Field
from pyiceberg.catalog import Catalog, load_catalog

from dagster_iceberg._db_io_manager import CustomDbIOManager
from dagster_iceberg._utils import preview
from dagster_iceberg.config import IcebergCatalogConfig


class PartitionSpecUpdateMode(enum.Enum):
    error = "error"
    update = "update"


class SchemaUpdateMode(enum.Enum):
    error = "error"
    update = "update"


class DbIOManagerImplementation(enum.Enum):
    default = "default"
    custom = "custom"


class _IcebergCatalogProperties(TypedDict):
    properties: dict[str, str]


class _IcebergTableIOManagerResourceConfig(TypedDict):
    name: str
    config: _IcebergCatalogProperties | None
    schema_: str | None
    db_io_manager: DbIOManagerImplementation
    partition_spec_update_mode: PartitionSpecUpdateMode
    schema_update_mode: SchemaUpdateMode


@preview
class IcebergDbClient(DbClient):
    @staticmethod
    def delete_table_slice(
        context: OutputContext,
        table_slice: TableSlice,
        connection: Catalog,
    ) -> None: ...

    @staticmethod
    def ensure_schema_exists(
        context: OutputContext,
        table_slice: TableSlice,
        connection: Catalog,
    ) -> None:
        connection.list_namespaces(table_slice.schema)

    @staticmethod
    def get_select_statement(table_slice: TableSlice) -> str:
        # The select statement here is just for illustrative purposes,
        # and is never actually executed. It does however logically correspond
        # the operation being executed.
        col_str = ", ".join(table_slice.columns) if table_slice.columns else "*"

        if (
            table_slice.partition_dimensions
            and len(table_slice.partition_dimensions) > 0
        ):
            query = f"SELECT {col_str} FROM {table_slice.schema}.{table_slice.table} WHERE\n"
            return query + _partition_where_clause(table_slice.partition_dimensions)

        return f"""SELECT {col_str} FROM {table_slice.schema}.{table_slice.table}"""

    @staticmethod
    @contextmanager
    def connect(context, table_slice: TableSlice) -> Iterator[Catalog]:
        resource_config = cast(
            "_IcebergTableIOManagerResourceConfig",
            context.resource_config,
        )
        # Config passed as env variables or using config file.
        #  See: https://py.iceberg.apache.org/configuration/
        if resource_config["config"] is None:
            yield load_catalog(name=resource_config["name"])
        else:
            yield load_catalog(
                name=resource_config["name"],
                **resource_config["config"]["properties"],
            )


@public
@preview
class IcebergIOManager(ConfigurableIOManagerFactory):
    """Base class for an I/O manager definition that reads inputs from and writes outputs to Iceberg tables.

    Examples:
        .. code-block:: python

            import pandas as pd
            import pyarrow as pa
            from dagster import Definitions, asset
            from dagster_iceberg.config import IcebergCatalogConfig
            from dagster_iceberg.io_manager.arrow import PyArrowIcebergIOManager

            CATALOG_URI = "sqlite:////home/vscode/workspace/.tmp/examples/select_columns/catalog.db"
            CATALOG_WAREHOUSE = (
                "file:///home/vscode/workspace/.tmp/examples/select_columns/warehouse"
            )


            resources = {
                "io_manager": PyArrowIcebergIOManager(
                    name="test",
                    config=IcebergCatalogConfig(
                        properties={"uri": CATALOG_URI, "warehouse": CATALOG_WAREHOUSE}
                    ),
                    namespace="dagster",
                )
            }


            @asset
            def iris_dataset() -> pa.Table:
                pa.Table.from_pandas(
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
            def make_my_table() -> pa.Table:
                ...

        To only use specific columns of a table as input to a downstream op or asset, add the metadata "columns" to the
        ``In`` or ``AssetIn``.

        .. code-block:: python

            @asset(
                ins={"my_table": AssetIn("my_table", metadata={"columns": ["a"]})}
            )
            def my_table_a(my_table: pa.Table):
                # my_table will just contain the data from column "a"
                ...

    """

    name: str = Field(description="The name of the iceberg catalog.")
    config: IcebergCatalogConfig | None = Field(
        description="Additional configuration properties for the iceberg catalog. See <https://py.iceberg.apache.org/configuration/>"
        " for passing these as environment variables or using a configuration file.",
        default=None,
    )
    schema_: str | None = Field(
        default=None,
        alias="namespace",
        description="Name of the iceberg catalog namespace to use.",
    )  # schema is a reserved word for pydantic
    db_io_manager: DbIOManagerImplementation = Field(
        default=DbIOManagerImplementation.default,
        description="The implementation of the DbIOManager to use. 'default' uses the dagster default 'DbIOManager'."
        " 'custom' uses the custom 'CustomDbIOManager' that allows you to use additional mappings. See <docs>.",
    )

    @staticmethod
    @abstractmethod
    def type_handlers() -> Sequence[DbTypeHandler]: ...

    @staticmethod
    def default_load_type() -> type | None:
        return None

    def create_io_manager(self, context) -> DbIOManager:
        if self.config is not None:
            self.config.model_dump()
        IOManagerImplementation = (
            DbIOManager
            if self.db_io_manager == DbIOManagerImplementation.default
            else CustomDbIOManager
        )
        return IOManagerImplementation(
            type_handlers=self.type_handlers(),
            db_client=IcebergDbClient(),
            database=self.name,
            schema=self.schema_,
            io_manager_name="IcebergIOManager",
            default_load_type=self.default_load_type(),
        )


def _partition_where_clause(
    partition_dimensions: Sequence[TablePartitionDimension],
) -> str:
    return " AND\n".join(
        (
            _time_window_where_clause(partition_dimension)
            if isinstance(partition_dimension.partitions, TimeWindow)
            else _static_where_clause(partition_dimension)
        )
        for partition_dimension in partition_dimensions
    )


def _time_window_where_clause(table_partition: TablePartitionDimension) -> str:
    partition = cast("TimeWindow", table_partition.partitions)
    start_dt, end_dt = partition
    start_dt_str = start_dt.isoformat()
    end_dt_str = end_dt.isoformat()
    return f"""{table_partition.partition_expr} >= '{start_dt_str}' AND {table_partition.partition_expr} < '{end_dt_str}'"""


def _static_where_clause(table_partition: TablePartitionDimension) -> str:
    partitions = ", ".join(f"'{partition}'" for partition in table_partition.partitions)
    return f"""{table_partition.partition_expr} in ({partitions})"""
