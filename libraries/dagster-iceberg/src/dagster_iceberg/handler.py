from abc import abstractmethod
from typing import Generic, TypeVar, cast

import pyarrow as pa
from dagster import (
    InputContext,
    MetadataValue,
    OutputContext,
    TableColumn,
    TableSchema,
)
from dagster._annotations import public
from dagster._core.storage.db_io_manager import DbTypeHandler, TableSlice
from pyiceberg import table as ibt
from pyiceberg.catalog import Catalog
from pyiceberg.table.snapshots import Snapshot

from dagster_iceberg._utils import preview, table_writer

U = TypeVar("U")

ArrowTypes = pa.Table | pa.RecordBatchReader


@public
@preview
class IcebergBaseTypeHandler(DbTypeHandler[U], Generic[U]):
    @abstractmethod
    def to_data_frame(
        self,
        table: ibt.Table,
        table_slice: TableSlice,
        target_type: type,
    ) -> U:
        pass

    @abstractmethod
    def to_arrow(self, obj: U) -> pa.Table:
        pass

    def handle_output(
        self,
        context: OutputContext,
        table_slice: TableSlice,
        obj: U,
        connection: Catalog,
    ):
        """Stores pyarrow types in Iceberg table"""
        metadata = context.definition_metadata or {}

        table_properties_usr = metadata.get("table_properties", {})
        partition_spec_update_mode = metadata.get("partition_spec_update_mode", "error")
        schema_update_mode = metadata.get("schema_update_mode", "error")

        table_writer(
            table_slice=table_slice,
            data=self.to_arrow(obj),
            catalog=connection,
            partition_spec_update_mode=partition_spec_update_mode,
            schema_update_mode=schema_update_mode,
            dagster_run_id=context.run_id,
            dagster_partition_key=(
                context.partition_key if context.has_asset_partitions else None
            ),
            table_properties=table_properties_usr,
        )

        table_ = connection.load_table(f"{table_slice.schema}.{table_slice.table}")

        current_snapshot = cast(Snapshot, table_.current_snapshot())

        context.add_output_metadata(
            {
                "table_columns": MetadataValue.table_schema(
                    TableSchema(
                        columns=[
                            TableColumn(name=f["name"], type=str(f["type"]))
                            for f in table_.schema().model_dump()["fields"]
                        ],
                    ),
                ),
                **current_snapshot.model_dump(),
            },
        )

    def load_input(
        self,
        context: InputContext,
        table_slice: TableSlice,
        connection: Catalog,
    ) -> U:
        """Loads the input using a dataframe implementation"""
        return self.to_data_frame(
            table=connection.load_table(f"{table_slice.schema}.{table_slice.table}"),
            table_slice=table_slice,
            target_type=context.dagster_type.typing_type,
        )
