from abc import abstractmethod
from typing import TYPE_CHECKING, Generic, TypeVar, cast

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

from dagster_iceberg._utils import (
    DEFAULT_PARTITION_FIELD_NAME_PREFIX,
    DEFAULT_WRITE_MODE,
    WriteMode,
    preview,
    table_writer,
)
from dagster_iceberg._utils.io import UpsertOptions
from dagster_iceberg.config import IcebergCatalogConfig

if TYPE_CHECKING:
    from pyiceberg.table.snapshots import Snapshot

U = TypeVar("U")

ArrowTypes = pa.Table | pa.RecordBatchReader


@public
@preview
class IcebergBaseTypeHandler(DbTypeHandler[U], Generic[U]):
    """Base class for a type handler that reads inputs from and writes outputs to Iceberg tables."""

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

        partition_field_name_prefix = self._get_partition_field_name_prefix(context)
        write_mode_with_output_override = self._get_write_mode(context)
        upsert_options = self._get_upsert_options(
            context, write_mode_with_output_override
        )

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
            write_mode=write_mode_with_output_override,
            partition_field_name_prefix=partition_field_name_prefix,
            upsert_options=upsert_options,
        )

        table_ = connection.load_table(f"{table_slice.schema}.{table_slice.table}")

        current_snapshot = cast("Snapshot", table_.current_snapshot())

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

    def _get_partition_field_name_prefix(self, context: OutputContext) -> str:
        """Get partition_field_name_prefix from asset definition metadata if available, otherwise fall back to IO manager config."""
        if (
            context.resource_config is None
        ):  # This doesn't seem to ever actually happen, but that's the way OutputContext is typed
            raise ValueError(
                "Resource config is required to get partition_field_name_prefix. Unexpected value None found for resource_config."
            )

        config = context.resource_config.get("config", {})
        if isinstance(config, dict):
            partition_field_name_prefix = config.get(
                "partition_field_name_prefix", DEFAULT_PARTITION_FIELD_NAME_PREFIX
            )
        elif isinstance(config, IcebergCatalogConfig):
            partition_field_name_prefix = config.partition_field_name_prefix
        else:
            raise ValueError(
                f"Unable to retrieve partition_field_name_prefix from `config` attribute of resource_config with unexpected type {type(config)}"
            )

        return context.definition_metadata.get(
            "partition_field_name_prefix", partition_field_name_prefix
        )

    def _get_write_mode(self, context: OutputContext) -> WriteMode:
        """Get write mode from asset definition metadata if available, otherwise from output metadata"""
        try:
            definition_write_mode = WriteMode(
                context.definition_metadata.get("write_mode", DEFAULT_WRITE_MODE)
            )
            return WriteMode(
                context.output_metadata.get("write_mode", definition_write_mode).value
            )
        except ValueError as ve:
            error_msg = f"Invalid write mode: {context.output_metadata.get('write_mode')}. Valid modes are {[mode.value for mode in WriteMode]}"
            raise ValueError(error_msg) from ve

    def _get_upsert_options(
        self, context: OutputContext, write_mode: WriteMode
    ) -> UpsertOptions | None:
        """Get upsert options from output metadata if available, otherwise from definition metadata.
        Returns None if no upsert options are found in asset definition or output metadata.

         Raises:
             ValueError: If upsert options are not provided when using upsert write mode.
        """
        upsert_options = None
        if write_mode == WriteMode.upsert:
            # Output metadata takes precedence over definition metadata
            output_upsert_options = context.output_metadata.get("upsert_options", {})
            definition_upsert_options = context.definition_metadata.get(
                "upsert_options", {}
            )
            upsert_options = {**definition_upsert_options, **output_upsert_options}
            if upsert_options:
                upsert_options = UpsertOptions.model_validate(upsert_options)
            else:
                raise ValueError(
                    "upsert_options must be provided when using upsert write mode, either in definition metadata or output metadata"
                )
        elif context.output_metadata.get(
            "upsert_options"
        ) or context.definition_metadata.get("upsert_options"):
            context.log.debug(
                "upsert_options detected but write_mode=%s is not 'upsert'. Ignoring upsert_options.",
                write_mode,
            )
        return upsert_options

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
