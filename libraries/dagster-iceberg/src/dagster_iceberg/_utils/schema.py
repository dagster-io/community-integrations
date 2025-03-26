import logging
from functools import cached_property

import pyarrow as pa
from pyiceberg import table

from dagster_iceberg._utils.retries import IcebergOperationWithRetry


def update_table_schema(
    table: table.Table,
    new_table_schema: pa.Schema,
    schema_update_mode: str,
):
    IcebergSchemaUpdaterWithRetry(table=table).execute(
        retries=6,
        exception_types=ValueError,
        new_table_schema=new_table_schema,
        schema_update_mode=schema_update_mode,
    )


class IcebergSchemaUpdaterWithRetry(IcebergOperationWithRetry):
    def operation(self, new_table_schema: pa.Schema, schema_update_mode: str):
        IcebergTableSchemaUpdater(
            schema_differ=SchemaDiffer(
                current_table_schema=self.table.schema().as_arrow(),
                new_table_schema=new_table_schema,
            ),
            schema_update_mode=schema_update_mode,
        ).update_table_schema(self.table)


class SchemaDiffer:
    def __init__(self, current_table_schema: pa.Schema, new_table_schema: pa.Schema):
        self.current_table_schema = current_table_schema
        self.new_table_schema = new_table_schema

    @property
    def has_changes(self) -> bool:
        return sorted(self.current_table_schema.names) != sorted(
            self.new_table_schema.names
        )

    @cached_property
    def deleted_columns(self) -> list[str]:
        return list(
            set(self.current_table_schema.names) - set(self.new_table_schema.names),
        )

    @cached_property
    def new_columns(self) -> list[str]:
        return list(
            set(self.new_table_schema.names) - set(self.current_table_schema.names),
        )


class IcebergTableSchemaUpdater:
    def __init__(
        self,
        schema_differ: SchemaDiffer,
        schema_update_mode: str,
    ):
        self.schema_update_mode = schema_update_mode
        self.schema_differ = schema_differ
        self.logger = logging.getLogger(
            "dagster_iceberg._utils.schema.IcebergTableSchemaUpdater",
        )

    def update_table_schema(self, table: table.Table):
        if self.schema_update_mode == "error" and self.schema_differ.has_changes:
            raise ValueError(
                "Schema spec update mode is set to 'error' but there are schema changes to the Iceberg table",
            )
        if not self.schema_differ.has_changes:
            return
        with table.update_schema() as update:
            for column in self.schema_differ.deleted_columns:
                self.logger.debug("Deleting column '%s' from schema", column)
                update.delete_column(column)
            if self.schema_differ.new_columns:
                self.logger.debug(
                    "Merging schemas with new columns %s",
                    self.schema_differ.new_columns,
                )
                update.union_by_name(self.schema_differ.new_table_schema)
