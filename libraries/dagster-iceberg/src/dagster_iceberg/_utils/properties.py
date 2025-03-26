import logging
from functools import cached_property

from pyiceberg import table
from pyiceberg.exceptions import CommitFailedException

from dagster_iceberg._utils.retries import IcebergOperationWithRetry


def update_table_properties(
    table: table.Table,
    current_table_properties: dict,
    new_table_properties: dict,
):
    """Update existing table properties with new table properties passed by user"""
    IcebergPropertiesUpdaterWithRetry(table=table).execute(
        retries=3,
        exception_types=CommitFailedException,
        current_table_properties=current_table_properties,
        new_table_properties=new_table_properties,
    )


class IcebergPropertiesUpdaterWithRetry(IcebergOperationWithRetry):
    def operation(self, current_table_properties: dict, new_table_properties: dict):
        """Diffs current and new properties and updates where relevant"""
        IcebergTablePropertiesUpdater(
            table_properties_differ=TablePropertiesDiffer(
                current_table_properties=current_table_properties,
                new_table_properties=new_table_properties,
            ),
        ).update_table_properties(self.table, table_properties=new_table_properties)


class TablePropertiesDiffer:
    def __init__(self, current_table_properties: dict, new_table_properties: dict):
        """Helper class to diff current and new table properties

        Args:
            current_table_properties (dict): Dictionary of current table properties retrieve from iceberg Table
            new_table_properties (dict): Dictionary of new table properties passed by user
        """

        self.current_table_properties = current_table_properties
        self.new_table_properties = new_table_properties

    @property
    def has_changes(self) -> bool:
        return (
            len(self.updated_properties)
            + len(self.deleted_properties)
            + len(self.new_properties)
            != 0
        )

    @cached_property
    def updated_properties(self) -> list[str]:
        updated = []
        for k in self.new_table_properties:
            if (
                k in self.current_table_properties
                and self.current_table_properties[k] != self.new_table_properties[k]
            ):
                updated.append(k)  # noqa: PERF401, long `if` clause
        return updated

    @cached_property
    def deleted_properties(self) -> list[str]:
        return list(
            set(self.current_table_properties.keys())
            - set(self.new_table_properties.keys()),
        )

    @cached_property
    def new_properties(self) -> list[str]:
        return list(
            set(self.new_table_properties.keys())
            - set(self.current_table_properties.keys()),
        )


class IcebergTablePropertiesUpdater:
    def __init__(
        self,
        table_properties_differ: TablePropertiesDiffer,
    ):
        self.table_properties_differ = table_properties_differ
        self.logger = logging.getLogger(
            "dagster_iceberg._utils.schema.IcebergTablePropertiesUpdater",
        )

    @property
    def deleted_properties(self):
        return self.table_properties_differ.deleted_properties

    def update_table_properties(self, table: table.Table, table_properties: dict):
        """Updates Iceberg Table properties

        Args:
            table (table.Table): Pyiceberg Table
            table_properties (dict): New or updated table properties that
        """
        if not self.table_properties_differ.has_changes:
            return
        self.logger.debug("Updating table properties")
        with table.transaction() as tx:
            self.logger.debug(
                "Deleting table properties '%s'",
                self.deleted_properties,
            )
            tx.remove_properties(*self.deleted_properties)
            self.logger.debug(
                "Updating table properties if applicable using '%s'",
                table_properties,
            )
            tx.set_properties(table_properties)
