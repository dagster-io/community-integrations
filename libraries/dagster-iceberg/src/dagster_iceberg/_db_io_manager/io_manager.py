from collections.abc import Mapping
from typing import Any, cast

from dagster import (
    InputContext,
    MultiPartitionsDefinition,
    OutputContext,
    TimeWindowPartitionsDefinition,
)
from dagster._core.definitions.metadata import ArbitraryMetadataMapping
from dagster._core.storage.db_io_manager import (
    DbIOManager,
    TablePartitionDimension,
    TableSlice,
)

from .utils import (
    generate_multi_partitions_dimension,
    generate_single_partition_dimension,
)


class CustomDbIOManager(DbIOManager):
    """Works exactly like the DbIOManager, but overrides the _get_table_slice method
    to allow user to pass multiple partitions to select. This happens e.g. when a user makes
    a mapping from partition A to partition B, where A is partitioned on two dimensions and
    B is partitioned on only one dimension.

        See:
            - Issue: <https://github.com/dagster-io/dagster/issues/17838>
            - Open PR: <https://github.com/dagster-io/dagster/pull/20400>
    """

    def _get_schema(
        self,
        context: OutputContext | InputContext,
        output_context_metadata: ArbitraryMetadataMapping | dict[str, Any],
    ) -> str:
        asset_key_path = context.asset_key.path
        # schema order of precedence: metadata, I/O manager 'schema' config, key_prefix
        if output_context_metadata.get("schema"):
            schema = cast(str, output_context_metadata["schema"])
        elif self._schema:
            schema = self._schema
        elif len(asset_key_path) > 1:
            schema = asset_key_path[-2]
        else:
            schema = "public"
        return schema

    def _get_table_slice(
        self,
        context: OutputContext | InputContext,
        output_context: OutputContext,
    ) -> TableSlice:
        output_context_metadata = output_context.definition_metadata or {}

        schema: str
        table: str
        partition_dimensions: list[TablePartitionDimension] = []
        if context.has_asset_key:
            table = context.asset_key.path[-1]
            schema = self._get_schema(context, output_context_metadata)

            if context.has_asset_partitions:
                partition_expr = output_context_metadata.get("partition_expr")
                if partition_expr is None:
                    raise ValueError(
                        f"Asset '{context.asset_key}' has partitions, but no 'partition_expr'"
                        " metadata value, so we don't know what column it's partitioned on. To"
                        " specify a column, set this metadata value. E.g."
                        ' @asset(metadata={"partition_expr": "your_partition_column"}).',
                    )
                if isinstance(context.asset_partitions_def, MultiPartitionsDefinition):
                    partition_dimensions = generate_multi_partitions_dimension(
                        asset_partition_keys=context.asset_partition_keys,
                        asset_partitions_def=context.asset_partitions_def,
                        partition_expr=cast(Mapping[str, str], partition_expr),
                        asset_key=context.asset_key,
                    )
                else:
                    partition_dimensions = [
                        generate_single_partition_dimension(
                            partition_expr=cast(str, partition_expr),
                            asset_partition_keys=context.asset_partition_keys,
                            asset_partitions_time_window=(
                                context.asset_partitions_time_window
                                if isinstance(
                                    context.asset_partitions_def,
                                    TimeWindowPartitionsDefinition,
                                )
                                else None
                            ),
                        ),
                    ]
        else:
            table = output_context.name
            if output_context_metadata.get("schema"):
                schema = cast(str, output_context_metadata["schema"])
            elif self._schema:
                schema = self._schema
            else:
                schema = "public"

        return TableSlice(
            table=table,
            schema=schema,
            database=self._database,
            partition_dimensions=partition_dimensions,
            columns=(context.definition_metadata or {}).get("columns"),
        )
