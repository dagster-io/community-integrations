import datetime as dt
from collections.abc import Mapping, Sequence
from typing import cast

from dagster import (
    AssetKey,
    MultiPartitionKey,
    MultiPartitionsDefinition,
    TimeWindowPartitionsDefinition,
)
from dagster._core.definitions.time_window_partitions import TimeWindow
from dagster._core.storage.db_io_manager import TablePartitionDimension
from pendulum import instance as pdi

from dagster_iceberg._utils.transforms import date_diff


class MultiTimePartitionsChecker:
    def __init__(self, partitions: list[TimeWindow]):
        """Helper class that defines checks on a list of TimeWindow objects
        most importantly, partitions should be consecutive.

        Args:
            partitions (List[TimeWindow]): List of TimeWindow objects
        """

        self._partitions = partitions

    @property
    def start(self) -> dt.datetime:
        date_ = min([w.start for w in self._partitions])
        if not isinstance(date_, dt.datetime):
            raise ValueError("Start date is not a datetime")
        return date_

    @property
    def end(self) -> dt.datetime:
        date_ = max([w.end for w in self._partitions])
        if not isinstance(date_, dt.datetime):
            raise ValueError("End date is not a datetime")
        return date_

    @property
    def hourly_deltas(self) -> list[int]:
        deltas = [date_diff(w.start, w.end).in_hours() for w in self._partitions]
        if len(set(deltas)) != 1:
            raise ValueError(
                "TimeWindowPartitionsDefinition must have the same delta from start to end",
            )
        return deltas

    @property
    def hourly_delta(self) -> int:
        try:
            return next(iter(set(self.hourly_deltas)))
        except StopIteration as exc:
            raise ValueError(
                "TimeWindowPartitionsDefinition must have at least one partition",
            ) from exc

    def is_consecutive(self):
        return (
            len(
                {
                    pdi(self.start).add(hours=self.hourly_delta * i)
                    for i in range(date_diff(self.start, self.end).in_days() + 1)
                }
                - {pdi(d.start) for d in self._partitions},
            )
            == 1
        )


def generate_multi_partitions_dimension(
    asset_partition_keys: Sequence[str],
    asset_partitions_def: MultiPartitionsDefinition,
    partition_expr: Mapping[str, str],
    asset_key: AssetKey,
) -> list[TablePartitionDimension]:
    """Given a MultiPartitionsDefinition, generate a list of TablePartitionDimension objects that can be
    used to create a TableSlice object.

    Args:
        asset_partition_keys (Sequence[str]): Partition keys for the asset
        asset_partitions_def (MultiPartitionsDefinition): The PartitionDefinition object for the asset
        partition_expr (Mapping[str, str]): The partition expression for the asset partition
        asset_key (AssetKey): The AssetKey for the asset that is being stored

    Raises:
        ValueError: If the asset has a partition but the partition_expr metadata does not contain a key for the partition
        ValueError: If the dates for a TimeWindow partition are not consecutive
        ValueError: If the partition type is unknown

    Returns:
        List[TablePartitionDimension]: List of TablePartitionDimension objects
    """
    partition_dimensions: list[TablePartitionDimension] = []
    multi_partition_key_mappings = [
        cast(MultiPartitionKey, partition_key).keys_by_dimension
        for partition_key in asset_partition_keys
    ]
    for part in asset_partitions_def.partitions_defs:
        partitions: list[TimeWindow | str] = []
        for multi_partition_key_mapping in multi_partition_key_mappings:
            partition_key = multi_partition_key_mapping[part.name]
            if isinstance(part.partitions_def, TimeWindowPartitionsDefinition):
                partitions.append(
                    part.partitions_def.time_window_for_partition_key(partition_key),
                )
            else:
                partitions.append(partition_key)

        partition_expr_str = partition_expr.get(part.name)
        if partition_expr is None:
            raise ValueError(
                f"Asset '{asset_key}' has partition {part.name}, but the"
                f" 'partition_expr' metadata does not contain a {part.name} entry,"
                " so we don't know what column to filter it on. Specify which"
                " column of the database contains data for the"
                f" {part.name} partition.",
            )
        partitions_: TimeWindow | Sequence[str]
        if all(isinstance(partition, TimeWindow) for partition in partitions):
            checker = MultiTimePartitionsChecker(
                partitions=cast(list[TimeWindow], partitions),
            )
            if not checker.is_consecutive():
                raise ValueError("Dates are not consecutive.")
            partitions_ = TimeWindow(
                start=checker.start,
                end=checker.end,
            )
        elif all(isinstance(partition, str) for partition in partitions):
            partitions_ = list(set(cast(list[str], partitions)))
        else:
            raise ValueError("Unknown partition type")
        partition_dimensions.append(
            TablePartitionDimension(
                partition_expr=cast(str, partition_expr_str),
                partitions=partitions_,
            ),
        )
    return partition_dimensions


def generate_single_partition_dimension(
    partition_expr: str,
    asset_partition_keys: Sequence[str],
    asset_partitions_time_window: TimeWindow | None,
) -> TablePartitionDimension:
    """Given a single partition, generate a TablePartitionDimension object that can be used to create a TableSlice object.

    Args:
        partition_expr (str): Partition expression for the asset partition
        asset_partition_keys (Sequence[str]): Partition keys for the asset
        asset_partitions_time_window (TimeWindow | None): TimeWindow object for the asset partition

    Returns:
        TablePartitionDimension: TablePartitionDimension object
    """
    partition_dimension: TablePartitionDimension
    if isinstance(asset_partitions_time_window, TimeWindow):
        partition_dimension = TablePartitionDimension(
            partition_expr=partition_expr,
            partitions=(asset_partitions_time_window if asset_partition_keys else []),
        )
    else:
        partition_dimension = TablePartitionDimension(
            partition_expr=partition_expr,
            partitions=asset_partition_keys,
        )
    return partition_dimension
