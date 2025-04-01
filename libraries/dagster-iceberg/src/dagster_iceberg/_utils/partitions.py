import datetime as dt
import itertools
import logging
from abc import abstractmethod
from collections.abc import Iterable, Sequence
from typing import (
    Generic,
    TypeVar,
    cast,
)

from dagster._core.definitions.time_window_partitions import TimeWindow
from dagster._core.storage.db_io_manager import TablePartitionDimension, TableSlice
from pyiceberg import expressions as E
from pyiceberg import types as T
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.update.spec import UpdateSpec
from pyiceberg.transforms import IdentityTransform

from dagster_iceberg._utils.retries import IcebergOperationWithRetry
from dagster_iceberg._utils.transforms import diff_to_transformation

time_partition_dt_types = (T.TimestampType, T.DateType)
partition_types = T.StringType

K = TypeVar("K")


class DagsterPartitionToPredicateMapper(Generic[K]):
    def __init__(
        self,
        partition_dimensions: Iterable[TablePartitionDimension],
        table_schema: Schema,
        table_partition_spec: PartitionSpec | None = None,
    ):
        self.partition_dimensions = partition_dimensions
        self.table_schema = table_schema
        self.table_partition_spec = table_partition_spec

    @abstractmethod
    def _partition_filters_to_predicates(
        self,
        partition_expr: str,
        partition_filters: Sequence[str],
    ) -> K: ...

    @abstractmethod
    def _time_window_partition_filters_to_predicates(
        self,
        partition_expr: str,
        start_dt: dt.date | dt.datetime,
        end_dt: dt.date | dt.datetime,
    ) -> K: ...

    def _partition_filter(
        self,
        table_partition: TablePartitionDimension,
    ) -> K:
        return self._partition_filters_to_predicates(
            partition_expr=table_partition.partition_expr,
            partition_filters=cast(Sequence[str], table_partition.partitions),
        )

    def _time_window_partition_filter(
        self,
        table_partition: TablePartitionDimension,
        iceberg_partition_spec_field_type: T.DateType
        | T.TimestampType
        | T.TimeType
        | T.TimestamptzType,
    ) -> K:
        """Create an iceberg filter for a dagster time window partition

        Args:
            table_partition (TablePartitionDimension): Dagster time window partition
            iceberg_partition_spec_field_type (Union[T.DateType, T.TimestampType, T.TimeType, T.TimestamptzType]): Iceberg field type
            required to correctly format the partition values

        Returns:
            List[E.BooleanExpression]: List of iceberg filters with start and end dates
        """
        partition = cast(TimeWindow, table_partition.partitions)
        start_dt, end_dt = partition
        if isinstance(start_dt, dt.datetime):
            start_dt = start_dt.replace(tzinfo=None)
            end_dt = end_dt.replace(tzinfo=None)
        if isinstance(iceberg_partition_spec_field_type, T.DateType):
            # Internally, PyIceberg uses dt.date.fromisoformat to parse dates.
            #  Dagster will pass dt.datetime objects in time window partitions.
            #  but dt.date.fromisoformat cannot parse dt.datetime.isoformat strings
            start_dt = start_dt.date()
            end_dt = end_dt.date()
        return self._time_window_partition_filters_to_predicates(
            partition_expr=table_partition.partition_expr,
            start_dt=start_dt,
            end_dt=end_dt,
        )

    def partition_dimensions_to_filters(self) -> list[K]:
        """Converts dagster partitions to iceberg filters"""
        predicates = []
        partition_spec_fields: dict[int, str] | None = None
        if self.table_partition_spec is not None:  # Only None when writing new tables
            partition_spec_fields = map_partition_spec_to_fields(
                partition_spec=self.table_partition_spec,
                table_schema=self.table_schema,
            )
        for partition_dimension in self.partition_dimensions:
            field = self.table_schema.find_field(partition_dimension.partition_expr)
            if (
                partition_spec_fields is not None
                and field.field_id not in partition_spec_fields
            ):
                raise ValueError(
                    f"Table is not partitioned by field '{field.name}' with id"
                    f"'{field.field_id}'. Available partition fields: "
                    f"{partition_spec_fields}",
                )
            # TODO(JasperHG90): add timestamp tz type and time type
            predicate_: K
            if isinstance(field.field_type, time_partition_dt_types):
                predicate_ = self._time_window_partition_filter(
                    table_partition=partition_dimension,
                    iceberg_partition_spec_field_type=field.field_type,
                )
            elif isinstance(field.field_type, partition_types):
                predicate_ = self._partition_filter(table_partition=partition_dimension)
            else:
                raise ValueError(
                    f"Partitioning by field type '{field.field_type!s}' not supported",
                )
            predicates.append(predicate_)
        return predicates


class DagsterPartitionToIcebergExpressionMapper(
    DagsterPartitionToPredicateMapper[E.BooleanExpression],
):
    def _partition_filters_to_predicates(
        self,
        partition_expr: str,
        partition_filters: Sequence[str],
    ) -> E.BooleanExpression | E.LiteralPredicate[str]:
        predicates = [E.EqualTo(partition_expr, p) for p in partition_filters]
        if len(predicates) > 1:
            return E.Or(*predicates)
        return predicates[0]

    def _time_window_partition_filters_to_predicates(
        self,
        partition_expr: str,
        start_dt: dt.date | dt.datetime,
        end_dt: dt.date | dt.datetime,
    ) -> E.BooleanExpression:
        return E.And(
            *[
                E.GreaterThanOrEqual(partition_expr, start_dt.isoformat()),
                E.LessThan(partition_expr, end_dt.isoformat()),
            ],
        )


class DagsterPartitionToSqlPredicateMapper(DagsterPartitionToPredicateMapper[str]):
    def _partition_filters_to_predicates(
        self,
        partition_expr: str,
        partition_filters: Sequence[str],
    ) -> str:
        predicates = [f"{partition_expr} = '{p}'" for p in partition_filters]
        if len(predicates) > 1:
            return f"({' OR '.join(predicates)})"
        return predicates[0]

    @abstractmethod
    def _time_window_partition_filters_to_predicates(
        self,
        partition_expr: str,
        start_dt: dt.date | dt.datetime,
        end_dt: dt.date | dt.datetime,
    ) -> str: ...


class DagsterPartitionToPolarsSqlPredicateMapper(DagsterPartitionToSqlPredicateMapper):
    def _time_window_partition_filters_to_predicates(
        self,
        partition_expr: str,
        start_dt: dt.date | dt.datetime,
        end_dt: dt.date | dt.datetime,
    ) -> str:
        return f"{partition_expr} >= '{start_dt.isoformat()}' AND {partition_expr} < '{end_dt.isoformat()}'"


class DagsterPartitionToDaftSqlPredicateMapper(DagsterPartitionToSqlPredicateMapper):
    def _time_window_partition_filters_to_predicates(
        self,
        partition_expr: str,
        start_dt: dt.date | dt.datetime,
        end_dt: dt.date | dt.datetime,
    ) -> str:
        # See https://docs.rs/chrono/latest/chrono/format/strftime/index.html
        parse_fmt = "%+" if isinstance(start_dt, dt.datetime) else "%F"
        return (
            f"{partition_expr} >= to_date('{start_dt.isoformat()}', '{parse_fmt}') "
            f"AND {partition_expr} < to_date('{end_dt.isoformat()}', '{parse_fmt}')"
        )


def update_table_partition_spec(
    table: Table,
    table_slice: TableSlice,
    partition_spec_update_mode: str,
):
    partition_dimensions = cast(
        Sequence[TablePartitionDimension],
        table_slice.partition_dimensions,
    )
    PyIcebergPartitionSpecUpdaterWithRetry(table=table).execute(
        # 3 retries per partition dimension
        retries=(3 * len(partition_dimensions) if len(partition_dimensions) > 0 else 3),
        exception_types=ValueError,
        table_slice=table_slice,
        partition_spec_update_mode=partition_spec_update_mode,
    )


class PyIcebergPartitionSpecUpdaterWithRetry(IcebergOperationWithRetry):
    def operation(self, table_slice: TableSlice, partition_spec_update_mode: str):
        self.logger.debug("Updating table partition spec")
        IcebergTableSpecUpdater(
            partition_mapping=PartitionMapper(
                table_slice=table_slice,
                iceberg_table_schema=self.table.schema(),
                iceberg_partition_spec=self.table.spec(),
            ),
            partition_spec_update_mode=partition_spec_update_mode,
        ).update_table_spec(table=self.table)


class PartitionMapper:
    def __init__(
        self,
        iceberg_table_schema: Schema,
        iceberg_partition_spec: PartitionSpec,
        table_slice: TableSlice,
    ):
        """Maps iceberg partition fields to dagster partition dimensions.

        Args:
            iceberg_table_schema (Schema): PyIceberg table schema
            iceberg_partition_spec (PartitionSpec): PyIceberg table partition spec
            table_slice (TableSlice): dagster database IO manager table slice. This
                contains information about dagster partitions.
        """
        self.iceberg_table_schema = iceberg_table_schema
        self.iceberg_partition_spec = iceberg_partition_spec
        self.table_slice = table_slice

    def get_table_slice_partition_dimensions(
        self,
        allow_empty_dagster_partitions: bool = False,
    ) -> Sequence[TablePartitionDimension]:
        """Retrieve dagster table slice partition dimensions."""
        # In practice, partition_dimensions is an empty list and not None
        #  But the type hint is Optional[Sequence[TablePartitionDimension]]
        partition_dimensions: Sequence[TablePartitionDimension] | None = None
        if self.table_slice.partition_dimensions:
            partition_dimensions = self.table_slice.partition_dimensions
        if partition_dimensions is None:
            if not allow_empty_dagster_partitions:
                raise ValueError(
                    "Partition dimensions are not set. Please set the 'partition_dimensions' field in the TableSlice.",
                )
            return []
        # Check if table columns have the correct data types
        for partition in partition_dimensions:
            field = self.iceberg_table_schema.find_field(partition.partition_expr)
            if isinstance(partition.partitions, TimeWindow):
                if not isinstance(field.field_type, time_partition_dt_types):
                    raise ValueError(
                        f"You have partitioned by some time-based partition window on column '{partition.partition_expr}'"
                        f". But the table column type associated with this partition is '{field.field_type!s}'"
                        f". Please cast the column to an appropriate time-based type"
                        " (e.g. datetime.date, datetime.datetime).",
                    )
            elif isinstance(partition.partitions, list):
                if not isinstance(field.field_type, partition_types):
                    raise ValueError(
                        f"You have defined a static partition on column '{partition.partition_expr}'"
                        f". But the table column type associated with this partition is '{field.field_type!s}'"
                        f". Please cast the column to an appropriate type (e.g. string).",
                    )
            else:
                raise ValueError(
                    f"Partition dimension of type '{type(partition.partitions)}' not supported",
                )

        return partition_dimensions

    def get_iceberg_partition_field_by_name(
        self,
        name: str,
    ) -> PartitionField | None:
        """Retrieve an iceberg partition field by its partition field spec name."""
        partition_field: PartitionField | None = None
        for field in self.iceberg_partition_spec.fields:
            if field.name == name:
                partition_field = field
                break
        return partition_field

    def get_dagster_partition_dimension_names(
        self,
        allow_empty_dagster_partitions: bool = False,
    ) -> list[str]:
        """Retrieve dagster partition dimension names.

        These are set in asset metadata using 'partition_expr', e.g.

        @asset(
            partitions_def=some_partition_definition,
            metadata={"partition_expr": "<COLUMN_NAME>"},
        )
        ...
        """
        return [
            p.partition_expr
            for p in self.get_table_slice_partition_dimensions(
                allow_empty_dagster_partitions=allow_empty_dagster_partitions,
            )
        ]

    @property
    def iceberg_table_partition_field_names(self) -> dict[int, str]:
        """TODO: rename property as this is a mapping"""
        return map_partition_spec_to_fields(
            partition_spec=self.iceberg_partition_spec,
            table_schema=self.iceberg_table_schema,
        )

    @property
    def new_partition_field_names(self) -> set[str]:
        """Retrieve new partition field names passed to a dagster asset that are
        not present in the iceberg table partition spec."""
        return set(self.get_dagster_partition_dimension_names()) - set(
            self.iceberg_table_partition_field_names.values(),
        )

    @property
    def deleted_partition_field_names(self) -> set[str]:
        """Retrieve partition field names that have been removed from a dagster asset."""
        return set(self.iceberg_table_partition_field_names.values()) - set(
            self.get_dagster_partition_dimension_names(
                allow_empty_dagster_partitions=True,
            ),
        )

    @property
    def dagster_time_partitions(self) -> list[TablePartitionDimension]:
        """Retrieve dagster time partitions if present."""
        time_partitions = [
            p
            for p in self.get_table_slice_partition_dimensions()
            if isinstance(p.partitions, TimeWindow)
        ]
        if len(time_partitions) > 1:
            raise ValueError(
                f"Multiple time partitions found: {time_partitions}. Only one time partition is allowed.",
            )
        return time_partitions

    @property
    def updated_dagster_time_partition_field(self) -> str | None:
        """If time partitions present, check whether these have been updated.
        This happens when users change e.g. from an hourly to a daily partition."""
        # The assumption is that even a multi-partitioned table will have only one time partition
        time_partition = next(iter(self.dagster_time_partitions), None)
        updated_field_name: str | None = None
        if time_partition is not None:
            time_partition_partitions = cast(TimeWindow, time_partition.partitions)
            time_partition_transformation = diff_to_transformation(
                time_partition_partitions.start,
                time_partition_partitions.end,
            )
            # Check if field is present in iceberg partition spec
            current_time_partition_field = self.get_iceberg_partition_field_by_name(
                time_partition.partition_expr,
            )
            if current_time_partition_field is not None and (
                time_partition_transformation != current_time_partition_field.transform
            ):
                updated_field_name = time_partition.partition_expr
        return updated_field_name

    def new(self) -> list[TablePartitionDimension]:
        """Retrieve partition dimensions that are not yet present in the iceberg table."""
        return [
            p
            for p in self.get_table_slice_partition_dimensions(
                allow_empty_dagster_partitions=True,
            )
            if p.partition_expr in self.new_partition_field_names
        ]

    def updated(self) -> list[TablePartitionDimension]:
        """Retrieve partition dimensions that have been updated."""
        return [
            p
            for p in self.get_table_slice_partition_dimensions(
                allow_empty_dagster_partitions=True,
            )
            if p.partition_expr == self.updated_dagster_time_partition_field
        ]

    def deleted(self) -> list[PartitionField]:
        """Retrieve partition fields need to be removed from the iceberg table."""
        return [
            p
            for p in self.iceberg_partition_spec.fields
            if p.name in self.deleted_partition_field_names
        ]


class IcebergTableSpecUpdater:
    def __init__(
        self,
        partition_mapping: PartitionMapper,
        partition_spec_update_mode: str,
    ):
        self.partition_spec_update_mode = partition_spec_update_mode
        self.partition_mapping = partition_mapping
        self.logger = logging.getLogger(
            "dagster_iceberg._utils.partitions.IcebergTableSpecUpdater",
        )

    def _changes(
        self,
    ) -> dict[str, list[TablePartitionDimension] | list[PartitionField]]:
        return {
            "new": self.partition_mapping.new(),
            "updated": self.partition_mapping.updated(),
            "deleted": self.partition_mapping.deleted(),
        }

    def _spec_update(self, update: UpdateSpec, partition: TablePartitionDimension):
        self._spec_delete(update=update, partition_name=partition.partition_expr)
        self._spec_new(update=update, partition=partition)

    def _spec_delete(self, update: UpdateSpec, partition_name: str):
        self.logger.debug("Removing partition column: %s", partition_name)
        update.remove_field(name=partition_name)

    def _spec_new(self, update: UpdateSpec, partition: TablePartitionDimension):
        if isinstance(partition.partitions, TimeWindow):
            transform = diff_to_transformation(*partition.partitions)
        else:
            transform = IdentityTransform()
        self.logger.debug("Setting new partition column: %s", partition.partition_expr)
        self.logger.debug("Using transform: %s", transform)
        update.add_field(
            source_column_name=partition.partition_expr,
            transform=transform,
            # Name the partition field spec the same as the column name.
            #  We rely on this throughout this codebase because it makes
            #  it a lot easier to make the mapping between dagster partitions
            #  and Iceberg partition fields.
            partition_field_name=partition.partition_expr,
        )

    @property
    def has_changes(self) -> bool:
        """Return the number of changes to the Iceberg table spec."""
        return len([*itertools.chain.from_iterable(self._changes().values())]) > 0

    def update_table_spec(self, table: Table):
        if self.partition_spec_update_mode == "error" and self.has_changes:
            raise ValueError(
                "Partition spec update mode is set to 'error' but there are partition spec changes to the Iceberg table",
            )
        # If there are no changes, do nothing
        if not self.has_changes:
            return
        with table.update_spec() as update:
            for type_, partitions in self._changes().items():
                if not partitions:  # Empty list
                    continue
                for partition in partitions:
                    match type_:
                        case "new":
                            partition_ = cast(
                                TablePartitionDimension,
                                partition,
                            )
                            self._spec_new(update=update, partition=partition_)
                        case "updated":
                            partition_ = cast(
                                TablePartitionDimension,
                                partition,
                            )
                            self._spec_update(
                                update=update,
                                partition=partition_,
                            )
                        case "deleted":
                            partition_ = cast(PartitionField, partition)
                            self._spec_delete(
                                update=update,
                                partition_name=partition_.name,
                            )
                        case _:
                            raise ValueError(
                                f"Unsupported spec update type: {type_}",
                            )


def map_partition_spec_to_fields(partition_spec: PartitionSpec, table_schema: Schema):
    """Maps partition spec to fields"""
    partition_spec_fields = {}
    for field in partition_spec.fields:
        field_name = next(
            iter(
                [
                    column.name
                    for column in table_schema.fields
                    if column.field_id == field.source_id
                ],
            ),
        )
        partition_spec_fields[field.source_id] = field_name
    return partition_spec_fields
