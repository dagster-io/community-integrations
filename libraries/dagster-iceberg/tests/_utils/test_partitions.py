import datetime as dt
from unittest import mock

import pytest
from dagster._core.definitions.time_window_partitions import TimeWindow
from dagster._core.storage.db_io_manager import TablePartitionDimension, TableSlice
from pyiceberg import expressions as E
from pyiceberg import partitioning as iceberg_partitioning
from pyiceberg import schema as iceberg_schema
from pyiceberg import table as iceberg_table
from pyiceberg import transforms
from pyiceberg import types as T

from dagster_iceberg._utils import partitions


@pytest.fixture
def dagster_partition_to_pyiceberg_expression_mapper(
    datetime_table_partition_dimension: TablePartitionDimension,
    category_table_partition_dimension_multiple: TablePartitionDimension,
    table_partitioned: iceberg_table.Table,
) -> partitions.DagsterPartitionToIcebergExpressionMapper:
    return partitions.DagsterPartitionToIcebergExpressionMapper(
        partition_dimensions=[
            datetime_table_partition_dimension,
            category_table_partition_dimension_multiple,
        ],
        table_schema=table_partitioned.schema(),
        table_partition_spec=table_partitioned.spec(),
    )


def test_time_window_partition_filter(
    dagster_partition_to_pyiceberg_expression_mapper: partitions.DagsterPartitionToIcebergExpressionMapper,
    datetime_table_partition_dimension: TablePartitionDimension,
):
    expected_filter = E.And(
        *[
            E.GreaterThanOrEqual("timestamp", "2023-01-01T00:00:00"),
            E.LessThan("timestamp", "2023-01-01T01:00:00"),
        ],
    )
    filter_ = (
        dagster_partition_to_pyiceberg_expression_mapper._time_window_partition_filter(
            datetime_table_partition_dimension,
            T.TimestampType,
        )
    )
    assert filter_ == expected_filter


def test_partition_filter(
    dagster_partition_to_pyiceberg_expression_mapper: partitions.DagsterPartitionToIcebergExpressionMapper,
    category_table_partition_dimension: TablePartitionDimension,
):
    expected_filter = E.EqualTo("category", "A")
    filter_ = dagster_partition_to_pyiceberg_expression_mapper._partition_filter(
        category_table_partition_dimension,
    )
    assert filter_ == expected_filter


def test_partition_filter_with_multiple(
    dagster_partition_to_pyiceberg_expression_mapper: partitions.DagsterPartitionToIcebergExpressionMapper,
    category_table_partition_dimension_multiple: TablePartitionDimension,
):
    expected_filter = E.Or(*[E.EqualTo("category", "A"), E.EqualTo("category", "B")])
    filter_ = dagster_partition_to_pyiceberg_expression_mapper._partition_filter(
        category_table_partition_dimension_multiple,
    )
    assert filter_ == expected_filter


def test_partition_dimensions_to_filters(
    datetime_table_partition_dimension: TablePartitionDimension,
    category_table_partition_dimension: TablePartitionDimension,
    table_partitioned: iceberg_table.Table,
):
    mapper = partitions.DagsterPartitionToIcebergExpressionMapper(
        partition_dimensions=[
            datetime_table_partition_dimension,
            category_table_partition_dimension,
        ],
        table_schema=table_partitioned.schema(),
        table_partition_spec=table_partitioned.spec(),
    )
    filters = mapper.partition_dimensions_to_filters()
    expected_filters = [
        E.And(
            *[
                E.GreaterThanOrEqual("timestamp", "2023-01-01T00:00:00"),
                E.LessThan("timestamp", "2023-01-01T01:00:00"),
            ],
        ),
        E.EqualTo("category", "A"),
    ]
    assert filters == expected_filters


def test_partition_dimensions_to_filters_multiple_categories(
    dagster_partition_to_pyiceberg_expression_mapper: partitions.DagsterPartitionToIcebergExpressionMapper,
):
    filters = dagster_partition_to_pyiceberg_expression_mapper.partition_dimensions_to_filters()
    expected_filters = [
        E.And(
            *[
                E.GreaterThanOrEqual("timestamp", "2023-01-01T00:00:00"),
                E.LessThan("timestamp", "2023-01-01T01:00:00"),
            ],
        ),
        E.Or(*[E.EqualTo("category", "A"), E.EqualTo("category", "B")]),
    ]
    assert filters == expected_filters


def test_iceberg_to_dagster_partition_mapper_new_fields(
    namespace: str,
    iceberg_table_schema: iceberg_schema.Schema,
):
    table_ = "handler_data_multi_partitioned_update_schema_change"
    spec = iceberg_partitioning.PartitionSpec(
        iceberg_partitioning.PartitionField(
            1,
            1,
            name="timestamp",
            transform=transforms.HourTransform(),
        ),
    )
    table_slice = TableSlice(
        table=table_,
        schema=namespace,
        partition_dimensions=[
            TablePartitionDimension(
                "timestamp",
                TimeWindow(dt.datetime(2023, 1, 1, 0), dt.datetime(2023, 1, 1, 1)),
            ),
            TablePartitionDimension(
                "category",
                ["A"],
            ),
        ],
    )
    new_partitions = partitions.PartitionMapper(
        iceberg_table_schema=iceberg_table_schema,
        iceberg_partition_spec=spec,
        table_slice=table_slice,
    ).new()
    assert len(new_partitions) == 1
    assert new_partitions[0].partition_expr == "category"


def test_iceberg_to_dagster_partition_mapper_changed_time_partition(
    namespace: str,
    iceberg_table_schema: iceberg_schema.Schema,
):
    table_ = "handler_data_multi_partitioned_update_schema_change"
    spec = iceberg_partitioning.PartitionSpec(
        iceberg_partitioning.PartitionField(
            1,
            1,
            name="timestamp",
            transform=transforms.HourTransform(),
        ),
    )
    table_slice = TableSlice(
        table=table_,
        schema=namespace,
        partition_dimensions=[
            # Changed from hourly to daily
            TablePartitionDimension(
                "timestamp",
                TimeWindow(dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 2)),
            ),
        ],
    )
    updated_partitions = partitions.PartitionMapper(
        iceberg_table_schema=iceberg_table_schema,
        iceberg_partition_spec=spec,
        table_slice=table_slice,
    ).updated()
    assert len(updated_partitions) == 1
    assert updated_partitions[0].partition_expr == "timestamp"


def test_iceberg_to_dagster_partition_mapper_deleted(
    namespace: str,
    iceberg_table_schema: iceberg_schema.Schema,
):
    table_ = "handler_data_multi_partitioned_update_schema_change"
    spec = iceberg_partitioning.PartitionSpec(
        iceberg_partitioning.PartitionField(
            1,
            1,
            name="timestamp",
            transform=transforms.HourTransform(),
        ),
        iceberg_partitioning.PartitionField(
            2,
            2,
            name="category",
            transform=transforms.IdentityTransform(),
        ),
    )
    table_slice = TableSlice(
        table=table_,
        schema=namespace,
        partition_dimensions=[],
    )
    deleted_partitions = partitions.PartitionMapper(
        iceberg_table_schema=iceberg_table_schema,
        iceberg_partition_spec=spec,
        table_slice=table_slice,
    ).deleted()

    assert len(deleted_partitions) == 2
    assert sorted(p.name for p in deleted_partitions) == ["category", "timestamp"]


def test_iceberg_table_spec_updater_delete_field(
    namespace: str,
    iceberg_table_schema: iceberg_schema.Schema,
):
    table_ = "handler_spec_updater_delete"
    spec = iceberg_partitioning.PartitionSpec(
        iceberg_partitioning.PartitionField(
            1,
            1,
            name="timestamp",
            transform=transforms.HourTransform(),
        ),
        iceberg_partitioning.PartitionField(
            2,
            2,
            name="category",
            transform=transforms.IdentityTransform(),
        ),
    )
    table_slice = TableSlice(
        table=table_,
        schema=namespace,
        partition_dimensions=[
            TablePartitionDimension(
                "timestamp",
                TimeWindow(dt.datetime(2023, 1, 1, 0), dt.datetime(2023, 1, 1, 1)),
            ),
        ],
    )
    spec_updater = partitions.IcebergTableSpecUpdater(
        partition_mapping=partitions.PartitionMapper(
            iceberg_table_schema=iceberg_table_schema,
            iceberg_partition_spec=spec,
            table_slice=table_slice,
        ),
        partition_spec_update_mode="update",
        partition_field_name_prefix="part",
    )
    mock_iceberg_table = mock.MagicMock()
    spec_updater.update_table_spec(table=mock_iceberg_table)
    mock_iceberg_table.update_spec.assert_called_once()
    mock_iceberg_table.update_spec.return_value.__enter__.return_value.remove_field.assert_called_once_with(
        name="category",
    )


def test_iceberg_table_spec_updater_update_field(
    namespace: str,
    iceberg_table_schema: iceberg_schema.Schema,
):
    table_ = "handler_spec_updater_update"
    spec = iceberg_partitioning.PartitionSpec(
        iceberg_partitioning.PartitionField(
            1,
            1,
            name="timestamp",
            transform=transforms.HourTransform(),
        ),
    )
    table_slice = TableSlice(
        table=table_,
        schema=namespace,
        partition_dimensions=[
            TablePartitionDimension(
                "timestamp",
                TimeWindow(dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 2)),
            ),
        ],
    )
    spec_updater = partitions.IcebergTableSpecUpdater(
        partition_mapping=partitions.PartitionMapper(
            iceberg_table_schema=iceberg_table_schema,
            iceberg_partition_spec=spec,
            table_slice=table_slice,
        ),
        partition_spec_update_mode="update",
        partition_field_name_prefix="part",
    )
    mock_iceberg_table = mock.MagicMock()
    spec_updater.update_table_spec(table=mock_iceberg_table)
    mock_iceberg_table.update_spec.assert_called_once()
    mock_iceberg_table.update_spec.return_value.__enter__.return_value.remove_field.assert_called_once_with(
        name="timestamp",
    )
    mock_iceberg_table.update_spec.return_value.__enter__.return_value.add_field.assert_called_once_with(
        source_column_name="timestamp",
        transform=transforms.DayTransform(),
        partition_field_name="part_timestamp",
    )


def test_iceberg_table_spec_updater_add_field(
    namespace: str,
    iceberg_table_schema: iceberg_schema.Schema,
):
    table_ = "handler_spec_updater_add"
    spec = iceberg_partitioning.PartitionSpec()
    table_slice = TableSlice(
        table=table_,
        schema=namespace,
        partition_dimensions=[
            TablePartitionDimension(
                "timestamp",
                TimeWindow(dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 2)),
            ),
        ],
    )
    spec_updater = partitions.IcebergTableSpecUpdater(
        partition_mapping=partitions.PartitionMapper(
            iceberg_table_schema=iceberg_table_schema,
            iceberg_partition_spec=spec,
            table_slice=table_slice,
        ),
        partition_spec_update_mode="update",
        partition_field_name_prefix="part",
    )
    mock_iceberg_table = mock.MagicMock()
    spec_updater.update_table_spec(table=mock_iceberg_table)
    mock_iceberg_table.update_spec.assert_called_once()
    mock_iceberg_table.update_spec.return_value.__enter__.return_value.add_field.assert_called_once_with(
        source_column_name="timestamp",
        transform=transforms.DayTransform(),
        partition_field_name="part_timestamp",
    )


def test_iceberg_table_spec_updater_fails_with_error_update_mode(
    namespace: str,
    iceberg_table_schema: iceberg_schema.Schema,
):
    table_ = "handler_spec_updater_fails"
    spec = iceberg_partitioning.PartitionSpec()
    table_slice = TableSlice(
        table=table_,
        schema=namespace,
        partition_dimensions=[
            TablePartitionDimension(
                "timestamp",
                TimeWindow(dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 2)),
            ),
        ],
    )
    spec_updater = partitions.IcebergTableSpecUpdater(
        partition_mapping=partitions.PartitionMapper(
            iceberg_table_schema=iceberg_table_schema,
            iceberg_partition_spec=spec,
            table_slice=table_slice,
        ),
        partition_spec_update_mode="error",
        partition_field_name_prefix="part",
    )
    mock_iceberg_table = mock.MagicMock()
    with pytest.raises(ValueError, match="Partition spec update mode is set to"):
        spec_updater.update_table_spec(table=mock_iceberg_table)


def test_iceberg_table_spec_updater_fails_with_bad_timestamp_data_type(
    namespace: str,
):
    # Situation: user returns e.g. a pandas DataFrame with a timestamp column
    #  that is of type string, not datetime.
    #  User partitions on this column, so we update the partition spec. However,
    #  this is not really possible since the column is of the wrong type.
    iceberg_table_schema = iceberg_schema.Schema(
        T.NestedField(1, "timestamp", T.StringType()),
        T.NestedField(
            2,
            "category",
            T.StringType(),
        ),
    )
    table_ = "handler_spec_updater_add_wrong_column_type"
    spec = iceberg_partitioning.PartitionSpec()
    table_slice = TableSlice(
        table=table_,
        schema=namespace,
        partition_dimensions=[
            TablePartitionDimension(
                "timestamp",
                TimeWindow(dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 2)),
            ),
        ],
    )
    mock_iceberg_table = mock.MagicMock()
    with pytest.raises(ValueError, match="You have partitioned by some time-based"):
        partitions.IcebergTableSpecUpdater(
            partition_mapping=partitions.PartitionMapper(
                iceberg_table_schema=iceberg_table_schema,
                iceberg_partition_spec=spec,
                table_slice=table_slice,
            ),
            partition_spec_update_mode="update",
            partition_field_name_prefix="part",
        ).update_table_spec(table=mock_iceberg_table)


def test_iceberg_table_spec_updater_fails_with_bad_static_partition_data_type(
    namespace: str,
):
    iceberg_table_schema = iceberg_schema.Schema(
        T.NestedField(1, "timestamp", T.TimestampType()),
        T.NestedField(
            2,
            "category",
            T.IntegerType(),
        ),
    )
    table_ = "handler_spec_updater_add_wrong_column_type"
    spec = iceberg_partitioning.PartitionSpec()
    table_slice = TableSlice(
        table=table_,
        schema=namespace,
        partition_dimensions=[
            TablePartitionDimension(
                "category",
                ["A"],
            ),
        ],
    )
    mock_iceberg_table = mock.MagicMock()
    with pytest.raises(ValueError, match="You have defined a static partition"):
        partitions.IcebergTableSpecUpdater(
            partition_mapping=partitions.PartitionMapper(
                iceberg_table_schema=iceberg_table_schema,
                iceberg_partition_spec=spec,
                table_slice=table_slice,
            ),
            partition_spec_update_mode="update",
            partition_field_name_prefix="part",
        ).update_table_spec(table=mock_iceberg_table)


def test_update_table_partition_spec(
    table: iceberg_table.Table,
    partitioned_table_slice: TableSlice,
):
    partitions.update_table_partition_spec(
        table=table,
        table_slice=partitioned_table_slice,
        partition_spec_update_mode="update",
    )
    table.refresh()
    assert sorted([f.name for f in table.spec().fields]) == [
        "part_category",
        "part_timestamp",
    ]


def test_update_table_partition_spec_with_retries(
    table: iceberg_table.Table,
    partitioned_table_slice: TableSlice,
):
    mock_table = mock.MagicMock()
    mock_update_method = mock.MagicMock()
    # NB: these go for the number of fields in the partition spec
    #  1st try: 2x ValueError -> Failure
    #  2nd try: 1x ValueError, 1x None (success) -> Failure
    #  3rd try: 1x ValueError, 1x None (success) -> Failure
    #  4th try: 2x None (success) -> Success
    mock_update_method.add_field.side_effect = [
        ValueError("An error"),
        ValueError("An error"),
        None,
        ValueError("An error"),
        ValueError("An error"),
        None,
        None,
    ]
    mock_table.update_spec.return_value.__enter__.return_value = mock_update_method
    mock_table.schema.return_value = table.schema()
    partitions.update_table_partition_spec(
        table=mock_table,
        table_slice=partitioned_table_slice,
        partition_spec_update_mode="update",
    )
    assert mock_update_method.add_field.call_count == 7


def test_dagster_partition_to_polars_sql_predicate_mapper(
    datetime_table_partition_dimension: TablePartitionDimension,
    category_table_partition_dimension: TablePartitionDimension,
    table_partitioned: iceberg_table.Table,
):
    mapper = partitions.DagsterPartitionToPolarsSqlPredicateMapper(
        partition_dimensions=[
            datetime_table_partition_dimension,
            category_table_partition_dimension,
        ],
        table_schema=table_partitioned.schema(),
        table_partition_spec=table_partitioned.spec(),
    )
    predicates = mapper.partition_dimensions_to_filters()
    assert (
        predicates[0]
        == "timestamp >= '2023-01-01T00:00:00' AND timestamp < '2023-01-01T01:00:00'"
    )
    assert predicates[1] == "category = 'A'"


def test_dagster_partition_to_daft_sql_predicate_mapper(
    datetime_table_partition_dimension: TablePartitionDimension,
    category_table_partition_dimension: TablePartitionDimension,
    table_partitioned: iceberg_table.Table,
):
    mapper = partitions.DagsterPartitionToDaftSqlPredicateMapper(
        partition_dimensions=[
            datetime_table_partition_dimension,
            category_table_partition_dimension,
        ],
        table_schema=table_partitioned.schema(),
        table_partition_spec=table_partitioned.spec(),
    )
    predicates = mapper.partition_dimensions_to_filters()
    assert (
        predicates[0]
        == "timestamp >= to_date('2023-01-01T00:00:00', '%+') AND timestamp < to_date('2023-01-01T01:00:00', '%+')"
    )
    assert predicates[1] == "category = 'A'"


def test_dagster_partition_to_sql_predicate_mapper_with_multiple_categories(
    datetime_table_partition_dimension: TablePartitionDimension,
    category_table_partition_dimension_multiple: TablePartitionDimension,
    table_partitioned: iceberg_table.Table,
):
    mapper = (
        partitions.DagsterPartitionToDaftSqlPredicateMapper(  # Same for all sql mappers
            partition_dimensions=[
                datetime_table_partition_dimension,
                category_table_partition_dimension_multiple,
            ],
            table_schema=table_partitioned.schema(),
            table_partition_spec=table_partitioned.spec(),
        )
    )
    predicates = mapper.partition_dimensions_to_filters()
    assert (
        predicates[0]
        == "timestamp >= to_date('2023-01-01T00:00:00', '%+') AND timestamp < to_date('2023-01-01T01:00:00', '%+')"
    )
    assert predicates[1] == "(category = 'A' OR category = 'B')"


def test_dagster_partition_to_pyiceberg_expression_mapper_with_multiple_categories(
    datetime_table_partition_dimension: TablePartitionDimension,
    category_table_partition_dimension_multiple: TablePartitionDimension,
    table_partitioned: iceberg_table.Table,
):
    mapper = partitions.DagsterPartitionToIcebergExpressionMapper(  # Same for all sql mappers
        partition_dimensions=[
            datetime_table_partition_dimension,
            category_table_partition_dimension_multiple,
        ],
        table_schema=table_partitioned.schema(),
        table_partition_spec=table_partitioned.spec(),
    )
    expressions = mapper.partition_dimensions_to_filters()
    expected_expressions = [
        E.And(
            *[
                E.GreaterThanOrEqual("timestamp", "2023-01-01T00:00:00"),
                E.LessThan("timestamp", "2023-01-01T01:00:00"),
            ],
        ),
        E.Or(*[E.EqualTo("category", "A"), E.EqualTo("category", "B")]),
    ]
    assert expressions == expected_expressions


@pytest.mark.parametrize(
    ("field_name", "prefix", "expected_name"),
    [
        # Different field names with same prefix
        ("ts", "part", "part_ts"),
        ("id", "part", "part_id"),
        ("name", "part", "part_name"),
        ("category", "part", "part_category"),
        # Test with different prefix
        ("field", "custom", "custom_field"),
    ],
)
def test_partition_field_name_for_transforms(
    field_name: str, prefix: str, expected_name: str
):
    """Test the simplified prefix-based naming convention (transform type no longer affects the name)"""
    assert partitions.partition_field_name_for(field_name, prefix) == expected_name


def test_partition_field_name_for_unhandled_transform():
    """Test the naming convention for any prefix"""
    assert partitions.partition_field_name_for("field", "part") == "part_field"


def test_get_partition_field_by_source_column(
    iceberg_table_schema: iceberg_schema.Schema,
):
    """Test lookup of partition fields by source column"""
    # Create a partition spec with fields
    spec = iceberg_partitioning.PartitionSpec(
        iceberg_partitioning.PartitionField(
            1,  # source_id for timestamp field
            1,  # field_id
            name="part_timestamp",
            transform=transforms.HourTransform(),
        ),
        iceberg_partitioning.PartitionField(
            2,  # source_id for category field
            2,  # field_id
            name="part_category",
            transform=transforms.IdentityTransform(),
        ),
    )

    # Test finding existing fields
    timestamp_field = partitions._get_partition_field_by_source_column(
        schema=iceberg_table_schema, spec=spec, column_name="timestamp"
    )
    assert timestamp_field is not None
    assert timestamp_field.name == "part_timestamp"
    assert timestamp_field.source_id == 1

    category_field = partitions._get_partition_field_by_source_column(
        schema=iceberg_table_schema, spec=spec, column_name="category"
    )
    assert category_field is not None
    assert category_field.name == "part_category"
    assert category_field.source_id == 2

    # Test non-existent column (schema.find_field will raise ValueError)
    missing_field = partitions._get_partition_field_by_source_column(
        schema=iceberg_table_schema, spec=spec, column_name="nonexistent"
    )
    assert missing_field is None

    # Test column exists but not in partition spec
    empty_spec = iceberg_partitioning.PartitionSpec()
    no_partition_field = partitions._get_partition_field_by_source_column(
        schema=iceberg_table_schema, spec=empty_spec, column_name="timestamp"
    )
    assert no_partition_field is None


def test_existing_table_partition_names_unchanged(
    catalog,
    namespace: str,
    iceberg_table_schema: iceberg_schema.Schema,
):
    """Test that existing tables with old partition field names continue to work unchanged"""
    table_name = "test_backward_compatibility"

    # Create table
    table = catalog.create_table(
        identifier=f"{namespace}.{table_name}",
        schema=iceberg_table_schema,
    )

    # Manually add a partition field with old-style naming (same as column name)
    with table.update_spec() as update:
        update.add_field(
            source_column_name="timestamp",
            transform=transforms.HourTransform(),
            partition_field_name="timestamp",  # Old style: same as column name
        )

    table.refresh()
    original_field_name = table.spec().fields[0].name
    assert original_field_name == "timestamp"

    # Now run update with no actual changes (same partition dimensions)
    table_slice = TableSlice(
        table=table_name,
        schema=namespace,
        partition_dimensions=[
            TablePartitionDimension(
                "timestamp",
                TimeWindow(dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 1, 1)),
            ),
        ],
    )

    # This should not change anything since the partition is already there with same transform
    partitions.update_table_partition_spec(
        table=table,
        table_slice=table_slice,
        partition_spec_update_mode="update",
    )

    table.refresh()

    # Verify the old partition field name is preserved (no unnecessary changes)
    assert len(table.spec().fields) == 1
    assert (
        table.spec().fields[0].name == original_field_name
    )  # Should still be "timestamp"


def test_partition_field_naming_avoids_column_conflicts(
    catalog,
    namespace: str,
    iceberg_table_schema: iceberg_schema.Schema,
):
    """Test that new naming approach works where old approach would fail"""
    table_name = "test_naming_compatibility"

    # Create table with a timestamp column
    table = catalog.create_table(
        identifier=f"{namespace}.{table_name}",
        schema=iceberg_table_schema,
    )

    # Try to add hourly partitioning on timestamp
    table_slice = TableSlice(
        table=table_name,
        schema=namespace,
        partition_dimensions=[
            TablePartitionDimension(
                "timestamp",
                TimeWindow(dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 1, 1)),
            ),
        ],
    )

    # This should succeed with new approach
    partitions.update_table_partition_spec(
        table=table,
        table_slice=table_slice,
        partition_spec_update_mode="update",
    )

    table.refresh()

    # Verify we get the new naming convention
    partition_fields = table.spec().fields
    assert len(partition_fields) == 1
    assert partition_fields[0].name == "part_timestamp"
    assert (
        partition_fields[0].source_id == table.schema().find_field("timestamp").field_id
    )
    assert isinstance(partition_fields[0].transform, transforms.HourTransform)


def test_partition_transform_change_detection(
    catalog,
    namespace: str,
    iceberg_table_schema: iceberg_schema.Schema,
):
    """Test that changing transform types is properly detected and updated"""
    table_name = "test_transform_change"

    # Create table and add hourly partitioning
    table = catalog.create_table(
        identifier=f"{namespace}.{table_name}",
        schema=iceberg_table_schema,
    )

    # Add hourly partition
    table_slice_hourly = TableSlice(
        table=table_name,
        schema=namespace,
        partition_dimensions=[
            TablePartitionDimension(
                "timestamp",
                TimeWindow(dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 1, 1)),
            ),
        ],
    )

    partitions.update_table_partition_spec(
        table=table,
        table_slice=table_slice_hourly,
        partition_spec_update_mode="update",
    )
    table.refresh()

    assert len(table.spec().fields) == 1
    assert table.spec().fields[0].name == "part_timestamp"
    assert isinstance(table.spec().fields[0].transform, transforms.HourTransform)

    # Now change to daily partitioning
    table_slice_daily = TableSlice(
        table=table_name,
        schema=namespace,
        partition_dimensions=[
            TablePartitionDimension(
                "timestamp",
                TimeWindow(dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 2)),
            ),
        ],
    )

    partitions.update_table_partition_spec(
        table=table,
        table_slice=table_slice_daily,
        partition_spec_update_mode="update",
    )
    table.refresh()

    # Should have updated to daily
    assert len(table.spec().fields) == 1
    assert table.spec().fields[0].name == "part_timestamp"
    assert isinstance(table.spec().fields[0].transform, transforms.DayTransform)


def test_multiple_transform_types_in_same_table(
    catalog,
    namespace: str,
):
    """Test table with multiple different transform types"""
    table_name = "test_multiple_transforms"

    # Create schema with multiple column types
    schema = iceberg_schema.Schema(
        T.NestedField(1, "id", T.LongType()),
        T.NestedField(2, "timestamp", T.TimestampType()),
        T.NestedField(3, "category", T.StringType()),
        T.NestedField(4, "name", T.StringType()),
    )

    # Create table
    table = catalog.create_table(
        identifier=f"{namespace}.{table_name}",
        schema=schema,
    )

    # Add multiple partitions with different transform types
    table_slice = TableSlice(
        table=table_name,
        schema=namespace,
        partition_dimensions=[
            # Time partition (day transform)
            TablePartitionDimension(
                "timestamp",
                TimeWindow(dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 2)),
            ),
            # Identity partition on category
            TablePartitionDimension(
                "category",
                ["A", "B"],
            ),
        ],
    )

    # First add the Dagster-managed partitions
    partitions.update_table_partition_spec(
        table=table,
        table_slice=table_slice,
        partition_spec_update_mode="update",
    )
    table.refresh()

    # Then manually add additional partition types (bucket and truncate)
    # that Dagster doesn't directly support
    with table.update_spec() as update:
        update.add_field(
            source_column_name="id",
            transform=transforms.BucketTransform(16),
            partition_field_name=partitions.partition_field_name_for("id", "part"),
        )
        update.add_field(
            source_column_name="name",
            transform=transforms.TruncateTransform(10),
            partition_field_name=partitions.partition_field_name_for("name", "part"),
        )

    table.refresh()

    # Verify all partition fields have correct naming
    partition_fields = {field.name: field for field in table.spec().fields}

    # Should have 4 partition fields total
    assert len(partition_fields) == 4

    # Check each field has correct naming and transform
    assert "part_timestamp" in partition_fields
    assert isinstance(
        partition_fields["part_timestamp"].transform, transforms.DayTransform
    )

    assert "part_category" in partition_fields
    assert isinstance(
        partition_fields["part_category"].transform, transforms.IdentityTransform
    )

    assert "part_id" in partition_fields
    assert isinstance(partition_fields["part_id"].transform, transforms.BucketTransform)
    assert partition_fields["part_id"].transform.num_buckets == 16

    assert "part_name" in partition_fields
    assert isinstance(
        partition_fields["part_name"].transform, transforms.TruncateTransform
    )
    assert partition_fields["part_name"].transform.width == 10
