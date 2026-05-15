from collections.abc import Sequence
from unittest.mock import Mock, patch
from uuid import uuid4

import pyarrow as pa
import pytest
from dagster import build_output_context
from dagster._core.storage.db_io_manager import TableSlice
from pyiceberg.catalog import Catalog
from pyiceberg.table import Table as IcebergTable

from dagster_iceberg._utils.io import (
    DEFAULT_PARTITION_FIELD_NAME_PREFIX,
    UpsertOptions,
    WriteMode,
)
from dagster_iceberg.config import IcebergCatalogConfig
from dagster_iceberg.handler import IcebergBaseTypeHandler


class MockTypeHandler(IcebergBaseTypeHandler[pa.Table]):
    """Mock type handler for testing."""

    def to_data_frame(self, table, table_slice, target_type):
        return table.scan().to_arrow()

    def to_arrow(self, obj):
        return obj

    def supported_types(self) -> Sequence[type[object]]:
        return (pa.Table, pa.RecordBatchReader)


@pytest.fixture
def mock_type_handler():
    return MockTypeHandler()


@pytest.fixture
def mock_catalog():
    """Mock catalog for testing."""
    catalog = Mock(spec=Catalog)
    mock_table = Mock()
    mock_table.schema.return_value.model_dump.return_value = {
        "fields": [
            {"name": "col1", "type": "string"},
            {"name": "col2", "type": "int32"},
        ]
    }
    mock_snapshot = Mock()
    mock_snapshot.model_dump.return_value = {"snapshot_id": "test_snapshot"}
    mock_table.current_snapshot.return_value = mock_snapshot
    catalog.load_table.return_value = mock_table
    return catalog


@pytest.fixture
def table_slice():
    """Mock table slice for testing."""
    return TableSlice(
        table="test_table",
        schema="test_schema",
        partition_dimensions=[],
    )


@pytest.fixture
def mock_table_writer():
    with patch("dagster_iceberg.handler.table_writer") as mock_table_writer:
        mock_table_writer.return_value = None
        yield mock_table_writer


@pytest.fixture
def sample_data():
    """Sample PyArrow table for testing."""
    return pa.table({"col1": ["a", "b"], "col2": [1, 2]})


@pytest.fixture
def table(catalog: Catalog, table_slice: TableSlice, sample_data: pa.Table):
    catalog.create_namespace_if_not_exists(table_slice.schema)
    catalog.create_table_if_not_exists(
        f"{table_slice.schema}.{table_slice.table}", sample_data.schema
    )
    table = catalog.load_table(f"{table_slice.schema}.{table_slice.table}")
    table.overwrite(sample_data)
    return table


def test_handle_output_metadata_passing(
    catalog: Catalog,
    table_slice: TableSlice,
    sample_data: pa.Table,
    mock_table_writer: Mock,
    table: IcebergTable,
):
    """Test that metadata from definition and output contexts is passed correctly to table_writer. Useful for testing overrides or calculated values"""
    # Test that output metadata overrides definition metadata for write mode
    definition_metadata = {
        "write_mode": "overwrite",
        "partition_spec_update_mode": "error",
        "schema_update_mode": "error",
        "table_properties": {"prop1": "value1"},
        "partition_key": None,
    }
    expected_write_mode = WriteMode.overwrite
    expected_partition_spec_mode = "error"
    expected_schema_mode = "error"
    expected_table_properties = {"prop1": "value1"}
    expected_partition_key = None

    run_id = str(uuid4())
    context = build_output_context(
        definition_metadata=definition_metadata,
        run_id=run_id,
    )

    handler = MockTypeHandler()

    handler.handle_output(
        context=context,
        table_slice=table_slice,
        obj=sample_data,
        connection=catalog,
    )

    mock_table_writer.assert_called_once_with(
        table_slice=table_slice,
        data=sample_data,
        catalog=catalog,
        schema_update_mode=expected_schema_mode,
        partition_spec_update_mode=expected_partition_spec_mode,
        dagster_run_id=run_id,
        dagster_partition_key=expected_partition_key,
        table_properties=expected_table_properties,
        write_mode=expected_write_mode,
        partition_field_name_prefix="part",
        upsert_options=None,
    )


def test_handle_output_invalid_write_mode():
    definition_metadata = {
        "write_mode": "invalid",
    }
    run_id = str(uuid4())
    context = build_output_context(
        definition_metadata=definition_metadata,
        run_id=run_id,
    )
    handler = MockTypeHandler()
    with pytest.raises(ValueError, match="^Invalid write mode.*"):
        handler.handle_output(
            context=context,
            table_slice=table_slice,
            obj=sample_data,
            connection=mock_catalog,
        )


@pytest.mark.parametrize(
    ("resource_config", "expected_partition_field_name_prefix"),
    [
        ({"config": {"partition_field_name_prefix": "custom_prefix"}}, "custom_prefix"),
        (
            {
                "config": IcebergCatalogConfig(
                    properties={"test": "value"},
                    partition_field_name_prefix="iceberg_prefix",
                )
            },
            "iceberg_prefix",
        ),
        ({"config": {}}, DEFAULT_PARTITION_FIELD_NAME_PREFIX),
        (
            {"config": IcebergCatalogConfig(properties={"test": "value"})},
            DEFAULT_PARTITION_FIELD_NAME_PREFIX,
        ),
        (None, DEFAULT_PARTITION_FIELD_NAME_PREFIX),
    ],
)
def test_get_partition_field_name_prefix(
    mock_type_handler: MockTypeHandler,
    resource_config: dict,
    expected_partition_field_name_prefix: str,
):
    """Test _get_partition_field_name_prefix when resource_config has dict config."""
    context = build_output_context(resource_config=resource_config)
    assert (
        mock_type_handler._get_partition_field_name_prefix(context)
        == expected_partition_field_name_prefix
    )


def test_get_partition_field_name_prefix_with_iceberg_config(
    mock_type_handler: MockTypeHandler,
):
    """Test _get_partition_field_name_prefix when resource_config has IcebergCatalogConfig."""

    # Test with custom prefix in IcebergCatalogConfig
    iceberg_config = IcebergCatalogConfig(
        properties={"test": "value"}, partition_field_name_prefix="iceberg_prefix"
    )
    context = build_output_context(resource_config={"config": iceberg_config})
    assert (
        mock_type_handler._get_partition_field_name_prefix(context) == "iceberg_prefix"
    )

    # Test with default prefix in IcebergCatalogConfig
    iceberg_config = IcebergCatalogConfig(properties={"test": "value"})
    context = build_output_context(resource_config={"config": iceberg_config})
    assert mock_type_handler._get_partition_field_name_prefix(context) == "part"


def test_get_partition_field_name_prefix_with_definition_metadata_override(
    mock_type_handler: MockTypeHandler,
):
    """Test _get_partition_field_name_prefix when definition_metadata overrides config."""
    # Test dict config with definition metadata override
    context = build_output_context(
        resource_config={"config": {"partition_field_name_prefix": "config_prefix"}},
        definition_metadata={"partition_field_name_prefix": "metadata_prefix"},
    )
    assert (
        mock_type_handler._get_partition_field_name_prefix(context) == "metadata_prefix"
    )


def test_get_partition_field_name_prefix_none_resource_config(
    mock_type_handler: MockTypeHandler,
):
    """Test _get_partition_field_name_prefix raises error when resource_config is None."""
    # build_output_context normalizes None resource_config to an empty dict, so create a mock context with resource_config set to None. This seems like it probably never happens in the wild.
    context = Mock()
    context.resource_config = None
    context.definition_metadata = {}

    with pytest.raises(
        ValueError,
        match="Resource config is required to get partition_field_name_prefix",
    ):
        mock_type_handler._get_partition_field_name_prefix(context)


def test_get_partition_field_name_prefix_invalid_config_type(
    mock_type_handler: MockTypeHandler,
):
    """Test _get_partition_field_name_prefix raises error for invalid config type."""

    # Test with invalid config type (not dict or IcebergCatalogConfig)
    context = build_output_context(resource_config={"config": "invalid_string_config"})

    with pytest.raises(
        ValueError,
        match="Unable to retrieve partition_field_name_prefix from `config` attribute",
    ):
        mock_type_handler._get_partition_field_name_prefix(context)


def test_handle_output_upsert_with_definition_metadata(
    catalog: Catalog,
    table_slice: TableSlice,
    sample_data: pa.Table,
    mock_table_writer: Mock,
    table: IcebergTable,
):
    """Test that upsert_options from definition metadata are passed correctly to table_writer."""
    upsert_options = {
        "join_cols": ["col1"],
        "when_matched_update_all": True,
        "when_not_matched_insert_all": True,
    }
    definition_metadata = {
        "write_mode": "upsert",
        "upsert_options": upsert_options,
    }

    run_id = str(uuid4())
    context = build_output_context(
        definition_metadata=definition_metadata,
        run_id=run_id,
    )

    handler = MockTypeHandler()
    handler.handle_output(
        context=context,
        table_slice=table_slice,
        obj=sample_data,
        connection=catalog,
    )

    mock_table_writer.assert_called_once()
    call_kwargs = mock_table_writer.call_args[1]
    assert call_kwargs["write_mode"] == WriteMode.upsert
    assert call_kwargs["upsert_options"] == UpsertOptions(**upsert_options)


def test_handle_output_upsert_with_output_metadata_override(
    mock_catalog: Mock,
    table_slice: TableSlice,
    sample_data: pa.Table,
    mock_table_writer: Mock,
):
    """Test that upsert_options from output metadata override definition metadata."""
    definition_upsert_options = {
        "join_cols": ["col1"],
        "when_matched_update_all": False,
        "when_not_matched_insert_all": False,
    }
    output_upsert_options = {
        "join_cols": ["col1", "col2"],
        "when_matched_update_all": True,
        "when_not_matched_insert_all": True,
    }

    # Create a fully mocked context with output_metadata
    mock_context = Mock()
    mock_context.definition_metadata = {
        "write_mode": "upsert",
        "upsert_options": definition_upsert_options,
    }
    mock_context.output_metadata = {"upsert_options": output_upsert_options}
    mock_context.run_id = str(uuid4())
    mock_context.has_asset_partitions = False
    mock_context.resource_config = {"config": {}}

    handler = MockTypeHandler()
    handler.handle_output(
        context=mock_context,
        table_slice=table_slice,
        obj=sample_data,
        connection=mock_catalog,
    )

    mock_table_writer.assert_called_once()
    call_kwargs = mock_table_writer.call_args[1]
    assert call_kwargs["write_mode"] == WriteMode.upsert
    assert call_kwargs["upsert_options"] == UpsertOptions(**output_upsert_options)


def test_upsert_actual_operation(
    catalog: Catalog,
    table_slice: TableSlice,
    sample_data: pa.Table,
):
    """Test that upsert actually updates and inserts rows in an Iceberg table."""
    # Create initial table with sample data
    catalog.create_namespace_if_not_exists(table_slice.schema)
    catalog.create_table_if_not_exists(
        f"{table_slice.schema}.{table_slice.table}", sample_data.schema
    )
    table = catalog.load_table(f"{table_slice.schema}.{table_slice.table}")
    table.overwrite(sample_data)

    # Verify initial data
    initial_data = table.scan().to_arrow()
    assert len(initial_data) == 2
    assert initial_data["col1"].to_pylist() == ["a", "b"]
    assert initial_data["col2"].to_pylist() == [1, 2]

    # Create new data that updates one row and inserts one new row
    upsert_data = pa.table({"col1": ["a", "c"], "col2": [10, 3]})

    upsert_options = {
        "join_cols": ["col1"],
        "when_matched_update_all": True,
        "when_not_matched_insert_all": True,
    }
    definition_metadata = {
        "write_mode": "upsert",
        "upsert_options": upsert_options,
    }

    run_id = str(uuid4())
    context = build_output_context(
        definition_metadata=definition_metadata,
        run_id=run_id,
    )

    handler = MockTypeHandler()
    handler.handle_output(
        context=context,
        table_slice=table_slice,
        obj=upsert_data,
        connection=catalog,
    )

    # Verify the upsert operation
    # Refresh the table to see the latest snapshot
    table = catalog.load_table(f"{table_slice.schema}.{table_slice.table}")
    result_data = table.scan().to_arrow()
    assert len(result_data) == 3

    # Sort by col1 for consistent comparison
    result_sorted = result_data.sort_by("col1")
    assert result_sorted["col1"].to_pylist() == ["a", "b", "c"]
    assert result_sorted["col2"].to_pylist() == [10, 2, 3]  # 'a' updated to 10


def test_handle_output_upsert_missing_options(
    catalog: Catalog,
    table_slice: TableSlice,
    sample_data: pa.Table,
):
    """Test that appropriate error is raised when upsert mode is used without upsert_options."""
    # Create namespace first
    catalog.create_namespace_if_not_exists(table_slice.schema)

    definition_metadata = {
        "write_mode": "upsert",
        # Missing upsert_options
    }

    run_id = str(uuid4())
    context = build_output_context(
        definition_metadata=definition_metadata,
        run_id=run_id,
    )

    handler = MockTypeHandler()
    with pytest.raises(ValueError, match=".*upsert_options.*"):
        handler.handle_output(
            context=context,
            table_slice=table_slice,
            obj=sample_data,
            connection=catalog,
        )
