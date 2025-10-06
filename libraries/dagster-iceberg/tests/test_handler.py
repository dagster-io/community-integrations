from collections.abc import Sequence
from unittest.mock import Mock, patch
from uuid import uuid4

import pyarrow as pa
import pytest
from dagster import build_output_context
from dagster._core.storage.db_io_manager import TableSlice
from pyiceberg.catalog import Catalog
from pyiceberg.table import Table as IcebergTable

from dagster_iceberg._utils.io import WriteMode
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
