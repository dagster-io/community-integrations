import datetime as dt

import pyarrow as pa
import pytest
from dagster._core.definitions.time_window_partitions import TimeWindow
from dagster._core.storage.db_io_manager import TablePartitionDimension, TableSlice
from pyiceberg import schema as iceberg_schema
from pyiceberg import table as iceberg_table
from pyiceberg import transforms
from pyiceberg import types as T
from pyiceberg.catalog import Catalog


@pytest.fixture
def time_window() -> TimeWindow:
    return TimeWindow(dt.datetime(2023, 1, 1, 0), dt.datetime(2023, 1, 1, 1))


@pytest.fixture
def datetime_table_partition_dimension(
    time_window: TimeWindow,
) -> TablePartitionDimension:
    return TablePartitionDimension("timestamp", time_window)


@pytest.fixture
def category_table_partition_dimension() -> TablePartitionDimension:
    return TablePartitionDimension("category", ["A"])


@pytest.fixture
def category_table_partition_dimension_multiple() -> TablePartitionDimension:
    return TablePartitionDimension("category", ["A", "B"])


@pytest.fixture(scope="module")
def table_name() -> str:
    return "handler_data"


@pytest.fixture(scope="module")
def table_partitioned_name() -> str:
    return "handler_data_partitioned"


@pytest.fixture(scope="module")
def table_partitioned_update_name() -> str:
    return "handler_data_partitioned_update"


@pytest.fixture
def partitioned_table_slice(
    datetime_table_partition_dimension: TablePartitionDimension,
    category_table_partition_dimension: TablePartitionDimension,
    table_partitioned_name: str,
    namespace: str,
) -> TableSlice:
    return TableSlice(
        table=table_partitioned_name,
        schema=namespace,
        partition_dimensions=[
            datetime_table_partition_dimension,
            category_table_partition_dimension,
        ],
    )


@pytest.fixture
def table_slice(table_name: str, namespace: str) -> TableSlice:
    return TableSlice(
        table=table_name,
        schema=namespace,
        partition_dimensions=[],
    )


@pytest.fixture
def table_slice_with_selected_columns(table_name: str, namespace: str) -> TableSlice:
    return TableSlice(
        table=table_name,
        schema=namespace,
        partition_dimensions=[],
        columns=["value"],
    )


@pytest.fixture
def iceberg_table_schema() -> iceberg_schema.Schema:
    return iceberg_schema.Schema(
        T.NestedField(
            1,
            "timestamp",
            T.TimestampType(),
        ),
        T.NestedField(
            2,
            "category",
            T.StringType(),
        ),
    )


@pytest.fixture
def table_identifier(namespace: str, table_name: str) -> str:
    return f"{namespace}.{table_name}"


@pytest.fixture
def table_partitioned_identifier(namespace: str, table_partitioned_name: str) -> str:
    return f"{namespace}.{table_partitioned_name}"


@pytest.fixture
def table_partitioned_update_identifier(
    namespace: str,
    table_partitioned_update_name: str,
) -> str:
    return f"{namespace}.{table_partitioned_update_name}"


@pytest.fixture(autouse=True)
def create_table_in_catalog(
    catalog: Catalog,
    table_identifier: str,
    data_schema: pa.Schema,
):
    catalog.create_table(table_identifier, schema=data_schema)


@pytest.fixture(autouse=True)
def create_partitioned_table_in_catalog(
    catalog: Catalog,
    table_partitioned_identifier: str,
    data_schema: pa.Schema,
):
    partitioned_table = catalog.create_table(
        table_partitioned_identifier,
        schema=data_schema,
    )
    with partitioned_table.update_spec() as update:
        update.add_field(
            source_column_name="timestamp",
            transform=transforms.HourTransform(),
            partition_field_name="timestamp",
        )
        update.add_field(
            source_column_name="category",
            transform=transforms.IdentityTransform(),
            partition_field_name="category",
        )


@pytest.fixture(autouse=True)
def create_partitioned_update_table_in_catalog(
    catalog: Catalog,
    table_partitioned_update_identifier: str,
    data_schema: pa.Schema,
):
    partitioned_update_table = catalog.create_table(
        table_partitioned_update_identifier,
        schema=data_schema,
    )
    with partitioned_update_table.update_spec() as update:
        update.add_field(
            source_column_name="timestamp",
            transform=transforms.HourTransform(),
            partition_field_name="timestamp",
        )
        update.add_field(
            source_column_name="category",
            transform=transforms.IdentityTransform(),
            partition_field_name="category",
        )


@pytest.fixture(autouse=True)
def append_data_to_table(
    create_table_in_catalog,
    catalog: Catalog,
    table_identifier: str,
    data: pa.Table,
):
    catalog.load_table(table_identifier).append(data)


@pytest.fixture(autouse=True)
def append_data_to_partitioned_table(
    create_partitioned_table_in_catalog,
    catalog: Catalog,
    table_partitioned_identifier: str,
    data: pa.Table,
):
    catalog.load_table(table_partitioned_identifier).append(data)


@pytest.fixture
def append_data_to_partitioned_update_table(
    create_partitioned_update_table_in_catalog,
    catalog: Catalog,
    table_partitioned_update_identifier: str,
    data: pa.Table,
):
    catalog.load_table(table_partitioned_update_identifier).append(data)


@pytest.fixture
def table(catalog: Catalog, table_identifier: str) -> iceberg_table.Table:
    return catalog.load_table(table_identifier)


@pytest.fixture
def table_partitioned(
    catalog: Catalog,
    table_partitioned_identifier: str,
) -> iceberg_table.Table:
    return catalog.load_table(table_partitioned_identifier)


@pytest.fixture
def table_partitioned_update(
    catalog: Catalog,
    table_partitioned_update_identifier: str,
) -> iceberg_table.Table:
    return catalog.load_table(table_partitioned_update_identifier)
