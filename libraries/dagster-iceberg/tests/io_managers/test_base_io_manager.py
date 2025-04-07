import pytest
from dagster import OutputContext, build_output_context
from dagster._core.storage.db_io_manager import TableSlice
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchNamespaceError

from dagster_iceberg.config import IcebergCatalogConfig
from dagster_iceberg.io_manager.arrow import PyArrowIcebergIOManager
from dagster_iceberg.io_manager.base import IcebergDbClient


@pytest.fixture
def io_manager(
    catalog_name: str,
    namespace: str,
    catalog_config_properties: dict[str, str],
) -> PyArrowIcebergIOManager:
    return PyArrowIcebergIOManager(
        name=catalog_name,
        config=IcebergCatalogConfig(properties=catalog_config_properties),
        namespace=namespace,
    )


@pytest.fixture
def table_slice_unknown_schema() -> TableSlice:
    return TableSlice(
        table="some_table",
        schema="this_namespace_does_not_exist",
        partition_dimensions=[],
    )


@pytest.fixture
def table_slice_known_schema(namespace_name: str) -> TableSlice:
    return TableSlice(
        table="some_table",
        schema=namespace_name,
        partition_dimensions=[],
    )


@pytest.fixture
def iceberg_db_client():
    return IcebergDbClient()


@pytest.fixture
def output_context(io_manager: PyArrowIcebergIOManager) -> OutputContext:
    return build_output_context(resources={"io_manager": io_manager})


def test_iceberg_db_client_with_known_schema(
    iceberg_db_client: IcebergDbClient,
    output_context: OutputContext,
    table_slice_known_schema: TableSlice,
    catalog: Catalog,
):
    iceberg_db_client.ensure_schema_exists(
        context=output_context,
        table_slice=table_slice_known_schema,
        connection=catalog,
    )


def test_iceberg_db_client_with_unknown_schema(
    iceberg_db_client: IcebergDbClient,
    output_context: OutputContext,
    table_slice_unknown_schema: TableSlice,
    catalog: Catalog,
):
    with pytest.raises(
        NoSuchNamespaceError,
        match="Namespace does not exist: this_namespace_does_not_exist",
    ):
        iceberg_db_client.ensure_schema_exists(
            context=output_context,
            table_slice=table_slice_unknown_schema,
            connection=catalog,
        )
