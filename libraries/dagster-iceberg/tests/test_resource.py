import pyarrow as pa
import pytest
from dagster import asset, materialize
from pyiceberg.catalog import Catalog
from pyiceberg.table import Table

from dagster_iceberg.config import IcebergCatalogConfig
from dagster_iceberg.resource import IcebergTableResource


@pytest.fixture(scope="module")
def table_name() -> str:
    return "resource_data"


@pytest.fixture
def table_identifier(namespace: str, table_name: str) -> str:
    return f"{namespace}.{table_name}"


@pytest.fixture(autouse=True)
def create_table_in_catalog(
    catalog: Catalog,
    table_identifier: str,
    data_schema: pa.Schema,
):
    catalog.create_table(table_identifier, data_schema)


@pytest.fixture(autouse=True)
def iceberg_table(
    create_table_in_catalog,
    catalog: Catalog,
    table_identifier: str,
) -> Table:
    return catalog.load_table(table_identifier)


@pytest.fixture(autouse=True)
def append_data_to_table(iceberg_table: Table, data: pa.Table):
    iceberg_table.append(df=data)


@pytest.fixture
def pyiceberg_table_resource(
    catalog_name: str,
    namespace: str,
    table_name: str,
    catalog_config_properties: dict[str, str],
) -> IcebergTableResource:
    return IcebergTableResource(
        name=catalog_name,
        config=IcebergCatalogConfig(properties=catalog_config_properties),
        namespace=namespace,
        table=table_name,
    )


def test_resource(
    pyiceberg_table_resource: IcebergTableResource,
    data: pa.Table,
):
    @asset
    def read_table(pyiceberg_table: IcebergTableResource):
        table_ = pyiceberg_table.load().scan().to_arrow()
        assert (
            table_.schema.to_string()
            == "timestamp: timestamp[us]\ncategory: large_string\nvalue: double"
        )
        assert table_.shape == (1440, 3)

    materialize(
        [read_table],
        resources={"pyiceberg_table": pyiceberg_table_resource},
    )
