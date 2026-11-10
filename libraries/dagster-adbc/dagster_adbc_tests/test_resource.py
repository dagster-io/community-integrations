import adbc_driver_sqlite
import pyarrow as pa

from dagster_adbc import ADBCResource


def test_connection() -> None:
    resource = ADBCResource(driver=adbc_driver_sqlite._driver_path(), uri=":memory:")
    with resource.get_connection() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT 1 AS value")
        table = cursor.fetch_arrow_table()

    assert isinstance(table, pa.Table)
    assert table.num_rows == 1
    assert table.column("value")[0].as_py() == 1
