import subprocess
from typing import Any, Generator

import pyarrow as pa
import pytest

from dagster_adbc import ADBCResource


@pytest.fixture(autouse=True)
def sqlite_adbc_driver() -> Generator[None, Any, None]:
    subprocess.run(["dbc", "install", "sqlite"])
    yield
    subprocess.run(["dbc", "uninstall", "sqlite"])


def test_connection() -> None:
    resource = ADBCResource(driver="sqlite", uri=":memory:")
    with resource.get_connection() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT 1 AS value")
        table = cursor.fetch_arrow_table()

    assert isinstance(table, pa.Table)
    assert table.num_rows == 1
    assert table.column("value")[0].as_py() == 1
