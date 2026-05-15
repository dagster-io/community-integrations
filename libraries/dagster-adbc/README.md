# dagster-adbc

A Dagster module that provides an integration with [ADBC](https://arrow.apache.org/adbc/current/index.html).

## Installation

The `dagster_adbc` module is available as a PyPI package - install with your preferred python environment manager (We recommend [uv](https://github.com/astral-sh/uv)).

```sh
uv venv
source .venv/bin/activate
uv pip install dagster-adbc
```

Additionally, the ADBC driver for your database must be installed.
We recommend [dbc](https://github.com/columnar-tech/dbc) for installing drivers.

```sh
uv pip install dbc
dbc install flightsql
```

## Example Usage

```python
from dagster import Definitions, EnvVar, asset
from dagster_adbc import ADBCResource


@asset
def my_table(dremio: ADBCResource) -> None:
    with dremio.get_connection() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT * FROM my_table")
        table = cursor.fetch_arrow_table()


defs = Definitions(
    assets=[my_table],
    resources={
        "dremio": ADBCResource(
            driver="flightsql",
            uri="grpc+tcp://localhost:32010",
            db_kwargs={"username": "admin", "password": EnvVar("DREMIO_PASSWORD")},
        )
    },
)
```

## Development

The `Makefile` provides the tools required to test and lint your local installation.

```sh
make test
make ruff
make check
```
