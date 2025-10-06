# dagster-ducklake

A dagster module that provides integration with [ducklake](https://ducklake.select/)

## Installation

The `dagster_ducklake` module is available as a PyPI package - install with your preferred python
environment manager (We recommend [uv](https://github.com/astral-sh/uv)).

```
source .venv/bin/activate
uv pip install dagster-ducklake
```

## Example Usage

```python
import dagster as dg

@dg.asset
def my_ducklake_asset(ddb: DuckDBConnectionProvider):
    with ddb.duckdb_connect() as con:
        query = "select * from table"

        df = con.query(query).df()
        df.head()
```

## Resource initialization

```python
{
    "ducklake": DuckLakeResource(
        metadata_backend=PostgresConfig(
            host="db.mycorp.com",
            port=5432,
            database="ducklake_catalog",
            user=dg.EnvVar("POSTGRES_USER"),
            password=dg.EnvVar("POSTGRES_PASSWORD"),
        ),
        storage_backend=S3Config(
            endpoint_url="objectstore.mycorp.com",
            bucket="duckpond-dev",
            prefix="stage",
            aws_access_key_id=dg.EnvVar("OBJECT_STORE_USER"),
            aws_secret_access_key=dg.EnvVar("OBJECT_STORE_PASSWORD"),
            region=dg.EnvVar("OBJECT_STORE_REGION"),
            use_ssl=True,
            url_style="path",
        ),
        alias="stage",
        plugins=["postgres", "httpfs", "ducklake"],
    ),
}

```

## Development

The `Makefile` provides the tools required to test and lint your local installation

```sh
make test
make ruff
make check
```
