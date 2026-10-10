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

## Store asset outputs with `ADBCIOManager`

`ADBCIOManager` writes `pyarrow.Table`, `pandas.DataFrame`, and `polars.DataFrame`
outputs as database tables and loads them for downstream assets. pandas and polars
are optional: install the library you use alongside `dagster-adbc`.

This runnable SQLite example needs no database server:

```sh
uv pip install dagster-adbc adbc-driver-sqlite
```

```python
import adbc_driver_sqlite
import pyarrow as pa
from dagster import AssetIn, Definitions, asset
from dagster_adbc import ADBCIOManager


@asset
def customers() -> pa.Table:
    return pa.table({"id": [1, 2], "name": ["Ada", "Grace"]})


@asset(ins={"customers": AssetIn(metadata={"columns": ["id"]})})
def customer_ids(customers: pa.Table) -> pa.Table:
    return customers


defs = Definitions(
    assets=[customers, customer_ids],
    resources={
        "io_manager": ADBCIOManager(
            driver=adbc_driver_sqlite._driver_path(),
            uri="assets.sqlite",
        )
    },
)
```

Use a persistent database URI: the IO manager opens a connection per output or
input, so a connection-local `:memory:` database does not persist between assets.
Annotate downstream inputs with their desired table/DataFrame type; unannotated
inputs default to `pyarrow.Table`. pandas indexes are not persisted as columns.
The database driver's Arrow mapping determines which types round-trip unchanged.

### Partitions and table names

The final component of an asset key is the table name. Schema precedence follows
Dagster's database IO managers: asset `schema` metadata, IO manager `schema`
configuration, asset key prefix, then `public`. `database` is an optional ADBC
catalog name; configure the actual connection through `uri` or `db_kwargs`.

For partitioned assets, add `partition_expr` metadata naming the database column
(or a trusted SQL expression) to filter. Re-materializing a partition deletes and
replaces its rows while retaining other partitions. Static, time-window, and
multipartitions use Dagster's table slices; for multipartitions, supply a mapping
from dimension names to SQL expressions.

```python
from dagster import AssetExecutionContext, StaticPartitionsDefinition, asset


@asset(
    partitions_def=StaticPartitionsDefinition(["east", "west"]),
    metadata={"partition_expr": '"region"'},
)
def regional_customers(context: AssetExecutionContext) -> pa.Table:
    return pa.table({"region": [context.partition_key], "id": [1]})
```

Downstream partitioned inputs load only the upstream partitions selected by
Dagster's partition mapping. Input `columns` metadata selects columns on load.
Table, schema, catalog, and selected column identifiers are quoted; partition
values are escaped. Treat `partition_expr` as trusted SQL configuration.

### Driver compatibility and write guarantees

- Connection fields match `ADBCResource`: `driver`, `uri`, `profile`, `entrypoint`,
  `db_kwargs`, `conn_kwargs`, and `autocommit`.
- `dialect="auto"` uses the ADBC vendor name: SQLite disables schema qualification
  and schema creation, MySQL/BigQuery use backticks, and other vendors use ANSI
  double quotes. Override with `dialect="ansi"`, `"sqlite"`, `"mysql"`, or
  `"bigquery"`. These options select SQL conventions, not a guarantee of every
  driver's capabilities. The integration tests exercise SQLite.
- `use_schema=False` disables schema qualification in SQL and ingestion. SQLite
  uses a single table namespace, so asset key prefixes do not separate tables.
  Set `create_schema=False` when schemas are provisioned externally or the
  database does not implement `CREATE SCHEMA IF NOT EXISTS`.
- Drivers must support Arrow ingestion in `create` and `append` modes, Arrow
  query results, and the relevant SQL (`DELETE`, `DROP TABLE IF EXISTS`, and
  `ALTER TABLE ... RENAME TO`). Partition writes additionally require ADBC
  `GetObjects` table metadata. No `replace` or `create_append` support is needed.
- Unpartitioned writes ingest into a uniquely named staging table, then drop the
  old table and rename staging. An ingestion failure preserves the old table.
  Staging is removed on successful writes and on failure (best effort for
  autocommit drivers). Replacement recreates the table, including its schema;
  existing indexes, constraints, and grants are not retained.
- With transactions enabled (the default), writes commit on success and roll
  back on failure. Atomic replacement also requires transactional DDL from the
  database; some vendors implicitly commit DDL.
- Set `autocommit=True` for an autocommit-only driver. If disabling autocommit
  fails, the ADBC manager warns and the IO manager follows the effective mode.
  Without transactions, readers can observe a gap between drop and rename,
  and a rename failure can lose the old table. Partition deletion and ingestion
  are also separate operations: a failed ingest can leave that partition empty.
  Avoid concurrent writers to the same asset or partition.

## Development

The `Makefile` provides the tools required to test and lint your local installation.

```sh
make test
make ruff
make check
```

### Optional Redis driver integration tests

The SQLite tests run by default. To also exercise the Redis ADBC driver, build or
install its shared library and point the tests at a Redis instance with Search
support. Each test creates a unique schema and drops it afterward.

```sh
REDIS_ADBC_DRIVER=/absolute/path/to/libadbc_driver_redis.so \
REDIS_ADBC_URI=redis://localhost:6379/0 \
uv run pytest -q
```

On macOS the driver library ends in `.dylib`. Without both variables, Redis tests
are skipped. These tests exercise DataFrame/Arrow round trips, selected columns,
repeated partition writes, multipartitions, staging cleanup, and the driver's
actual autocommit behavior, including the absence of rollback after a failed
partition append.
