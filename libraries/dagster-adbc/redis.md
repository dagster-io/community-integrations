# Redis integration guide

Use `ADBCResource` with the community-maintained
[Redis ADBC driver](https://github.com/redis/redis-adbc-driver) to execute SQL and
fetch Arrow data inside Dagster assets. Redis runs separately and must include
the Query Engine (Search). This connects directly to Redis through the native
driver; it does not require an Arrow Flight SQL server.

## Install the resource and driver

In the environment that executes your assets:

```sh
uv pip install dagster-adbc 'adbc-driver-redis[dbapi]' 'adbc-driver-manager>=1.12.0'
python -m adbc_driver_redis install-manifest --venv
```

Run the manifest command in an active virtual environment. Alternatively, use
`--user` for a user-level manifest. The driver wheel bundles the native library
on supported Linux amd64/arm64 and macOS arm64 platforms. For source builds and
other installation details, follow the
[driver installation guide](https://github.com/redis/redis-adbc-driver/blob/main/docs/python-integrations.md#install-and-discover-the-driver).
Installing the package does not install or start Redis.

## Configure an ADBCResource

Set `REDIS_URI` in the asset execution environment, for example
`redis://localhost:6379/0`. Use `rediss://` for verified TLS and keep credentials
in your deployment's secrets configuration.

```python
from dagster import Definitions, EnvVar, asset
from dagster_adbc import ADBCResource


@asset
def redis_answer(redis: ADBCResource) -> int:
    with redis.get_connection() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT 42 AS answer")
        return cursor.fetchone()[0]


defs = Definitions(
    assets=[redis_answer],
    resources={
        "redis": ADBCResource(
            driver="redis",
            uri=EnvVar("REDIS_URI"),
            autocommit=True,
        )
    },
)
```


Set `autocommit=True` explicitly: Redis does not implement transactions, whereas
ADBCResource defaults to transaction mode. `driver="redis"` resolves the installed
manifest. You can instead set `driver` to the absolute native library path.
Native options can be passed in `db_kwargs`, such as
`{"adbc.redis.default_schema": "analytics"}`.

To read a table, replace the constant query with SQL for a table created, ingested,
or adopted through the driver. Existing arbitrary Redis keys do not automatically
become SQL tables. Use `cursor.fetch_arrow_table()` for a PyArrow table, or
`cursor.adbc_ingest(...)` for Arrow ingestion. Connections and cursors should stay
inside their asset's context managers; do not share open handles across workers.

## Optional Redis-specific IO manager

The Redis driver project also ships `RedisADBCIOManager`, which persists Arrow,
pandas, and Polars asset outputs and loads downstream inputs. It is a separate
integration from `ADBCResource`; it is not exported by the released `dagster-adbc`
package or dependent on the proposed generic ADBC IO manager.

```sh
uv pip install 'adbc-driver-redis[dagster]'
```

```python
import pyarrow as pa
from adbc_driver_redis.dagster import RedisADBCIOManager
from dagster import Definitions, EnvVar, asset


@asset(io_manager_key="redis", key_prefix=["analytics"])
def orders() -> pa.Table:
    return pa.table({"id": [1, 2], "amount": [10, 20]})


defs = Definitions(
    assets=[orders],
    resources={"redis": RedisADBCIOManager(uri=EnvVar("REDIS_URI"))},
)
```

This example writes `analytics.orders`; use a dedicated database/schema when
trying it. See the [IO manager guide](https://github.com/redis/redis-adbc-driver/blob/main/docs/python-integrations.md#dagster)
for column selection, partition configuration, type annotations and indexing.

## Write and deployment limits

- Writes autocommit. Asset failures and `rollback()` cannot undo them.
- The Redis IO manager's unpartitioned replacement stages data, then drops and
  renames tables. Readers can observe a missing-table window; a failure between
  those steps can leave the destination absent.
- Partition writes delete the selected rows and append replacements. A failed
  append can leave partial data. Serialize writes to the same asset/partition.
- Install compatible Python/native driver builds and any TLS CA files on every
  execution worker. A caller-local library path or `localhost` endpoint may not
  work on remote workers.

The Redis project's [resource validation](https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/dagster-adbc-resource/validation.md)
records local tests with dagster-adbc 0.0.2 and Dagster 1.13.27, including driver
manifest/library discovery, Arrow ingestion, parameters and real assets. These
are version-specific local results, not upstream Dagster endorsement or
validation of every deployment.
