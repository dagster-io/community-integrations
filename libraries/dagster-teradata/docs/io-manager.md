# Teradata I/O Manager — User Guide

A complete guide to storing Dagster assets as Teradata Vantage tables using
`TeradataIOManager`.

This guide assumes **no prior Dagster knowledge**. If you already know what an I/O
manager is, skip to [Quick start](#quick-start).

---

## Table of contents

1. [What is an I/O manager?](#what-is-an-io-manager)
2. [What the Teradata I/O manager does](#what-the-teradata-io-manager-does)
3. [Prerequisites](#prerequisites)
4. [Quick start](#quick-start)
5. [Where does my table go? Name resolution](#where-does-my-table-go-name-resolution)
6. [Configuration reference](#configuration-reference)
7. [Partitioned assets](#partitioned-assets)
8. [Loading only some columns](#loading-only-some-columns)
9. [Using it with ops instead of assets](#using-it-with-ops-instead-of-assets)
10. [Mixing several I/O managers](#mixing-several-io-managers)
11. [What actually runs against the database](#what-actually-runs-against-the-database)
12. [Writing your own type handler](#writing-your-own-type-handler)
13. [Teradata-specific behaviour and gotchas](#teradata-specific-behaviour-and-gotchas)
14. [Troubleshooting](#troubleshooting)
15. [Testing your pipeline](#testing-your-pipeline)
16. [Current limitations and roadmap](#current-limitations-and-roadmap)
17. [API reference](#api-reference)
18. [FAQ](#faq)

---

## What is an I/O manager?

In Dagster, an **asset** is a function that produces a piece of data — a table, a
file, a model. Normally *you* are responsible for both computing the data and
storing it:

```python
from dagster import asset
import pandas as pd


@asset
def customers() -> None:
    df = pd.DataFrame({"id": [1, 2], "name": ["ada", "grace"]})
    # You have to write all of this plumbing yourself:
    con = teradatasql.connect(host="...", user="...", password="...")
    con.cursor().execute(
        "CREATE TABLE analytics.customers (id BIGINT, name VARCHAR(100))"
    )
    con.cursor().executemany("INSERT INTO analytics.customers VALUES (?, ?)", rows)


@asset
def vip_customers() -> None:
    # ...and read it back by hand in every downstream asset
    con = teradatasql.connect(host="...", user="...", password="...")
    df = pd.read_sql("SELECT * FROM analytics.customers", con)
    ...
```

That plumbing is repetitive, easy to get wrong, and it hard-codes storage decisions
into your business logic.

An **I/O manager** takes over the "save this" and "load that" steps. Your asset just
*returns* a value, and a downstream asset just *receives* it as an argument:

```python
@asset
def customers() -> pd.DataFrame:
    return pd.DataFrame({"id": [1, 2], "name": ["ada", "grace"]})  # saved for you


@asset
def vip_customers(customers: pd.DataFrame) -> pd.DataFrame:
    return customers[customers["id"] > 1]  # loaded for you
```

Dagster calls the I/O manager's `handle_output` when an asset returns a value, and
`load_input` when a downstream asset asks for it. Two concrete benefits:

- **Your asset code has no storage code in it.** The same asset can be materialized
  to Teradata in production and to an in-memory store in tests, by swapping the I/O
  manager — no change to the asset.
- **Consistency.** Table naming, partition replacement and cleanup all follow one
  set of rules instead of being reinvented per asset.

> **When *not* to use one.** If your asset writes to Teradata itself (for example via
> `tdload_operator` or a big `INSERT ... SELECT` that never pulls data into Python),
> don't use an I/O manager — return `None` and use the `TeradataResource` directly.
> I/O managers are for data that passes *through* Python as a DataFrame.

---

## What the Teradata I/O manager does

`TeradataIOManager` stores each asset as a table in a Teradata database, and loads
it back for downstream assets.

| It does | It does not |
| --- | --- |
| Derive the database and table name from the asset key | Create databases (Teradata needs an explicit `PERM` allocation) |
| Replace the table contents on re-materialization, so runs are idempotent | Migrate or alter the schema of an existing table |
| Delete and rewrite only the affected rows for partitioned assets | Decide *how* a DataFrame becomes rows — that is the type handler's job |
| Push column selection into the generated `SELECT` | |
| Wrap each connection in an explicit ANSI-mode transaction, committing or rolling back around the materialization | |
| Reuse every connection option on `TeradataResource` (TLS, proxy, logmech, ...) | |

It is built on Dagster's standard `DbIOManager` machinery, the same foundation used
by the Snowflake, BigQuery, DuckDB and ClickHouse I/O managers, so the concepts
transfer directly between them.

---

## Prerequisites

- Python 3.10+ required. The package declares `requires-python = ">=3.10"`, since the
  I/O manager code itself (and the examples here) use the `X | None` union syntax.
- `dagster>=1.8.0` and `dagster-teradata` installed. For the built-in pandas support
  use the extra:

  ```bash
  pip install "dagster-teradata[pandas]"
  ```

- Network access to a Teradata Vantage system, and a user that can create tables
- **An existing Teradata database.** The I/O manager will not create one:

  ```sql
  CREATE DATABASE analytics AS PERM = 1000000000;
  GRANT ALL ON analytics TO my_user;
  ```

- A **type handler** for the in-memory type you use.

> ### Which class should I use?
>
> - **Using pandas?** Use **`TeradataPandasIOManager`**. It ships with the package and
>   works out of the box — that is what the quick start below uses.
> - **Using another type** (polars, PySpark, a custom object)? Subclass the abstract
>   **`TeradataIOManager`** and supply your own handler. `TeradataIOManager` knows
>   *where* to put data and *when* to replace it, but not *how* to turn your object
>   into Teradata rows. See
>   [Writing your own type handler](#writing-your-own-type-handler).

---

## Quick start

### 1. Configure credentials

Never hard-code credentials. Use `EnvVar` so values are read from the environment at
runtime:

```bash
export TERADATA_HOST=my-vantage-host
export TERADATA_USER=my_user
export TERADATA_PASSWORD=my_password
export TERADATA_DATABASE=analytics
```

### 2. Define the I/O manager

```python
import pandas as pd
from dagster import Definitions, EnvVar, asset

from dagster_teradata import TeradataPandasIOManager, TeradataResource

teradata = TeradataResource(
    host=EnvVar("TERADATA_HOST"),
    user=EnvVar("TERADATA_USER"),
    password=EnvVar("TERADATA_PASSWORD"),
    database=EnvVar("TERADATA_DATABASE"),
)
```

### 3. Write assets that just return DataFrames

```python
@asset(key_prefix=["analytics"])
def customers() -> pd.DataFrame:
    """Stored as the Teradata table analytics.customers."""
    return pd.DataFrame({"id": [1, 2, 3], "name": ["ada", "grace", "alan"]})


@asset(key_prefix=["analytics"])
def vip_customers(customers: pd.DataFrame) -> pd.DataFrame:
    """Reads analytics.customers, writes analytics.vip_customers."""
    return customers[customers["id"] > 1]
```

### 4. Wire it together

```python
defs = Definitions(
    assets=[customers, vip_customers],
    resources={
        "io_manager": TeradataPandasIOManager(teradata=teradata),
    },
)
```

Materialize with `dagster dev`, or from Python:

```python
from dagster import materialize

materialize(
    [customers, vip_customers],
    resources={"io_manager": TeradataPandasIOManager(teradata=teradata)},
)
```

You now have two tables — `analytics.customers` and `analytics.vip_customers` — and
neither asset function contains a line of SQL.

---

## Where does my table go? Name resolution

**Table name** = the *last* component of the asset key.
**Database** = the first of these that is set:

| # | Source | Example | Use it when |
| --- | --- | --- | --- |
| 1 | Asset definition metadata `schema` | `@asset(metadata={"schema": "staging"})` | One asset needs a different database |
| 2 | I/O manager `schema` config | `TeradataPandasIOManager(schema="analytics", ...)` | One database for everything using this I/O manager |
| 3 | Second-to-last asset key component | `@asset(key_prefix=["analytics"])` | Grouping assets by database in the asset graph |
| 4 | `database` on `TeradataResource` | `TeradataResource(database="analytics")` | A sensible account-wide default |
| 5 | *nothing* | — | Fails with a configuration error |

Worked examples:

```python
@asset(key_prefix=["analytics"])
def customers(): ...


# -> analytics.customers                         (rule 3)


@asset(metadata={"schema": "staging"})
def customers(): ...


# -> staging.customers                           (rule 1, beats everything)


@asset
def customers(): ...


# with TeradataResource(database="analytics")
# -> analytics.customers                         (rule 4)


@asset(key_prefix=["warehouse", "analytics"])
def customers(): ...


# -> analytics.customers   (only the *second-to-last* component is the database)
```

> **Teradata note.** Other warehouses have a database *and* a schema inside it
> (`db.schema.table`). Teradata does not — a database **is** the schema. So names are
> always two-part `database.table`, and Dagster's `schema` concept maps onto the
> Teradata database. The rules above are why you'll see `schema` in the config and a
> database name in the value.

Rule 5 is deliberate. Dagster's generic machinery would silently fall back to a
database literally named `public`; this I/O manager raises an actionable error
instead, because that fallback is almost never what a Teradata user wants.

---

## Configuration reference

### `TeradataIOManager`

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `teradata` | `TeradataResource` | ✅ | The connection. Most of its options (port, `logmech`, TLS, proxies, …) are honoured as-is; `tmode` must resolve to ANSI (the default) or `connect()` raises `ValueError`, since the I/O manager relies on ANSI-mode transaction semantics. |
| `schema` | `str` | ❌ | Default Teradata database for assets handled by this I/O manager. Rule 2 above. |

Methods you implement on your subclass:

| Method | Required | Description |
| --- | --- | --- |
| `type_handlers()` | ✅ | Sequence of `DbTypeHandler`s declaring which Python types can be stored. |
| `default_load_type()` | ❌ | Type to load inputs as when there's no type annotation. Auto-inferred if you supply exactly one handler supporting exactly one type. |

Example with an explicit database:

```python
TeradataPandasIOManager(
    teradata=TeradataResource(
        host=EnvVar("TERADATA_HOST"),
        user=EnvVar("TERADATA_USER"),
        password=EnvVar("TERADATA_PASSWORD"),
    ),
    schema="analytics",
)
```

### Legacy `build_teradata_io_manager`

For codebases using the older resource API:

```python
from dagster_teradata import TeradataPandasTypeHandler, build_teradata_io_manager

teradata_io_manager = build_teradata_io_manager(
    [TeradataPandasTypeHandler()],
    default_load_type=pd.DataFrame,
)

defs = Definitions(
    assets=[customers],
    resources={
        "io_manager": teradata_io_manager.configured(
            {
                "teradata": {
                    "host": {"env": "TERADATA_HOST"},
                    "user": {"env": "TERADATA_USER"},
                    "password": {"env": "TERADATA_PASSWORD"},
                    "database": {"env": "TERADATA_DATABASE"},
                },
                "schema": "analytics",
            }
        )
    },
)
```

Prefer `TeradataIOManager` for new code.

---

## Partitioned assets

A partitioned asset is materialized one partition at a time, and each run must
replace **only that partition's rows**. To do that, the I/O manager needs to know
which column identifies the partition. You declare it with `partition_expr`
metadata.

### Static partitions

```python
from dagster import StaticPartitionsDefinition, asset


@asset(
    key_prefix=["analytics"],
    partitions_def=StaticPartitionsDefinition(["red", "blue", "green"]),
    metadata={"partition_expr": "color"},
)
def sales_by_color(context) -> pd.DataFrame:
    color = context.partition_key
    return pd.DataFrame({"color": [color], "amount": [100]})
```

Before inserting, the I/O manager runs:

```sql
DELETE FROM "analytics"."sales_by_color" WHERE (color IN ('red'))
```

so re-materializing `red` leaves `blue` and `green` untouched.

### Time-window partitions

```python
from dagster import DailyPartitionsDefinition, asset


@asset(
    key_prefix=["analytics"],
    partitions_def=DailyPartitionsDefinition(start_date="2024-01-01"),
    metadata={"partition_expr": "order_ts"},
)
def daily_orders(context) -> pd.DataFrame:
    day = context.partition_key
    return fetch_orders_for(day)
```

Generates a half-open range — the start is included, the end excluded, so adjacent
days never overlap:

```sql
DELETE FROM "analytics"."daily_orders" WHERE
(order_ts >= CAST('2024-03-01 00:00:00' AS TIMESTAMP(6)) AND
 order_ts <  CAST('2024-03-02 00:00:00' AS TIMESTAMP(6)))
```

The explicit `CAST` avoids relying on implicit string-to-timestamp conversion, which
is format-sensitive in Teradata.

If your column is a `DATE` rather than a `TIMESTAMP`, use an expression:

```python
metadata = {"partition_expr": "CAST(order_date AS TIMESTAMP(6))"}
```

### Multi-dimensional partitions

Supply a **mapping**, one entry per dimension:

```python
from dagster import MultiPartitionsDefinition, asset


@asset(
    key_prefix=["analytics"],
    partitions_def=MultiPartitionsDefinition(
        {
            "date": DailyPartitionsDefinition(start_date="2024-01-01"),
            "region": StaticPartitionsDefinition(["emea", "apac"]),
        }
    ),
    metadata={"partition_expr": {"date": "order_ts", "region": "region_code"}},
)
def regional_orders(context) -> pd.DataFrame: ...
```

Dimensions are combined with `AND`, each parenthesised so the mixed `AND`/`OR`
precedence is unambiguous. If you forget a dimension in the mapping you get a clear
error naming the asset, rather than malformed SQL.

> ### `partition_expr` is raw SQL — keep it trusted
> The value is interpolated into the statement verbatim, which is what makes
> expressions like `CAST(order_date AS TIMESTAMP(6))` possible. Never build it from
> user input. (Partition *values* and identifiers are escaped for you.)

---

## Loading only some columns

Push column selection into the `SELECT` instead of loading the whole table:

```python
from dagster import AssetIn, asset


@asset(
    ins={"customers": AssetIn("customers", metadata={"columns": ["id"]})},
)
def customer_ids(customers: pd.DataFrame) -> pd.DataFrame:
    # Only the "id" column was ever fetched from Teradata
    return customers
```

Generates `SELECT "id" FROM "analytics"."customers"`.

---

## Using it with ops instead of assets

Ops have no asset key, so name the table through output metadata:

```python
from dagster import Out, job, op


@op(out=Out(metadata={"schema": "analytics", "table": "staged_customers"}))
def stage_customers() -> pd.DataFrame:
    return pd.DataFrame({"id": [1, 2]})


@op(out=Out(metadata={"schema": "analytics", "table": "customer_counts"}))
def count_customers(staged: pd.DataFrame) -> pd.DataFrame:
    # The job's default I/O manager only handles pandas DataFrames, so wrap the
    # scalar result in one instead of returning a bare `int`.
    return pd.DataFrame({"count": [len(staged)]})


@job(resource_defs={"io_manager": TeradataPandasIOManager(teradata=teradata)})
def staging_job():
    count_customers(stage_customers())
```

If `table` is omitted, the output name is used.

---

## Mixing several I/O managers

Not everything belongs in Teradata. Assets pick an I/O manager by key:

```python
from dagster_aws.s3 import S3PickleIOManager


@asset(io_manager_key="teradata_io_manager", key_prefix=["analytics"])
def customers() -> pd.DataFrame: ...


@asset(io_manager_key="s3_io_manager")
def trained_model(customers: pd.DataFrame): ...  # input still loaded from Teradata


defs = Definitions(
    assets=[customers, trained_model],
    resources={
        "teradata_io_manager": TeradataPandasIOManager(teradata=teradata),
        "s3_io_manager": S3PickleIOManager(...),
    },
)
```

The `io_manager_key` controls where an asset's **output** goes; each input is loaded
by the I/O manager belonging to the *upstream* asset.

---

## What actually runs against the database

Useful for reviewing SQL or debugging permissions. For each materialization:

1. **Connect** — via `TeradataResource`, honouring all its options. The connection is
   always closed afterwards, including on failure.
2. **Verify the database exists**
   ```sql
   SELECT 1 FROM DBC.DatabasesV WHERE UPPER(DatabaseName) = UPPER(?)
   ```
   If your user cannot read `DBC`, the check is skipped with a warning rather than
   failing the run.
3. **Delete the rows being replaced**
   ```sql
   -- unpartitioned
   DELETE FROM "analytics"."customers"
   -- partitioned
   DELETE FROM "analytics"."daily_orders" WHERE (...)
   ```
   A "table does not exist" error (`[Error 3807]`) is ignored, so the first run of a
   new asset works. Any other database error is raised.
4. **Hand off to the type handler**, which creates the table if needed and inserts.
5. **Record metadata** — the equivalent `SELECT` is attached to the run so you can see
   exactly what a downstream load would read.

Loading an input runs just the `SELECT`:

```sql
SELECT * FROM "analytics"."customers"
SELECT "id" FROM "analytics"."customers"                      -- with columns metadata
SELECT * FROM "analytics"."daily_orders" WHERE (...)          -- partitioned
```

Object names are always double-quoted and any embedded quotes doubled, so unusual
names and reserved words are safe. Because Teradata resolves object names
case-insensitively, quoting does not change which table you get.

---

## The built-in pandas handler

`TeradataPandasIOManager` is the batteries-included option. Install the extra and
point it at a `TeradataResource`:

```bash
pip install "dagster-teradata[pandas]"
```

```python
from dagster_teradata import TeradataPandasIOManager, TeradataResource

io_manager = TeradataPandasIOManager(
    teradata=TeradataResource(...),
    schema="analytics",  # optional, see name resolution
    chunk_size=5000,  # rows per executemany batch
    min_varchar_length=256,  # floor for inferred VARCHAR widths
)
```

### dtype mapping

The handler creates the table on first write, deriving Teradata column types from the
DataFrame's dtypes. The mapping below is verified against Teradata 17.10 — the codes
in the last column are what you will see in `DBC.ColumnsV`.

| pandas dtype | Teradata column type | `DBC.ColumnsV` code |
| --- | --- | --- |
| `bool` | `BYTEINT` | `I1` |
| `int8` | `BYTEINT` | `I1` |
| `int16` | `SMALLINT` | `I2` |
| `int32` | `INTEGER` | `I` |
| `int64` | `BIGINT` | `I8` |
| `uint8` | `SMALLINT` | `I2` |
| `uint16` | `INTEGER` | `I` |
| `uint32` | `BIGINT` | `I8` |
| `uint64` | `DECIMAL(20,0)` | `D` |
| `float32` / `float64` | `FLOAT` | `F` |
| `datetime64[*]` (naive) | `TIMESTAMP(6)` | `TS` |
| `datetime64[*, tz]` | `TIMESTAMP(6) WITH TIME ZONE` | `SZ` |
| `string` | `VARCHAR(n)` | `CV` |
| `category` | `VARCHAR(n)` from the category values | `CV` |
| `object` of `Decimal` | `DECIMAL(p, s)` fitted to the values | `D` |
| `object` of `bool` | `BYTEINT` | `I1` |
| `object` of `date` | `DATE` | `DA` |
| `object` of `time` | `TIME(6)` | `AT` |
| `object` of `bytes` / `bytearray` | `BLOB` | `BO` |
| anything else | `VARCHAR(n)` | `CV` |

Integer widths follow the dtype's `itemsize`, so a narrow dtype produces a narrow
Teradata column rather than everything collapsing to `BIGINT`.

`timedelta64` is **rejected** with an explicit error rather than guessed at — convert
it to a number of seconds or a string first. Teradata's `INTERVAL` types do not map
cleanly onto a pandas timedelta.

Nullable extension dtypes (`Int64`, `Float64`, `boolean`, `string`) map the same way
as their NumPy equivalents, and `pd.NA` / `NaT` / `NaN` become SQL `NULL`.

`VARCHAR` width is **twice** the longest observed string, measured in UTF-16 code
units (an emoji counts as two), with `min_varchar_length` as
a floor — the table is created once, so a tight bound would break a later run that
carries a longer value. Above 32 000 characters the column becomes `CLOB` (`CO`).
Inferred `VARCHAR`/`CLOB` columns are declared `CHARACTER SET UNICODE` (by the pandas,
polars and PySpark handlers alike), so they never inherit a `LATIN` user default
that would reject non-Latin text.
Sizing from a sample is still a heuristic, so pin the type explicitly when you know
the domain:

```python
TeradataPandasIOManager(
    teradata=teradata,
    column_types={"description": "VARCHAR(10000)", "amount": "DECIMAL(18,4)"},
)
```

`column_types` wins over inference for the named columns, and is also the escape
hatch for `CLOB`, `JSON`, `NUMBER`, `INTERVAL`, period types and anything else the
mapping does not infer. It applies only when the table is created.

### Metadata

Each write attaches `row_count` and a `dagster/column_schema` `TableSchema` to the
materialization, so the resolved Teradata types are visible in the Dagster UI without
querying the database.

### Performance

Rows are inserted with `executemany` in batches of `chunk_size`. That is appropriate
up to the low millions of rows. Beyond that, land the data as files and use
`tdload_operator` / TPT — `dagster-teradata` exposes those separately.

### Schema drift

`CREATE TABLE` runs only when the table is absent. If a later run returns a DataFrame
with new or retyped columns, the existing table is *not* altered and the insert will
fail. Evolve the table deliberately with DDL, or drop it and let the handler recreate
it.

---

## Writing your own type handler

For polars, PySpark or a custom object, write a handler and register it on a
`TeradataIOManager` subclass. A handler implements three things:

| Member | Purpose |
| --- | --- |
| `handle_output(context, table_slice, obj, connection)` | Persist `obj`. Return a dict of metadata to attach to the run. |
| `load_input(context, table_slice, connection)` | Return the object for the given slice. Use `TeradataDbClient.get_select_statement(table_slice)` so partitions and column selection are respected. |
| `supported_types` | The Python types this handler covers. |

A minimal skeleton:

```python
from collections.abc import Sequence

import polars as pl
from dagster._core.storage.db_io_manager import DbTypeHandler, TableSlice

from dagster_teradata import TeradataDbClient, TeradataIOManager


class PolarsTeradataTypeHandler(DbTypeHandler[pl.DataFrame]):
    def handle_output(
        self, context, table_slice: TableSlice, obj: pl.DataFrame, connection
    ):
        # `get_quoted_table_name` returns a safely quoted "database"."table".
        table_name = TeradataDbClient.get_quoted_table_name(table_slice)
        cursor = connection.cursor()
        # ... CREATE TABLE if absent, then INSERT rows ...
        return {"row_count": obj.height}

    def load_input(self, context, table_slice: TableSlice, connection) -> pl.DataFrame:
        cursor = connection.cursor()
        cursor.execute(TeradataDbClient.get_select_statement(table_slice))
        columns = [d[0] for d in cursor.description]
        return pl.DataFrame(cursor.fetchall(), schema=columns, orient="row")

    @property
    def supported_types(self) -> Sequence[type]:
        return [pl.DataFrame]


class PolarsTeradataIOManager(TeradataIOManager):
    def type_handlers(self) -> Sequence[DbTypeHandler]:
        return [PolarsTeradataTypeHandler()]

    @staticmethod
    def default_load_type() -> type | None:
        return pl.DataFrame
```

Read `dagster_teradata/pandas_type_handler.py` for a complete, production-shaped
reference — in particular how it chunks inserts, tolerates a concurrently created
table, and coerces NumPy scalars into values `teradatasql` accepts.

Two rules worth repeating:

- **Never build the table name yourself.** Use
  `TeradataDbClient.get_quoted_table_name(table_slice)`; it quotes and escapes both
  parts, so reserved words and mixed-case names are safe.
- **Never `SELECT *`.** Use `get_select_statement(table_slice)` so partition
  predicates and column selection are applied.

Registering more than one handler lets a single I/O manager serve several types:

```python
class MyIOManager(TeradataIOManager):
    def type_handlers(self):
        return [TeradataPandasTypeHandler(), PolarsTeradataTypeHandler()]
```

Dagster picks a handler from the asset's return type annotation, which is why type
annotations matter here.

---

## Teradata-specific behaviour and gotchas

### ANSI mode makes string comparisons case-sensitive

`TeradataResource` connects with `tmode="ANSI"` by default. In ANSI mode string
comparisons are `CASESPECIFIC`; in Teradata (BTET) mode they are not. This bites in
two places:

- Internally, the database-existence check folds case on both sides
  (`UPPER(DatabaseName) = UPPER(?)`), so it works in either mode.
- **In your own partition columns.** A static partition key `"EMEA"` will *not* match
  a stored value `"emea"` in ANSI mode. Normalize the case in your data, or write
  `metadata={"partition_expr": "UPPER(region)"}` and use upper-case partition keys.

### Databases are not created for you

Snowflake and DuckDB I/O managers issue `CREATE SCHEMA IF NOT EXISTS`. Teradata
databases need an explicit `PERM` allocation and are an administrative act, so a
missing database raises an error telling you what to run.

### No `TRUNCATE`

Teradata has no `TRUNCATE TABLE`. Unpartitioned replacement uses `DELETE FROM`, which
is transactional and preserves the existing table definition — including any changes
made by a DBA, which will *not* be picked up from a changed DataFrame shape.

### Object name length

Teradata object names are limited to 128 characters. Very long asset keys will be
rejected by the database; the I/O manager does not shorten them for you.

### Case of created tables

An asset named `customers` creates a table stored as `customers`. Because Teradata
resolves names case-insensitively, `SELECT * FROM CUSTOMERS` still works.

---

## Troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| `Could not determine which Teradata database to use for 'x'` | No database from any of the 4 sources | Set `schema=` on the I/O manager, add `key_prefix`, or set `database` on `TeradataResource` |
| `Teradata database 'X' does not exist` | Database genuinely missing, or a typo | `CREATE DATABASE X AS PERM = 1000000000;` and grant rights |
| `Could not verify that Teradata database ... exists` (warning) | User lacks `SELECT` on `DBC.DatabasesV` | Harmless; grant `SELECT ON DBC` to silence it |
| `[Error 3807] Object 'x' does not exist` during load | Downstream asset ran before the upstream was ever materialized | Materialize the upstream asset first |
| `does not have a handler for type '<class 'str'>'` | Asset returns a type no handler covers | Add a handler, or fix the return type annotation |
| `'partition_expr' metadata value does not provide a column for every partition dimension` | Multi-partitioned asset with a missing dimension | Provide a dict with one entry per dimension |
| Partitioned asset writes rows but the next run duplicates them | `partition_expr` names a column that isn't actually in the table | Point it at a real column; verify the emitted `DELETE` in run metadata |
| `[Error 3803] Table already exists` | Two runs creating the same table concurrently | Serialize materializations, or catch it in your handler as shown above |
| Static partition deletes nothing | ANSI case-sensitivity | See [ANSI mode](#ansi-mode-makes-string-comparisons-case-sensitive) |

To see the exact SQL a run used, open the run in the Dagster UI and look at the
`Query` entry in the materialization metadata.

---

## Testing your pipeline

### Without a database

Swap in an in-memory I/O manager — your asset code is unchanged:

```python
from dagster import materialize


def test_vip_logic():
    result = materialize([customers, vip_customers])  # default in-memory I/O manager
    assert result.success
```

### Against a live Teradata system

This package ships live tests at
`dagster_teradata_tests/functional/test_io_manager.py`. The `functional/` directory is
excluded from the default `pytest` run by `addopts` in `pyproject.toml`, so name the
path explicitly. The tests are also skipped unless all four environment variables are
set:

```bash
export TERADATA_HOST=... TERADATA_USER=... TERADATA_PASSWORD=... TERADATA_DATABASE=...
pytest dagster_teradata_tests/functional/test_io_manager.py -v
```

They cover the round trip, idempotent re-materialization, column selection, static
and time-window partitions, database resolution and error handling, and drop every
table they create. Use them as a template for your own integration tests.

---

## Current limitations and roadmap

| Status | Item |
| --- | --- |
| ✅ Available | Core I/O manager, name resolution, partition handling, cleanup, live-tested |
| ✅ Available | Built-in pandas type handler (IDE-26551) |
| ✅ Available | Built-in PySpark type handler (IDE-26552) |
| ✅ Available | Built-in polars type handler (IDE-26553) |
| 🚧 Planned | Extended configuration (IDE-26554) |
| 🚧 Planned | Expanded test suite (IDE-26555) |
| 🚧 Planned | Published docs and examples (IDE-26556) |

Known gaps to be aware of:

- No `write_mode` switch yet (append / replace-table). Non-partitioned assets always
  replace their rows. Tracked under IDE-26554.
- Bulk loading goes through `executemany`; very large volumes should use TPT.
- Schema evolution of an existing table is not handled.
- The pandas handler sizes `VARCHAR`/`CLOB` from the data it sees on the first
  write; use `column_types` to pin widths when the domain is wider than the sample.

---

## API reference

Exported from `dagster_teradata`:

| Name | Kind | Use it to |
| --- | --- | --- |
| `TeradataPandasIOManager` | `TeradataIOManager` | **Start here for pandas** — ready to use, no subclassing |
| `TeradataPandasTypeHandler` | `DbTypeHandler` | The pandas ↔ Teradata adapter; register it on your own subclass alongside other handlers |
| `teradata_pandas_io_manager` | `@io_manager` resource | Legacy-API equivalent of `TeradataPandasIOManager` |
| `TeradataPolarsIOManager` | `TeradataIOManager` | **Start here for polars** — ready to use, no subclassing |
| `TeradataPolarsTypeHandler` | `DbTypeHandler` | The polars ↔ Teradata adapter; register it on your own subclass alongside other handlers |
| `teradata_polars_io_manager` | `@io_manager` resource | Legacy-API equivalent of `TeradataPolarsIOManager` |
| `TeradataPySparkIOManager` | `TeradataIOManager` | **Start here for PySpark** — ready to use, no subclassing |
| `TeradataPySparkTypeHandler` | `DbTypeHandler` | The PySpark ↔ Teradata adapter; register it on your own subclass alongside other handlers |
| `TeradataIOManager` | Abstract `ConfigurableIOManagerFactory` | Subclass with your `type_handlers()` for non-pandas types |
| `build_teradata_io_manager` | Factory function | Legacy `@io_manager`-style construction |
| `TeradataDbClient` | `DbClient` | Called by the I/O manager; use `get_select_statement()` / `get_quoted_table_name()` / `get_cleanup_statement()` in handlers |
| `TeradataDbIOManager` | `DbIOManager` | Returned by `create_io_manager()`; subclass only for advanced customization |

Each handler requires its matching extra (`pandas`, `polars`, `pyspark`); importing
a name without its dependency installed raises an `ImportError` telling you which
`pip install "dagster-teradata[...]"` to run. An installed but too-old dependency
(for example `pyspark==3.3`) raises an `ImportError` naming the installed version.

The polars handler supports the same configuration as the pandas one
(`chunk_size`, `min_varchar_length`, `column_types`) and maps polars dtypes onto
the same Teradata column types, with `Duration` and nested dtypes
(`List`/`Array`/`Struct`) rejected with an actionable error instead of a
best-effort mapping. `Categorical` columns are sized from the values actually
present, not from the dtype's category list: polars keeps categories in a
process-global registry shared by every categorical Series, so sizing from it
would let unrelated DataFrames widen the column. Use `column_types` if you need
to reserve width for categories a first materialization does not contain.

The PySpark handler moves data over JDBC (`df.write.jdbc` / `spark.read.jdbc`), so
the Teradata JDBC driver (`terajdbc4.jar`) must be on the Spark classpath and a
`SparkSession` must exist when loading inputs. Spark `StringType` schemas
carry no length, so string columns default to
`VARCHAR(string_length) CHARACTER SET UNICODE` (1024); the character set is
explicit so the column never inherits a `LATIN` user default. Pin wider columns via
`column_types`. A string column stored as `CHARACTER SET LATIN` (as the catalog
reports, which covers a column that took a `LATIN` user default) is also checked
before the cleanup `DELETE` is committed: a value with characters outside LATIN's
ISO-8859-1/Windows-1252 repertoire fails the run rather than the JDBC write. If
`DBC.ColumnsV` is not readable, every string column whose declared type is not
explicitly `CHARACTER SET UNICODE` is checked instead. Before the cleanup `DELETE` is committed the
handler measures the longest value of every string column bound for a sized
character type (one extra Spark job over the cached frame) and fails the
run if any value would not fit, so an overlength value leaves the previous rows
intact instead of failing inside the JDBC write. Every spelling Teradata accepts is
understood -- `VARCHAR`/`CHAR VARYING`/`CHARACTER VARYING`, `CHAR`/`CHARACTER`,
`LONG VARCHAR` (which must name `CHARACTER SET LATIN` or `UNICODE`, since it is
byte-bounded), `CLOB`/`CHARACTER LARGE OBJECT` with `K`/`M`/`G` units -- along
with trailing qualifiers such as `CHARACTER SET UNICODE` or `NOT CASESPECIFIC`. The
same parser feeds the schema-drift check, so a widened override is reported as drift
whatever its spelling. Only a
bare `CLOB` is left unchecked. A `column_types` character override whose capacity
cannot be determined (for example a bare `VARCHAR`) is rejected before any DDL runs.
`column_types` must give the type only: `NOT NULL`, `PRIMARY KEY`, `UNIQUE`,
`CHECK`, `REFERENCES` and `GENERATED ... AS IDENTITY` are rejected the same way,
because a violating value (or, for an identity column, the explicit value Spark
always inserts) would otherwise fail inside the JDBC write after the previous rows
had been deleted. Teradata
`FLOAT` cannot hold NaN, so `FloatType`/`DoubleType` NaN values are written as NULL,
matching the pandas and polars handlers. All-null `void` columns are rejected up
front, because Spark has no JDBC type for them and the write would otherwise fail
inside the JVM only after the cleanup `DELETE` had been committed; cast them to a
concrete type first. Partitioned reads are configured with
`read_partitioning` and parallel writes with `write_num_partitions`.
`read_partitioning` is an allowlist: only Spark's `partitionColumn`,
`lowerBound`, `upperBound`, `numPartitions`, `fetchsize` and `queryTimeout`
options are accepted (matched case-insensitively, as Spark does, so setting one
option under two spellings is rejected), so it cannot be
used to smuggle in options such as `sessionInitStatement` (arbitrary SQL on every
JDBC connection) or `customSchema`. `partitionColumn`, `lowerBound`, `upperBound`
and `numPartitions` must be set together, as Spark requires. `fetchsize` and
`queryTimeout` may be set on their own. An empty DataFrame skips the JDBC write
entirely. The DataFrame is materialized (cached and counted) *before* the cleanup
`DELETE` takes its lock on the target table, so its lineage may read that same
table, for example a self-dependent partitioned asset. Note
that JDBC writes run outside the materialization transaction that
`TeradataDbClient` opens: the cleanup `DELETE` is committed *before* the write
starts, because its Teradata WRITE lock would otherwise block Spark's `INSERT`s
and deadlock the run. Cleanup and write are therefore not atomic — a failed write
leaves the deleted rows gone. Re-materializing the asset is always safe.

Because that commit releases the lock that would otherwise serialize runs, **two
overlapping materializations of the same PySpark-backed asset are not safe**: both
can commit their cleanup `DELETE` before either Spark write begins, and both then
append, leaving duplicated rows (the tables are created `NO PRIMARY INDEX`, so
nothing rejects them). This is a concurrent-*writer* hazard, distinct from the
concurrent-*reader* exposure above. After each write the handler counts the rows
in the materialized slice (under an `ACCESS` lock) and fails the run if the table
holds more rows than were written. That check is **best-effort**: it is skipped
(with a warning) if the count query itself fails, and it can miss a duplication —
for example when the frame contains rows outside a partition slice's bounds, which
are written but not counted. Treat it as a diagnostic, not a guard: it cannot
prevent or undo a duplication (re-materialize the asset to repair the table), and
it does not replace serialization. Serialize materializations of a given asset —
for example with a Dagster [concurrency
pool](https://docs.dagster.io/guides/operate/managing-concurrency) or a run-queue
tag concurrency limit — or, if you need overlapping writes, write each run to its
own staging table and swap it into place yourself. The pandas and polars handlers
are unaffected: they insert on the same connection as the cleanup, so the lock is
held for the whole materialization.

**Only classic Spark sessions are supported, on PySpark 3.4–3.5.** The `pyspark`
extra is pinned to `>=3.4,<4`. Spark Connect sessions and DataFrames (created with
`SparkSession.builder.remote(...)` or `SPARK_REMOTE`) are rejected with a named
error: the handler needs the driver JVM for the time-zone check below and runs
JDBC through the Teradata driver on the Spark classpath.

**Credentials in Spark.** The Teradata password reaches Spark as the JDBC
`password` data source option. Spark redacts data source options matching either
`spark.sql.redaction.options.regex` (default `(?i)url`) or
`spark.redaction.regex` (default `(?i)secret|password|token`) before they appear in
query plans, the Spark UI SQL tab and the event log — verified on Spark 3.5, where
the plan shows the option's value replaced by `*********(redacted)`. If you
override `spark.redaction.regex`, keep `password` in the pattern.

**Connection settings over JDBC.** The JDBC URL carries the resource's `database`,
`port`, `logmech`, TLS settings (`sslmode`, `sslca`, `sslcapath`, `sslcrc`,
`sslcipher`, `sslprotocol`, `slcrl`, `sslocsp`, `oidc_sslmode`), HTTPS proxy
settings (`https_proxy*`, `proxy_bypass_hosts`) and, with `logmech="browser"`, the
`browser*` settings. A proxy password therefore travels in the `url` option, which
Spark redacts by default. The `http_proxy*` settings have no Teradata JDBC
equivalent: they are not applied to the JDBC connections and a warning is logged.
The resource's query band is not applied to the JDBC connections either.

The table is created once and appended to thereafter, so the handler cannot add,
drop, rename or retype columns. A DataFrame whose schema has drifted from the
existing table is rejected *before* the cleanup `DELETE` is committed — otherwise
Spark would reject the append inside the JVM with the previous rows already gone.
Both column names and column types are compared: an `id INTEGER` column
rematerialized as a string is caught, including when the type came from a
`column_types` override. Sizes are compared too — `VARCHAR`/`CHAR` lengths,
`DECIMAL` precision and scale, and `CLOB`/`BLOB` sizes, so an existing `CLOB(100)`
column is not appended to as `CLOB(1000)`. A bare `CLOB`/`BLOB` means Teradata's
maximum size. Migrate the table with `ALTER TABLE`, or drop it so the
next materialization recreates it. A target that exists but is not a base table
(a view, for example) is rejected the same way.

The comparison is an **exact-schema policy**, by design: modeled types must match
exactly, so it also rejects some appends Teradata itself would accept — an
`INTEGER` column in the frame appended to an existing `BIGINT` column, or
`VARCHAR(50)` into `VARCHAR(1024)`. Give the frame the table's exact types (a
`column_types` override is the simplest way) or migrate the table. The check is
otherwise lenient only in that individual columns are skipped when
either side uses a type it cannot confidently parse (an exotic override, or a
hand-created type this handler never emits). If the existing table's columns
cannot be read from `DBC.ColumnsV`/`DBC.TablesV` at all, the write is rejected
before the cleanup `DELETE` is committed, since an unvalidated append could fail
after the previous rows were gone.

**`DATE`/`TIMESTAMP` columns require the Spark driver JVM's default time zone to
be `UTC`.** Spark's JDBC data source binds date/time values as
`java.sql.Date`/`java.sql.Timestamp`, and neither type carries a time zone of its
own: converting them to and from Spark's internal, zone-naive representation is
done relative to `java.util.TimeZone.getDefault()` — **not**
`spark.sql.session.timeZone`, which only affects SQL date/time functions. If the
JVM's default time zone is not UTC, every `DateType`/`TimestampType`/
`TimestampNTZType` value written or read over JDBC is silently shifted by that
zone's offset (for example a JVM defaulting to `Asia/Kolkata`, UTC+5:30, writes
`08:34:05` to Teradata for a value that was `03:04:05` in the DataFrame). Rather
than let that pass silently, the handler checks the JVM's default time zone before
any write or read that touches a date/time column and raises a clear error if it
is not `UTC`. Fix it by setting the JVM's default time zone before the driver
process starts, since it cannot be changed once the JVM is running — for example
with the environment variable `_JAVA_OPTIONS=-Duser.timezone=UTC` (or
`JDK_JAVA_OPTIONS=-Duser.timezone=UTC` on JDK 9+) wherever the Dagster run
launches. On a cluster the JDBC conversions run in the executor JVMs, which the
driver cannot inspect reliably, so outside `local` mode the handler requires the
Spark configuration to set their time zone: the last `-Duser.timezone=` across
`spark.executorEnv.JAVA_TOOL_OPTIONS`, `spark.executor.defaultJavaOptions`,
`spark.executor.extraJavaOptions` and `spark.executorEnv._JAVA_OPTIONS` (the order
the JVM applies them) must name a zone with UTC's rules. Otherwise the run is
rejected before the cleanup `DELETE` is committed. Set
`spark.executor.extraJavaOptions=-Duser.timezone=UTC`.

---

## FAQ

**Do I have to use an I/O manager?**
No. Use `TeradataResource` directly for SQL-only work. I/O managers pay off when data
flows through Python.

**Can one project use several databases?**
Yes — via `key_prefix`, per-asset `schema` metadata, or several configured I/O
managers.

**Does it work with Teradata VantageCloud / Lake?**
Yes. It uses the standard `teradatasql` driver through `TeradataResource`, so
anything the resource can reach works.

**What Teradata versions are supported?**
Verified against 17.10. Nothing used is version-specific; the SQL emitted is standard
`DELETE`/`SELECT`.

**Is it safe from SQL injection?**
Identifiers are quoted and escaped, and partition values are escaped as string
literals. The one deliberate exception is `partition_expr`, which is raw SQL by
design — keep it out of user control.

**Can I see the SQL without running anything?**

```python
from dagster._core.storage.db_io_manager import TableSlice
from dagster_teradata import TeradataDbClient

print(
    TeradataDbClient.get_select_statement(
        TableSlice(table="customers", schema="analytics")
    )
)
# SELECT * FROM "analytics"."customers"
```
