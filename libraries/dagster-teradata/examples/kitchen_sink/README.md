# dagster-teradata kitchen sink

A runnable Dagster project that exercises every `TeradataIOManager` /
`TeradataPandasIOManager` scenario, so you can explore them interactively in
the Dagster UI against a real Teradata instance (a local Vantage Express VM,
a shared dev system, ...).

## Setup

1. Install the package (from `libraries/dagster-teradata`):

   ```bash
   uv sync
   ```

2. Set connection env vars for your Teradata instance:

   ```bash
   export TERADATA_HOST=localhost
   export TERADATA_USER=dbc
   export TERADATA_PASSWORD=dbc
   export TERADATA_DATABASE=my_db
   ```

3. Launch the UI from `libraries/dagster-teradata` (module target, since the
   project uses relative imports between its own files):

   ```bash
   uv run dagster dev -m examples.kitchen_sink.definitions
   ```

## Scenarios

| File | Asset(s)/job | What it demonstrates |
|---|---|---|
| `assets_basic.py` | `basic_customers`, `basic_customers_us`, `basic_no_prefix` | Unpartitioned round trip, re-materialization replacing rows (not appending), column subsetting via `AssetIn(metadata={"columns": [...]})`, and database resolution falling back to the resource's `database` (not a `public` pseudo-schema) when there's no `key_prefix`. |
| `assets_partitions.py` | `orders_by_region`, `events_by_day`, `sales_by_region_and_day` | Static partitions (`IN (...)` cleanup), daily time-window partitions (`CAST(... AS TIMESTAMP(6))` cleanup, sub-second-safe), and multi-partitioned assets (`partition_expr` as a `{dimension: column}` mapping). Materialize a few different partition keys for each to see only the targeted partition's rows get replaced. |
| `assets_dtypes.py` | `dtype_showcase`, `categorical_showcase`, `varchar_fallback_showcase`, `chunked_writes_showcase`, `duplicate_columns_showcase` | The shipped pandas dtype -> Teradata type mapping (including widened unsigned ints), categorical columns sized from the full category domain, VARCHAR/CLOB fallback for unsupported objects (stringified before insert), chunked `executemany()` writes, and a column-name collision that's **expected to fail** with a clear `ValueError`. |
| `assets_tdload.py` | `tdload_job` | Standalone TDLoad execution via `TeradataResource.tdload_operator()`. Edit the `source_table`/`target_table` (or `select_stmt`/`insert_stmt`) and `tdload_options` to match your environment before running - requires the `tdload` client utilities on `PATH`. |
| `assets_tmode.py` | `btet_guard_showcase` | Wired to a `TeradataResource(tmode="TERA")`. Materializing it **is expected to fail** with a `ValueError` explaining that this I/O manager requires ANSI transaction mode - BTET mode's "DDL must be the last statement in a transaction" rule is incompatible with the I/O manager's explicit CREATE TABLE + DELETE + INSERT transaction. |

## Notes

- All tables are created in the database configured by `TERADATA_DATABASE`
  (or an asset's `key_prefix`, which takes precedence). Nothing is dropped
  automatically - clean up tables between runs if you want a fresh start.
- `duplicate_columns_showcase` and `btet_guard_showcase` are **intentionally
  broken** assets used to showcase specific validation/error paths; a failed
  run for either of those two is the expected/correct outcome.
