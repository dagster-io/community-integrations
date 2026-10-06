# ExecPlan: polars and PySpark type handlers for `TeradataIOManager`

Tickets: IDE-26553 (polars), IDE-26552 (PySpark). PR:
[#4](https://github.com/Teradata-PE/devtools-community-integrations/pull/4).

This is a living document: update **Progress**, **Decision log** and
**Validation** as the work changes.

## Purpose

`dagster-teradata` could only store pandas DataFrames. This change lets assets
return and load **polars** and **PySpark** DataFrames through Teradata tables. The
two new handlers follow the pandas handler's semantics: create the table on first
materialization, replace the partition slice on each run, and read back
partition-aware. They are exposed as `TeradataPolarsIOManager` /
`TeradataPolarsTypeHandler` and `TeradataPySparkIOManager` /
`TeradataPySparkTypeHandler`, and ship as optional extras
(`dagster-teradata[polars]`, `dagster-teradata[pyspark]`). The type mapping stays
consistent across all three handlers.

## Scope and codemap

| Area | Files |
|------|-------|
| Shared handler base (DDL, table existence, value helpers) | `dagster_teradata/_type_handler_base.py` |
| Catalog reflection and schema-drift detection | `dagster_teradata/_catalog.py` |
| polars handler | `dagster_teradata/polars_type_handler.py` |
| PySpark handler (JDBC) | `dagster_teradata/pyspark_type_handler.py` |
| pandas handler (moved onto the shared base) | `dagster_teradata/pandas_type_handler.py` |
| Lazy optional exports | `dagster_teradata/__init__.py` |
| Packaging | `pyproject.toml`, `uv.lock` |
| Docs | `docs/io-manager.md`, `README.md` |
| Unit tests (mocked connection, no JVM) | `dagster_teradata_tests/test_*_type_handler.py`, `test_type_handler_datatypes.py` |
| Live tests (real Teradata; Spark via JDBC) | `dagster_teradata_tests/functional/test_io_manager_datatypes.py` |
| CI | `.github/workflows/quality-check-dagster-teradata.yml` |

## Milestones

1. **polars handler.** Map dtypes to Teradata types, create the table, insert
   with `executemany` in chunks, and load back with partition-aware selects.
2. **PySpark handler.** Map the Spark schema, then write and read over the
   Teradata JDBC driver on the Spark classpath. Supports partitioned reads
   (`read_partitioning`) and parallel writes (`write_num_partitions`).
3. **Shared infrastructure.** Extract the DDL, table-existence and
   value-conversion helpers into `_type_handler_base.py`, and make the pandas
   handler use them so the three handlers cannot drift apart.
4. **Cross-handler datatype matrix.** Unit tests that pin the dtype → Teradata
   type mapping for every handler, plus a live matrix that writes, reads and
   checks every emitted type against `DBC.ColumnsV`.
5. **Hardening from review and live runs.** Schema-drift detection, locking and
   data-loss safety, time zones, Spark Connect, and the remaining edge cases
   listed in the decision log below.
6. **Docs and packaging.** Extras, lazy exports with named errors, and the
   PySpark caveats documented in `io-manager.md`.

## Progress

- [x] Milestone 1: polars handler (f17699f, b2a0603).
- [x] Milestone 2: PySpark handler (791fba2, ceab30a, 7ac4d1d).
- [x] Milestone 3: shared base and pandas refactor.
- [x] Milestone 4: datatype matrix, unit and live (6f344d0).
- [x] Milestone 5: review hardening (05141d1 → 347d243), round by round on PR #4.
- [x] Milestone 6: docs and packaging.
- [ ] Final reviewer sign-off and merge.

## Decision log

| Decision | Rationale |
|----------|-----------|
| Optional extras with lazy exports from `__init__` | Keeps `pip install dagster-teradata` free of polars and pyspark. A missing extra raises a named `ImportError` instead of breaking the whole package. |
| New tables are `NO PRIMARY INDEX` | No natural key is known. Avoids skewing on an arbitrary first column. |
| String widths are doubled observed length with a floor (pandas/polars); `CLOB` past `VARCHAR(32000)` | The table is created only once, so tight widths would break later runs. |
| PySpark strings use a fixed `VARCHAR(string_length)` (default 1024), not `CLOB` | Spark schemas carry no length, and `CLOB` would be slower for every string column. Over-length values are caught before cleanup (see below). |
| The string-length pre-check and the drift check share one character-type parser (`_catalog.parse_character_type`); sized character types it cannot measure are rejected | Two parsers disagreeing lets an override skip the drift check and then pass a length check against a width the table does not have. A bare `LONG VARCHAR` or one in another character set has an unmodeled byte-to-character ratio, so it is rejected before cleanup rather than guessed at. |
| PySpark commits the cleanup `DELETE` before the JDBC write | Spark writes on its own sessions and needs a `WRITE` lock, so holding the `DELETE` would deadlock. The cost is that cleanup and write are not atomic; this is documented, and overlapping runs are detected best-effort. |
| Roll back the `DELETE`, materialize (`cache` + `count`), then re-issue it | The frame's lineage may read its own table. Counting while the `DELETE`'s lock is held hangs forever. |
| Validate everything that can fail inside the JVM before committing the cleanup | Unwritable types, JDBC options, schema drift, view targets and over-length strings must not delete the previous rows and then fail. |
| Schema drift uses an exact-schema policy (names, types, sizes, LOB sizes) | The handlers append and never alter tables. Exact matching is predictable; widening is rejected on purpose and documented. |
| Require the driver JVM's default time zone to be UTC for temporal columns | Spark's JDBC path converts through `java.util.TimeZone.getDefault()`, not `spark.sql.session.timeZone`. |
| Reject Spark Connect with a named error, including Connect-only sessions and Connect DataFrames | The handler needs the driver JVM and JDBC. |
| `read_partitioning` is an allowlist; the four partitioning keys are required together; case-duplicate keys are rejected | Stops overriding connection options, and invalid Spark option sets are caught up front. |
| Type polars columns with no values from `cursor.description`, and zoned timestamps from `DBC.ColumnsV` | Value inference yields `Null`, and the description cannot tell zoned from naive timestamps. |
| Reflected column names are right-stripped only | Leading blanks are part of a quoted identifier. |
| Lazy exports report "too old" only for `AttributeError` | That is how a too-old requirement fails (e.g. pyspark 3.3 has no `TimestampNTZType`). An `ImportError` from anything else in the handler's import chain is re-raised as-is so it stays actionable. |
| Spark column references are backtick-quoted | Teradata allows `.` and `` ` `` in quoted names, but Spark would parse an unquoted `a.b` as a nested field. |
| PySpark `column_types` overrides must be type-only (no `NOT NULL`, `PRIMARY KEY`, `UNIQUE`, `CHECK`, `REFERENCES`, `GENERATED`/`IDENTITY`) | The handler validates types, not constraints, before it commits the cleanup; a violating value would fail in JDBC after the previous rows were gone. |
| PySpark maps NaN to NULL in float/double columns | Teradata `FLOAT` cannot store NaN and the JDBC write would fail after the commit. pandas and polars already do the same. |
| Artifactory-only dependency resolution and ARC runners are left out of scope | These are repo-wide, pre-existing conventions for every library, to be handled as a separate follow-up. |

## Validation

Run from `libraries/dagster-teradata`:

```bash
uv run ruff format --check && uv run ruff check   # formatting and lint
uv run ty check                                   # type checking
uv run pytest                                     # unit suite (mocked; no DB or JVM)
```

Live suite (requires a reachable Teradata system; PySpark tests also need Java
and the JDBC driver):

```bash
export TERADATA_HOST=... TERADATA_USER=... TERADATA_PASSWORD=... TERADATA_DATABASE=...
export TERADATA_JDBC_JAR=/path/to/terajdbc4.jar   # enables the PySpark tests
export _JAVA_OPTIONS=-Duser.timezone=UTC
uv run pytest dagster_teradata_tests/functional/test_io_manager_datatypes.py
```

Latest results on this branch (Teradata 20.0, PySpark 3.5.9 on Zulu JDK 21,
`terajdbc4.jar`):
- Unit: 567 passed. The only failures are two `test_tpt_utils` POSIX-permission tests that also fail on `main` when run on Windows.
- Live: 16/16 passed, none skipped. That includes all 9 PySpark tests, which run the JDBC write and read path end to end against the live database.

An earlier run (reported in the first version of the PR description) skipped the 2
PySpark tests that existed then, because that machine had no JVM. Every later run
included them.

Every data-loss or hang fix was confirmed to reproduce on the pre-fix code before
being marked done.
