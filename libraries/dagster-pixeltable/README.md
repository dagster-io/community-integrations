# dagster-pixeltable

Run Dagster jobs against existing [Pixeltable](https://github.com/pixeltable/pixeltable) tables. Dagster schedules
runs, orders work across systems, and records run history. Pixeltable stores the data and maintains computed
columns, views, and indexes.

The package provides one resource, `PixeltableResource`. During resource setup it checks that the Dagster process
uses the Pixeltable home you expect, then initializes the SDK. Assets open tables by full path and write to them
with the Pixeltable SDK.

## Installation

```sh
pip install dagster-pixeltable
```

Requires Python 3.11 or later, `dagster>=1.9.4`, and `pixeltable>=0.7.15`.

## Usage

```python
import dagster as dg
from dagster_pixeltable import PixeltableResource


@dg.asset
def ingest_documents(pixeltable: PixeltableResource) -> dg.MaterializeResult:
    table = pixeltable.get_table("dagster_demo.documents")
    status = table.batch_update(
        [{"source_id": "doc-1", "body": "Hello", "page_count": 1}],
        if_not_exists="insert",
    )
    return dg.MaterializeResult(
        metadata={
            "dagster/row_count": table.count(),
            "pixeltable/num_excs": status.num_excs,
        }
    )


defs = dg.Definitions(
    assets=[ingest_documents],
    resources={"pixeltable": PixeltableResource(expected_home="/srv/pixeltable")},
    executor=dg.in_process_executor,
)
```

### Environment

Before starting the Dagster process, set `PIXELTABLE_HOME` to an absolute path, along with any database settings and
provider API keys. Pixeltable fixes its home the first time the process uses the SDK, so a later change to
`PIXELTABLE_HOME` has no effect. Run `pxt schema update` with the same environment.

| Field | Meaning |
| --- | --- |
| `expected_home` | Absolute path of the Pixeltable home the job uses. Setup fails unless the process uses this home. |

### Execution mode

This release supports the in-process executor on one host with persistent local storage. Set
`executor=dg.in_process_executor` on `Definitions`: Dagster's default executor runs each step in its own process,
and this release has not been tested that way.

### Table access

`get_table(table_path)` opens an existing local table, view, or snapshot by its full catalog path, such as
`dagster_demo.documents`.

- It never creates a table, and never derives a path from an asset key.
- A version suffix, such as `dagster_demo.documents:7`, opens a read-only handle to that version.
- Hosted paths (`pxt://...`, `https://pixeltable.com/t/...`) are rejected.
- It works only during a run, after resource setup.

## Examples

Each example declares its tables in an `app.py`. `pxt schema update` creates the tables, and Dagster writes rows
into them. Run the commands from `examples/`, which holds the Pixeltable project file:

```sh
cd examples
export PIXELTABLE_HOME="$HOME/.pixeltable"
pxt schema update basic/app.py dagster_demo
dagster asset materialize -f basic/definitions.py -a defs --select ingest_documents
```

On Dagster 1.13 and later, `dagster asset materialize` prints a supersession warning and still runs. For the UI,
install `dagster-webserver` and start `dagster dev -f basic/definitions.py -a defs` from the same shell, so it
inherits `PIXELTABLE_HOME`.

### `examples/basic`: upsert by source id

`dagster_demo.documents` has a `source_id` primary key, two input columns, and two computed columns. The
`ingest_documents` asset reads `basic/documents.json` and writes it with
`batch_update(records, if_not_exists="insert")`:

- **Replay.** A record whose `source_id` already exists updates that row, so running the asset twice keeps one row
  per id. Each run can still advance the table version and recompute values. UDF side effects are not
  exactly-once.
- **Rejected input.** A record without a `source_id`, or two records with the same one, fail the run before the
  write. An empty file skips the write and succeeds.
- **SDK errors.** An unknown column or a value of the wrong type raises, and the table is unchanged.
- **Computed errors.** A record with `page_count` 0 commits with a division error stored in `chars_per_page`. The
  asset reports the count in `pixeltable/num_excs`, and the `documents_computed` check fails. A failed check does
  not undo the write.

The asset returns a `MaterializeResult` with the table path, row count, column schema, table id, and version. The
version is the one observed after the write; downstream reads see the current table, not that version.

### `examples/video`: append media

`batch_update` cannot write media columns, so `ingest_videos` uses `insert`. Each run appends `clip.mp4` again, and
the `frames` view extracts its frames. Declare media columns explicitly, as `pxt.Video` here: a URL inserted into
an inferred schema becomes a `String` column, which `frame_iterator` rejects.

```sh
pxt schema update video/app.py dagster_video_demo
dagster asset materialize -f video/definitions.py -a defs --select ingest_videos
```

## Development

```sh
make install
make test
make ruff
make check
```

The tests create a temporary Pixeltable home with its own embedded Postgres server and pxt daemon, and stop both
when the session ends.

Tested on macOS and Linux.
