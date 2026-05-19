# dagster-openlineage

Asset-centric OpenLineage emission for Dagster. Emits schema, column-lineage, data-quality-assertion, and partition-nominal-time facets alongside existing run/step events. Two emission mechanisms are available — pick exactly one per deployment.

## Features

- **Asset-centric emission** — materializations, observations, check evaluations, and synthesized failures
- **Schema + column lineage facets** from `dagster/column_schema` and `dagster/column_lineage` metadata
- **Data quality assertions** placed on `InputDataset` (spec-conformant)
- **Partition → nominal time** heuristic (ISO date or date-hour)
- **Multi-tenant namespaces** via string templates (`{namespace}`, `{tag:KEY}`)
- **Bounded emit** — synchronous, 2s default timeout, retries disabled, failures swallowed
- **Pipeline / step events** preserved (v0.1 surface unchanged)

## Requirements

- Python 3.10+
- Dagster `>=1.11.6`

## Installation

```bash
pip install dagster-openlineage
```

## Compatibility matrix

| Environment | Mechanism A (storage wrapper) | Mechanism B (sensor) |
|---|---|---|
| OSS Dagster (self-hosted) | ✅ | ✅ |
| OSS Dagster, `dagster dev` | ✅ | ✅ |
| Dagster+ Hybrid | ❌ (operator doesn't control `instance.yaml`) | ✅ |
| Dagster+ Serverless | ❌ | ✅ |
| Dagster+ Branch Deployments | ❌ | ✅ |

## Which mechanism do I want?

- **You control `instance.yaml`** → Mechanism A (push). Every event hits OpenLineage as it is persisted. No daemon dependency. Process-local failure synthesis (see Known limitations).
- **You run on Dagster+ Hybrid / Serverless / Branch Deployments** → Mechanism B (polled). The sensor tails the event log and converts asset events into OpenLineage emissions.
- **Both at once** → don't. See *Pick exactly one mechanism* below.

## Mechanism A — storage wrapper (push)

Configure `OpenLineageEventLogStorage` as your event log storage. It composes any inner `EventLogStorage` class and intercepts `store_event` / `store_event_batch` to emit OpenLineage events.

```yaml
# instance.yaml
event_log_storage:
  module: dagster_openlineage
  class: OpenLineageEventLogStorage
  config:
    wrapped:
      module: dagster_postgres.event_log
      class: PostgresEventLogStorage
      config:
        postgres_url:
          env: DAGSTER_PG_URL
    namespace: my-company
    # Optional:
    # namespace_template: "{namespace}/{tag:tenant}"
    # timeout: 2.0
    # strict_assertion_mapping: false
```

Set `OPENLINEAGE_URL` (and optionally `OPENLINEAGE_API_KEY`) in the environment of any process that writes Dagster events — typically the run worker and the daemon.

## Mechanism B — sensor (polled)

Add `openlineage_sensor(include_asset_events=True)` to your `Definitions`. v0.2 keeps `include_asset_events=False` as the default (v0.1 parity); v0.3 will flip it.

```python
from dagster import Definitions
from dagster_openlineage import openlineage_sensor

defs = Definitions(
    assets=[...],
    sensors=[openlineage_sensor(include_asset_events=True)],
)
```

Environment variables go on the process that runs the Dagster daemon:

- `OPENLINEAGE_URL` (required)
- `OPENLINEAGE_API_KEY` (optional)
- `OPENLINEAGE_NAMESPACE` (optional, default `dagster`)

## Namespace templates

Provide `namespace_template` (wrapper) or let the adapter/sensor derive the default. v0.2 supports two token families:

- `{namespace}` — the configured default namespace
- `{tag:KEY}` — the run tag named `KEY`, empty if unset

Adjacent slashes collapse; trailing slashes strip. Unknown tokens raise `NamespaceTemplateError` at construction. `{code_location}` and `{repository}` are deferred to v0.3 — they do not reliably reach `store_event` time.

Example:

```python
# Template
"{namespace}/{tag:tenant}"

# Run tags            -> resolved namespace
{"tenant": "acme"}    -> "dagster/acme"
{}                    -> "dagster"          # tag unresolved, trailing slash stripped
```

## Emit path

The adapter emits synchronously and swallows `Exception`; `BaseException` (shutdown signals, `SystemExit`, `KeyboardInterrupt`, `MemoryError`) propagates. The default transport disables retries to keep the per-event wall clock bounded by `timeout` (default 2s). Configure retry/async behavior via OpenLineage's own `openlineage.yml` or `OPENLINEAGE_CONFIG` if you need it.

## Pick exactly one mechanism

Configuring Mechanism A and Mechanism B simultaneously produces duplicate OpenLineage events. OpenLineage has no client-side idempotency primitive and backend dedup is not spec-defined (Marquez dedupes on `(runId, eventType, eventTime)`; DataHub and OpenMetadata do not). This is a deployment-time contract — the library does not enforce it at runtime. Mechanism B logs a WARN at sensor construction when opted in, reminding operators of this.

## Migration from v0.1

1. Pin `dagster>=1.11.6`.
2. Namespace default is flat (`dagster`). v0.1 used `repository_name` as the namespace when reachable — existing OL-backend lineage keyed under that old namespace may need a one-time rename. Set `OPENLINEAGE_NAMESPACE` or configure `namespace_template` to match your prior layout.
3. If you relied on the `openlineage_sensor` in v0.1, it still works unchanged — asset events are now supported via the new `include_asset_events=True` flag (default remains `False`).
4. `OpenLineageEventListener` is removed; it was a dead stub with no call sites.

## Sensor Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `minimum_interval_seconds` | 300 | Minimum seconds between sensor evaluations |
| `record_filter_limit` | 30 | Max number of event logs to process per evaluation |
| `after_storage_id` | 0 | Starting storage ID for event processing |
| `include_asset_events` | `False` (v0.2) | Opt in to asset-level emission |

## Not in v0.2

- Dagster+ Insights bridge / Catalog UI mirroring
- IO manager enrichment (`LOADED_INPUT` / `HANDLED_OUTPUT`)
- Spark / engine-specific facets
- SQL parsing or column-graph inference (reads `dagster/column_lineage` metadata if present)
- Python-callable `naming_fn` (string templating only; callable form deferred to v0.3)
- `{code_location}` and `{repository}` namespace tokens (deferred to v0.3)
- Shipped JSON schema file for the custom `dagster_asset_check` run facet (deferred to v0.2.1)
- Wrapper-side synthesis reconciliation across process restarts (deferred to v0.2.1; use Mechanism B if you need crash-tolerant failure reporting)

## Version Compatibility

This library supports Dagster `>=1.11.6`. End users get the constraint from `pyproject.toml`; CI and reproducible builds use the committed `uv.lock`.

## Development

```bash
uv sync
make test
make ruff
make check
```
