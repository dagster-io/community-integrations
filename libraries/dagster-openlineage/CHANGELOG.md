# Changelog

## 0.2.0

### Breaking

- **Dagster floor raised to `>=1.11.6`.** v0.1's support for older Dagster versions (`>=1.6.9`) is dropped; pre-1.11.6 compat branches and origin shims are removed.
- **Default namespace is flat (`dagster` unless `OPENLINEAGE_NAMESPACE` is set).** v0.1 used `repository_name` when reachable via the run origin; it is not reliably reachable at `store_event` time (pipes-originated events have no run) and produced inconsistent namespaces across emission paths. Set `OPENLINEAGE_NAMESPACE` or configure `namespace_template` to preserve your prior layout.
- **`OpenLineageEventListener` removed.** It was a dead stub; no call sites existed.

### Features

- **Asset-centric OpenLineage emission** via two mechanisms. Pick exactly one per deployment.
  - **Mechanism A (push) — `OpenLineageEventLogStorage`**: a generic wrapper that composes any `EventLogStorage` via `ConfigurableClassData.rehydrate()` and intercepts `store_event` / `store_event_batch`. Available on any Dagster deployment that owns `instance.yaml`.
  - **Mechanism B (polled) — `openlineage_sensor(include_asset_events=True)`**: the existing sensor gains an opt-in flag. Works on Dagster+ Hybrid / Serverless / Branch Deployments. Default stays `False` in v0.2 for v0.1 parity; v0.3 will flip it.
- **Schema**, **column lineage**, **data source**, **nominal time** (partition-key heuristic), and **data quality assertions** (placed on `InputDataset` per the OL spec) facets. Error-message run facet on synthesized failures.
- **Custom `dagster_asset_check` run facet** carrying Dagster severity (`ERROR` / `WARN`) alongside standard assertions.
- **Namespace templates** — string tokens `{namespace}` and `{tag:KEY}`. Parsed at construction; unknown tokens raise `NamespaceTemplateError`. Adjacent slashes collapse, trailing slashes strip.
- **Synthesized asset-failure events** — when a run ends with `ASSET_MATERIALIZATION_PLANNED` entries that never materialized, both mechanisms emit `asset_failed_to_materialize` OL events.

### Internal

- Adapter imports migrated from `openlineage.client.facet` + `openlineage.client.run` to `openlineage.client.facet_v2` + `event_v2`.
- Emission routed through a new `OpenLineageEmitter` that disables HTTP transport retries (defeats the default `total=5 × backoff=0.3` stack, which would multiply the 2s per-event timeout ~7.5×) and rate-limits error logs per exception class.
- Pyright strict pass on v0.2-touched files. Remaining targeted `# pyright: ignore[...]` pragmas carry justifying comments.

### Known limitations

- **Wrapper synthesis state is process-local in v0.2.** If a run worker crashes without cleanly receiving `RUN_FAILURE`, planned-but-not-materialized assets will not produce synthesized OL failures. Users needing crash-tolerant failure reporting should configure Mechanism B (sensor-based, cursor-persisted state). Reconciliation is deferred to v0.2.1 pending a dedup-safe recovery design.
- **`dagster_asset_check` facet `_schemaURL` does not resolve in v0.2.** The URL is release-tag-pinned at `.../dagster-openlineage-0.2.0/...` but the actual JSON schema file ships in v0.2.1. Most OL backends accept unknown custom facets with an unresolvable `_schemaURL` as warn-log, not reject.
- **Both mechanisms configured together emit duplicate OL events.** OpenLineage has no client-side idempotency; backend dedup is not spec-defined. This is documented as a contract, not enforced at runtime. The sensor logs a WARN on opt-in.

## 0.1.0

Initial release — pipeline and step events only.
