# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Pure facet builders for OpenLineage emission.

Builders translate Dagster metadata shapes to OpenLineage ``facet_v2`` objects
without performing any I/O. Callers (``adapter.py``, ``storage.py``,
``sensor.py``) are responsible for attaching the returned facets to the right
dataset or run event.

The ``dagster_asset_check`` custom run facet's ``_schemaURL`` points at a
release-tag-pinned path that does NOT resolve in v0.2. Most OpenLineage
backends accept unknown custom facets with an unresolvable ``_schemaURL`` as
warn-log, not reject. The real JSON schema file ships in v0.2.1 at a new
release-tag path; v0.2 installs keep their non-resolving URL (degraded but
functional).
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, Optional, Tuple

from dagster import (
    AssetCheckEvaluation,
    AssetCheckSeverity,
    TableColumnLineage,
    TableSchema,
)
from openlineage.client.facet_v2 import (
    column_lineage_dataset,
    data_quality_assertions_dataset,
    datasource_dataset,
    error_message_run,
    nominal_time_run,
    schema_dataset,
)

from dagster_openlineage.version import __version__ as OPENLINEAGE_DAGSTER_VERSION

_NOMINAL_TIME_FMT = "%Y-%m-%dT%H:%M:%S.%fZ"

# Release-tag-pinned. Does NOT resolve in v0.2 — the JSON file ships in v0.2.1.
# Version bumps force URL bumps, matching the _PRODUCER pattern in adapter.py.
DAGSTER_ASSET_CHECK_FACET_SCHEMA_URL = (
    "https://raw.githubusercontent.com/dagster-io/community-integrations/"
    f"dagster-openlineage-{OPENLINEAGE_DAGSTER_VERSION}/"
    "libraries/dagster-openlineage/schemas/dagster_asset_check_run_facet.json"
)

_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ISO_DATE_HOUR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[-T]\d{2}:\d{2}(:\d{2})?$")


def _to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime(_NOMINAL_TIME_FMT)


def build_schema_facet(table_schema: TableSchema) -> schema_dataset.SchemaDatasetFacet:
    """Translate a Dagster ``TableSchema`` into a ``SchemaDatasetFacet``."""
    fields = [
        schema_dataset.SchemaDatasetFacetFields(
            name=col.name,
            type=col.type,
            description=col.description,
        )
        for col in table_schema.columns
    ]
    return schema_dataset.SchemaDatasetFacet(fields=fields)


def build_column_lineage_facet(
    table_column_lineage: TableColumnLineage,
    upstream_namespace: str,
) -> column_lineage_dataset.ColumnLineageDatasetFacet:
    """Translate ``dagster/column_lineage`` metadata into a lineage facet.

    All upstream inputs share ``upstream_namespace`` because the namespace is
    resolved per-event upstream. If multi-tenant splits by upstream asset tag
    are needed later, accept a resolver callable here.
    """
    fields: dict[str, column_lineage_dataset.Fields] = {}
    for out_column, deps in table_column_lineage.deps_by_column.items():
        input_fields = [
            column_lineage_dataset.InputField(
                namespace=upstream_namespace,
                name="/".join(dep.asset_key.path),
                field=dep.column_name,
            )
            for dep in deps
            if dep.column_name is not None
        ]
        fields[out_column] = column_lineage_dataset.Fields(inputFields=input_fields)
    return column_lineage_dataset.ColumnLineageDatasetFacet(fields=fields)


def build_datasource_facet(
    *,
    name: Optional[str] = None,
    uri: Optional[str] = None,
) -> Optional[datasource_dataset.DatasourceDatasetFacet]:
    """Return a datasource facet when ``name`` or ``uri`` is present."""
    if not name and not uri:
        return None
    return datasource_dataset.DatasourceDatasetFacet(name=name, uri=uri)


def build_nominal_time_facet(
    partition_key: Optional[str],
) -> Optional[nominal_time_run.NominalTimeRunFacet]:
    """Heuristic partition-key parse; omit the facet on anything ambiguous.

    Recognized:
      * ``YYYY-MM-DD`` → 24h window starting at 00:00 UTC
      * ``YYYY-MM-DD-HH:MM`` / ``YYYY-MM-DDTHH:MM`` / ``...THH:MM:SS`` → 1h window

    Anything else — dynamic partitions, multi-partition composites (``|``), static
    non-date keys — returns ``None`` so the facet is omitted rather than misattributed.
    """
    if partition_key is None:
        return None
    # Multi-partition composites look like "2026-04-23|customer-42". Don't guess.
    if "|" in partition_key:
        return None

    parsed = _parse_partition_window(partition_key)
    if parsed is None:
        return None
    start, end = parsed
    return nominal_time_run.NominalTimeRunFacet(
        nominalStartTime=_to_iso(start), nominalEndTime=_to_iso(end)
    )


def _parse_partition_window(key: str) -> Optional[Tuple[datetime, datetime]]:
    if _ISO_DATE_RE.match(key):
        try:
            start = datetime.strptime(key, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            return None
        return start, start + timedelta(days=1)

    if _ISO_DATE_HOUR_RE.match(key):
        # Normalize "YYYY-MM-DDTHH:MM[:SS]" and "YYYY-MM-DD-HH:MM[:SS]" to a
        # single canonical form for strptime.
        raw = key.replace("T", "-", 1) if "T" in key else key
        for fmt in ("%Y-%m-%d-%H:%M", "%Y-%m-%d-%H:%M:%S"):
            try:
                parsed = datetime.strptime(raw, fmt)
            except ValueError:
                continue
            start = parsed.replace(minute=0, second=0, tzinfo=timezone.utc)
            return start, start + timedelta(hours=1)
    return None


def build_data_quality_assertions_facet(
    evaluations: Iterable[AssetCheckEvaluation],
    *,
    strict_assertion_mapping: bool = False,
) -> data_quality_assertions_dataset.DataQualityAssertionsDatasetFacet:
    """Build the ``InputDataset``-placed data quality assertions facet.

    WARN severities map to ``success=True`` by default (a warn is not a
    quality-gate break). Callers pass ``strict_assertion_mapping=True`` to treat
    WARN as failure (``success=False``).
    """
    assertions: list[data_quality_assertions_dataset.Assertion] = []
    for ev in evaluations:
        success = _evaluate_success(ev, strict_assertion_mapping)
        column = _extract_column_name(ev.metadata)
        assertions.append(
            data_quality_assertions_dataset.Assertion(
                assertion=ev.check_name,
                success=success,
                column=column,
            )
        )
    return data_quality_assertions_dataset.DataQualityAssertionsDatasetFacet(
        assertions=assertions
    )


def _evaluate_success(ev: AssetCheckEvaluation, strict_assertion_mapping: bool) -> bool:
    if not ev.passed:
        return False
    if strict_assertion_mapping and ev.severity == AssetCheckSeverity.WARN:
        return False
    return True


def _extract_column_name(metadata: Optional[Mapping[str, Any]]) -> Optional[str]:
    if not metadata:
        return None
    for key in ("column", "dagster/column"):
        value = metadata.get(key)
        if value is None:
            continue
        # Metadata values may be wrapped (e.g. TextMetadataValue); .value or .text.
        for attr in ("value", "text"):
            unwrapped = getattr(value, attr, None)
            if isinstance(unwrapped, str):
                return unwrapped
        if isinstance(value, str):
            return value
    return None


def build_error_message_facet(
    *, message: str, stack_trace: Optional[str] = None
) -> error_message_run.ErrorMessageRunFacet:
    return error_message_run.ErrorMessageRunFacet(
        message=message,
        programmingLanguage="python",
        stackTrace=stack_trace,
    )


def build_dagster_asset_check_run_facet(
    evaluations: Iterable[AssetCheckEvaluation],
) -> dict:
    """Return an inline custom ``dagster_asset_check`` run-facet payload.

    OL's ``Assertion`` has no severity field. This facet carries the Dagster
    severity plus raw check metadata for each evaluation. ``_schemaURL`` points
    at a release-tag-pinned path that does NOT resolve in v0.2 — the real JSON
    schema file ships in v0.2.1 (see module docstring).
    """
    checks = []
    for ev in evaluations:
        checks.append(
            {
                "assertion": ev.check_name,
                "severity": ev.severity.value,
                "passed": ev.passed,
                # Stringify metadata — the custom facet is a hint for severity,
                # not a structured contract. Backends that warn-log unknown
                # _schemaURLs will accept the inline bag.
                "check_metadata": {
                    str(k): str(v) for k, v in (ev.metadata or {}).items()
                },
            }
        )
    return {
        "dagster_asset_check": {
            "_producer": _PRODUCER_URL,
            "_schemaURL": DAGSTER_ASSET_CHECK_FACET_SCHEMA_URL,
            "checks": checks,
        }
    }


_PRODUCER_URL = (
    f"https://github.com/OpenLineage/OpenLineage/tree/"
    f"{OPENLINEAGE_DAGSTER_VERSION}/integration/dagster"
)
