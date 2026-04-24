# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Mapping, Optional, Sequence

from openlineage.client import OpenLineageClient, set_producer
from openlineage.client.constants import DEFAULT_NAMESPACE_NAME
from openlineage.client.event_v2 import (
    DatasetEvent,
    InputDataset,
    Job,
    OutputDataset,
    Run,
    RunEvent,
    RunState,
    StaticDataset,
)
from openlineage.client.facet_v2 import parent_run as parent_run_facet

from dagster import (
    AssetCheckEvaluation,
    AssetKey,
    TableColumnLineage,
    TableSchema,
)

from dagster_openlineage.emitter import (
    DEFAULT_TIMEOUT_SECONDS,
    OpenLineageEmitter,
)
from dagster_openlineage.facets import (
    build_column_lineage_facet,
    build_dagster_asset_check_run_facet,
    build_data_quality_assertions_facet,
    build_datasource_facet,
    build_error_message_facet,
    build_nominal_time_facet,
    build_schema_facet,
)
from dagster_openlineage.naming import (
    ParsedTemplate,
    parse_namespace_template,
    resolve_namespace,
)
from dagster_openlineage.utils import make_step_job_name, to_utc_iso_8601
from dagster_openlineage.version import __version__ as OPENLINEAGE_DAGSTER_VERSION

_DEFAULT_NAMESPACE_NAME = os.getenv("OPENLINEAGE_NAMESPACE", DEFAULT_NAMESPACE_NAME)
_PRODUCER = (
    f"https://github.com/OpenLineage/OpenLineage/tree/"
    f"{OPENLINEAGE_DAGSTER_VERSION}/integration/dagster"
)

set_producer(_PRODUCER)

log = logging.getLogger(__name__)


class OpenLineageAdapter:
    """Adapter translating Dagster events to OpenLineage emissions.

    Construction accepts optional namespace/template and strictness knobs; the
    v0.1 pipeline/step surface keeps its default arguments unchanged so
    existing callers work without modification.
    """

    def __init__(
        self,
        *,
        namespace: Optional[str] = None,
        namespace_template: Optional[str] = None,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        strict_assertion_mapping: bool = False,
        emitter: Optional[OpenLineageEmitter] = None,
    ) -> None:
        self._namespace = namespace or _DEFAULT_NAMESPACE_NAME
        self._parsed_template: Optional[ParsedTemplate] = (
            parse_namespace_template(namespace_template) if namespace_template else None
        )
        self._strict_assertion_mapping = strict_assertion_mapping
        self._client = OpenLineageClient()
        self._emitter = emitter or OpenLineageEmitter(
            timeout=timeout, client=self._client
        )

    # ------------------------------------------------------------------
    # v0.1 pipeline surface (preserved)
    # ------------------------------------------------------------------

    def start_pipeline(
        self,
        pipeline_name: str,
        pipeline_run_id: str,
        timestamp: float,
        repository_name: Optional[str] = None,
    ):
        self._emit_pipeline_event(
            RunState.START, pipeline_name, pipeline_run_id, timestamp, repository_name
        )

    def complete_pipeline(
        self,
        pipeline_name: str,
        pipeline_run_id: str,
        timestamp: float,
        repository_name: Optional[str] = None,
    ):
        self._emit_pipeline_event(
            RunState.COMPLETE,
            pipeline_name,
            pipeline_run_id,
            timestamp,
            repository_name,
        )

    def fail_pipeline(
        self,
        pipeline_name: str,
        pipeline_run_id: str,
        timestamp: float,
        repository_name: Optional[str] = None,
    ):
        self._emit_pipeline_event(
            RunState.FAIL, pipeline_name, pipeline_run_id, timestamp, repository_name
        )

    def cancel_pipeline(
        self,
        pipeline_name: str,
        pipeline_run_id: str,
        timestamp: float,
        repository_name: Optional[str] = None,
    ):
        self._emit_pipeline_event(
            RunState.ABORT, pipeline_name, pipeline_run_id, timestamp, repository_name
        )

    def _emit_pipeline_event(
        self,
        event_type: RunState,
        pipeline_name: str,
        pipeline_run_id: str,
        timestamp: float,
        repository_name: Optional[str],
    ):
        namespace = repository_name if repository_name else self._namespace
        self._emit(
            RunEvent(
                eventType=event_type,
                eventTime=to_utc_iso_8601(timestamp),
                run=self._build_run(namespace=namespace, run_id=pipeline_run_id),
                job=self._build_job(namespace=namespace, job_name=pipeline_name),
                producer=_PRODUCER,
            )
        )

    # ------------------------------------------------------------------
    # v0.1 step surface (preserved)
    # ------------------------------------------------------------------

    def start_step(
        self,
        pipeline_name: str,
        pipeline_run_id: str,
        timestamp: float,
        step_run_id: str,
        step_key: str,
        repository_name: Optional[str] = None,
    ):
        self._emit_step_event(
            RunState.START,
            pipeline_name,
            pipeline_run_id,
            timestamp,
            step_run_id,
            step_key,
            repository_name,
        )

    def complete_step(
        self,
        pipeline_name: str,
        pipeline_run_id: str,
        timestamp: float,
        step_run_id: str,
        step_key: str,
        repository_name: Optional[str] = None,
    ):
        self._emit_step_event(
            RunState.COMPLETE,
            pipeline_name,
            pipeline_run_id,
            timestamp,
            step_run_id,
            step_key,
            repository_name,
        )

    def fail_step(
        self,
        pipeline_name: str,
        pipeline_run_id: str,
        timestamp: float,
        step_run_id: str,
        step_key: str,
        repository_name: Optional[str] = None,
    ):
        self._emit_step_event(
            RunState.FAIL,
            pipeline_name,
            pipeline_run_id,
            timestamp,
            step_run_id,
            step_key,
            repository_name,
        )

    def _emit_step_event(
        self,
        event_type: RunState,
        pipeline_name: str,
        pipeline_run_id: str,
        timestamp: float,
        step_run_id: str,
        step_key: str,
        repository_name: Optional[str],
    ):
        namespace = repository_name if repository_name else self._namespace
        self._emit(
            RunEvent(
                eventType=event_type,
                eventTime=to_utc_iso_8601(timestamp),
                run=self._build_run(
                    namespace=namespace,
                    run_id=step_run_id,
                    parent_run_id=pipeline_run_id,
                    parent_job_name=pipeline_name,
                ),
                job=self._build_job(
                    namespace=namespace,
                    job_name=make_step_job_name(pipeline_name, step_key),
                ),
                producer=_PRODUCER,
            )
        )

    # ------------------------------------------------------------------
    # v0.2 asset surface
    # ------------------------------------------------------------------

    def asset_materialization_planned(
        self,
        asset_key: AssetKey,
        run_id: str,
        timestamp: float,
        *,
        run_tags: Optional[Mapping[str, str]] = None,
    ) -> None:
        namespace = self._resolve_namespace(run_tags)
        self._emit(
            RunEvent(
                eventType=RunState.START,
                eventTime=to_utc_iso_8601(timestamp),
                run=self._build_run(namespace=namespace, run_id=run_id),
                job=self._build_job(
                    namespace=namespace, job_name=asset_key.to_user_string()
                ),
                producer=_PRODUCER,
                outputs=[
                    OutputDataset(
                        namespace=namespace, name="/".join(asset_key.path), facets={}
                    )
                ],
            )
        )

    def asset_materialization(
        self,
        asset_key: AssetKey,
        run_id: str,
        timestamp: float,
        *,
        metadata: Optional[Mapping[str, Any]] = None,
        upstream_asset_keys: Optional[Sequence[AssetKey]] = None,
        partition_key: Optional[str] = None,
        run_tags: Optional[Mapping[str, str]] = None,
    ) -> None:
        namespace = self._resolve_namespace(run_tags)
        output_facets = self._build_asset_output_facets(metadata, namespace)
        run_facets = self._build_asset_run_facets(partition_key)

        inputs: List[InputDataset] = [
            InputDataset(namespace=namespace, name="/".join(key.path))
            for key in (upstream_asset_keys or [])
        ]

        self._emit(
            RunEvent(
                eventType=RunState.COMPLETE,
                eventTime=to_utc_iso_8601(timestamp),
                run=self._build_run(
                    namespace=namespace, run_id=run_id, run_facets=run_facets
                ),
                job=self._build_job(
                    namespace=namespace, job_name=asset_key.to_user_string()
                ),
                producer=_PRODUCER,
                inputs=inputs,
                outputs=[
                    OutputDataset(
                        namespace=namespace,
                        name="/".join(asset_key.path),
                        facets=output_facets,
                    )
                ],
            )
        )

    def asset_failed_to_materialize(
        self,
        asset_key: AssetKey,
        run_id: str,
        timestamp: float,
        *,
        error_message: Optional[str] = None,
        stack_trace: Optional[str] = None,
        partition_key: Optional[str] = None,
        run_tags: Optional[Mapping[str, str]] = None,
    ) -> None:
        namespace = self._resolve_namespace(run_tags)
        run_facets = self._build_asset_run_facets(partition_key)
        if error_message:
            run_facets["errorMessage"] = build_error_message_facet(
                message=error_message, stack_trace=stack_trace
            )
        self._emit(
            RunEvent(
                eventType=RunState.FAIL,
                eventTime=to_utc_iso_8601(timestamp),
                run=self._build_run(
                    namespace=namespace, run_id=run_id, run_facets=run_facets
                ),
                job=self._build_job(
                    namespace=namespace, job_name=asset_key.to_user_string()
                ),
                producer=_PRODUCER,
                outputs=[
                    OutputDataset(
                        namespace=namespace, name="/".join(asset_key.path), facets={}
                    )
                ],
            )
        )

    def asset_observation(
        self,
        asset_key: AssetKey,
        run_id: str,
        timestamp: float,
        *,
        metadata: Optional[Mapping[str, Any]] = None,
        partition_key: Optional[str] = None,
        run_tags: Optional[Mapping[str, str]] = None,
    ) -> None:
        namespace = self._resolve_namespace(run_tags)
        facets = self._build_asset_output_facets(metadata, namespace)
        dataset = StaticDataset(
            namespace=namespace, name="/".join(asset_key.path), facets=facets
        )
        run_facets = self._build_asset_run_facets(partition_key)
        event = DatasetEvent(
            eventTime=to_utc_iso_8601(timestamp),
            producer=_PRODUCER,
            dataset=dataset,
        )
        # DatasetEvent in event_v2 does not carry run facets; emit as-is.
        del run_id, run_facets
        self._emit(event)

    def asset_check_evaluation_planned(
        self,
        asset_key: AssetKey,
        check_name: str,
        run_id: str,
        timestamp: float,
        *,
        standalone: bool,
        run_tags: Optional[Mapping[str, str]] = None,
    ) -> None:
        # Only emit when a check runs in its own job (standalone). When the
        # check shares a run_id with the asset materialization, the
        # materialization start already covers this moment.
        if not standalone:
            return
        del check_name  # surfaced via Assertion entries on completion
        namespace = self._resolve_namespace(run_tags)
        job_name = f"{asset_key.to_user_string()}__checks"
        self._emit(
            RunEvent(
                eventType=RunState.START,
                eventTime=to_utc_iso_8601(timestamp),
                run=self._build_run(namespace=namespace, run_id=run_id),
                job=self._build_job(namespace=namespace, job_name=job_name),
                producer=_PRODUCER,
                inputs=[
                    InputDataset(namespace=namespace, name="/".join(asset_key.path))
                ],
            )
        )

    def asset_check_evaluation(
        self,
        asset_key: AssetKey,
        evaluations: Sequence[AssetCheckEvaluation],
        run_id: str,
        timestamp: float,
        *,
        run_tags: Optional[Mapping[str, str]] = None,
    ) -> None:
        namespace = self._resolve_namespace(run_tags)
        job_name = f"{asset_key.to_user_string()}__checks"
        quality_facet = build_data_quality_assertions_facet(
            evaluations, strict_assertion_mapping=self._strict_assertion_mapping
        )
        custom_run_facet = build_dagster_asset_check_run_facet(evaluations)

        input_ds = InputDataset(
            namespace=namespace,
            name="/".join(asset_key.path),
            inputFacets={"dataQualityAssertions": quality_facet},
        )
        run = self._build_run(
            namespace=namespace,
            run_id=run_id,
            run_facets={**custom_run_facet},
        )
        self._emit(
            RunEvent(
                eventType=RunState.COMPLETE,
                eventTime=to_utc_iso_8601(timestamp),
                run=run,
                job=self._build_job(namespace=namespace, job_name=job_name),
                producer=_PRODUCER,
                inputs=[input_ds],
            )
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _resolve_namespace(self, run_tags: Optional[Mapping[str, str]]) -> str:
        if self._parsed_template is None:
            return self._namespace
        resolved = resolve_namespace(
            self._parsed_template, self._namespace, run_tags or {}
        )
        return resolved or self._namespace

    def _build_asset_output_facets(
        self, metadata: Optional[Mapping[str, Any]], namespace: str
    ) -> Dict[str, Any]:
        facets: Dict[str, Any] = {}
        if not metadata:
            return facets
        schema = _extract_schema(metadata)
        if schema is not None:
            facets["schema"] = build_schema_facet(schema)
        col_lineage = _extract_column_lineage(metadata)
        if col_lineage is not None:
            facets["columnLineage"] = build_column_lineage_facet(col_lineage, namespace)
        uri = _extract_path_or_uri(metadata)
        ds_facet = build_datasource_facet(uri=uri) if uri else None
        if ds_facet is not None:
            facets["dataSource"] = ds_facet
        return facets

    def _build_asset_run_facets(self, partition_key: Optional[str]) -> Dict[str, Any]:
        facets: Dict[str, Any] = {}
        nominal = build_nominal_time_facet(partition_key)
        if nominal is not None:
            facets["nominalTime"] = nominal
        return facets

    def _emit(self, event):
        self._emitter.emit(event)
        log.debug("Emitted OpenLineage event: %s", type(event).__name__)

    @staticmethod
    def _build_run(
        namespace: str,
        run_id: str,
        *,
        parent_run_id: Optional[str] = None,
        parent_job_name: Optional[str] = None,
        run_facets: Optional[Mapping[str, Any]] = None,
    ) -> Run:
        facets: Dict[str, Any] = dict(run_facets) if run_facets else {}
        if parent_run_id is not None and parent_job_name is not None:
            facets["parent"] = parent_run_facet.ParentRunFacet(
                run=parent_run_facet.Run(runId=parent_run_id),
                job=parent_run_facet.Job(namespace=namespace, name=parent_job_name),
            )
        return Run(runId=run_id, facets=facets)

    @staticmethod
    def _build_job(namespace: str, job_name: str) -> Job:
        return Job(namespace=namespace, name=job_name, facets={})


def _extract_schema(metadata: Mapping[str, Any]) -> Optional[TableSchema]:
    for key in ("dagster/column_schema", "column_schema"):
        value = metadata.get(key)
        if value is None:
            continue
        schema = getattr(value, "schema", value)
        if isinstance(schema, TableSchema):
            return schema
    return None


def _extract_column_lineage(
    metadata: Mapping[str, Any],
) -> Optional[TableColumnLineage]:
    for key in ("dagster/column_lineage", "column_lineage"):
        value = metadata.get(key)
        if value is None:
            continue
        lineage = getattr(value, "value", value)
        if isinstance(lineage, TableColumnLineage):
            return lineage
    return None


def _extract_path_or_uri(metadata: Mapping[str, Any]) -> Optional[str]:
    for key in ("path", "uri"):
        value = metadata.get(key)
        if value is None:
            continue
        for attr in ("path", "value", "text"):
            unwrapped = getattr(value, attr, None)
            if isinstance(unwrapped, str):
                return unwrapped
        if isinstance(value, str):
            return value
    return None
