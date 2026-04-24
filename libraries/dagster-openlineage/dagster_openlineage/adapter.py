# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import logging
import os
from typing import Dict, Optional

from openlineage.client import OpenLineageClient, set_producer
from openlineage.client.constants import DEFAULT_NAMESPACE_NAME
from openlineage.client.event_v2 import Job, Run, RunEvent, RunState
from openlineage.client.facet_v2 import parent_run as parent_run_facet

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
    """Adapter to translate Dagster metadata to OpenLineage events
    and emit them to OpenLineage backend
    """

    def __init__(self):
        self._client = OpenLineageClient()

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
        namespace = repository_name if repository_name else _DEFAULT_NAMESPACE_NAME
        self._emit(
            RunEvent(
                eventType=event_type,
                eventTime=to_utc_iso_8601(timestamp),
                run=self._build_run(
                    namespace=namespace,
                    run_id=pipeline_run_id,
                ),
                job=self._build_job(namespace=namespace, job_name=pipeline_name),
                producer=_PRODUCER,
            )
        )

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
        namespace = repository_name if repository_name else _DEFAULT_NAMESPACE_NAME
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

    def _emit(self, event: RunEvent):
        self._client.emit(event)
        log.debug("Successfully emitted OpenLineage run event: %s", event)

    @staticmethod
    def _build_run(
        namespace: str,
        run_id: str,
        parent_run_id: Optional[str] = None,
        parent_job_name: Optional[str] = None,
    ) -> Run:
        facets: Dict = {}
        if parent_run_id is not None and parent_job_name is not None:
            facets["parent"] = parent_run_facet.ParentRunFacet(
                run=parent_run_facet.Run(runId=parent_run_id),
                job=parent_run_facet.Job(namespace=namespace, name=parent_job_name),
            )
        return Run(runId=run_id, facets=facets)

    @staticmethod
    def _build_job(namespace: str, job_name: str) -> Job:
        return Job(namespace=namespace, name=job_name, facets={})
