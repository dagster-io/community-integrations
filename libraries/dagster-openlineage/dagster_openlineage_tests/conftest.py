# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import time
from typing import Optional

from openlineage.client.uuid import generate_new_uuid

from dagster import (
    DagsterEvent,
    DagsterEventType,
    DagsterRun,
    EventLogEntry,
    EventLogRecord,
)
from dagster._core.execution.plan.objects import StepFailureData, StepSuccessData
from dagster._core.types.loadable_target_origin import LoadableTargetOrigin

from dagster_openlineage.version import __version__ as OPENLINEAGE_DAGSTER_VERSION

PRODUCER = (
    f"https://github.com/OpenLineage/OpenLineage/tree/"
    f"{OPENLINEAGE_DAGSTER_VERSION}/integration/dagster"
)


def make_pipeline_run_with_external_pipeline_origin(
    repository_name: str,
) -> DagsterRun:
    """Construct a DagsterRun with a RemoteJobOrigin for tests.

    Floor is Dagster >= 1.11.6; ``dagster._core.remote_origin`` is the current
    import path. Older origin modules were removed with the floor raise in
    v0.2.
    """
    from dagster._core.remote_origin import (
        InProcessCodeLocationOrigin,
        RemoteJobOrigin,
        RemoteRepositoryOrigin,
    )

    return DagsterRun(
        job_name="test",
        execution_plan_snapshot_id="123",
        remote_job_origin=RemoteJobOrigin(
            repository_origin=RemoteRepositoryOrigin(
                code_location_origin=InProcessCodeLocationOrigin(
                    loadable_target_origin=LoadableTargetOrigin(
                        python_file="/openlineage/dagster/tests/test_pipelines/repo.py",
                    )
                ),
                repository_name=repository_name,
            ),
            job_name="test",
        ),
    )


def make_test_event_log_record(
    event_type: Optional[DagsterEventType] = None,
    pipeline_name: str = "a_job",
    pipeline_run_id: Optional[str] = None,
    timestamp: Optional[float] = None,
    step_key: Optional[str] = None,
    storage_id: int = 1,
):
    event_log_entry = EventLogEntry(
        None,
        "debug",
        "user_msg",
        pipeline_run_id or str(generate_new_uuid()),
        timestamp or time.time(),
        step_key,
        pipeline_name,
        _make_dagster_event(event_type, pipeline_name, step_key)
        if event_type
        else None,
    )

    return EventLogRecord(storage_id=storage_id, event_log_entry=event_log_entry)


def _make_dagster_event(
    event_type: DagsterEventType, pipeline_name: str, step_key: Optional[str]
):
    event_specific_data = None
    if event_type == DagsterEventType.STEP_SUCCESS:
        event_specific_data = StepSuccessData(duration_ms=1.0)
    elif event_type == DagsterEventType.STEP_FAILURE:
        event_specific_data = StepFailureData(error=None, user_failure_data=None)
    return DagsterEvent(
        event_type.value,
        pipeline_name,
        step_key=step_key,
        event_specific_data=event_specific_data,
    )
