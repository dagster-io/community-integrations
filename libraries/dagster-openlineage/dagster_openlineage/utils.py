# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timezone
from typing import Iterable, Optional, Set, Union

from openlineage.client.uuid import generate_new_uuid

from dagster import DagsterInstance, EventLogRecord, EventRecordsFilter
from dagster_openlineage.compat import (
    SensorDefinition,  # noqa: F401
    DagsterEventType,
    get_pipeline_origin,
    get_job_origin,
    get_repository_origin,
)  # noqa: F401

# Type hints for Pyright
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dagster import DagsterRun  # noqa: F401

NOMINAL_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def to_utc_iso_8601(timestamp: float) -> str:
    """Convert Unix timestamp to UTC ISO 8601 format string.

    Uses timezone-aware datetime to avoid deprecation warnings.
    """
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime(
        NOMINAL_TIME_FORMAT
    )


def make_step_run_id() -> str:
    return str(generate_new_uuid())


def make_step_job_name(pipeline_name: str, step_key: str) -> str:
    return f"{pipeline_name}.{step_key}"


def get_event_log_records(
    instance: DagsterInstance,
    event_type: Union[DagsterEventType, Set[DagsterEventType]],
    last_storage_id: int,
    record_filter_limit: int,
) -> Iterable[EventLogRecord]:
    """Returns a list of Dagster event log records in ascending order
    from the instance's event log storage.
    :param instance: active instance to get records from
    :param event_type: event type to filter by
    :param last_storage_id: storage id to use as after cursor filter
    :param record_filter_limit: maximum number of event logs to retrieve
    :return: list of Dagster event log records
    """
    event_type_set = event_type if isinstance(event_type, set) else set([event_type])
    event_records: list[EventLogRecord] = []
    for item in event_type_set:
        event_records += instance.get_event_records(
            EventRecordsFilter(event_type=item, after_cursor=last_storage_id),
            limit=record_filter_limit,
        )
    return sorted(event_records, key=lambda record: record.event_log_entry.timestamp)


def get_repository_name(
    instance: DagsterInstance, pipeline_run_id: str
) -> Optional[str]:
    """Returns an optional repository name
    :param instance: active instance to get the pipeline run
    :param pipeline_run_id: run id to look up its run
    :return: optional repository name
    """

    pipeline_run: Optional["DagsterRun"] = instance.get_run_by_id(pipeline_run_id)
    repository_name: Optional[str] = None

    if pipeline_run:
        origin: Any = get_pipeline_origin(pipeline_run) or get_job_origin(pipeline_run)
        repo_origin: Any = get_repository_origin(origin)
        if repo_origin:
            repository_name = repo_origin.repository_name

    return repository_name
