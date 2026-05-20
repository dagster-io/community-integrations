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
    """Returns a list of Dagster event log records in ascending storage_id order.

    Each event type is queried separately with ``record_filter_limit`` to bound
    per-type fetch size. The merged results are then sorted by ``storage_id``
    (monotonically increasing, guaranteed consistent with the cursor) and
    capped at ``record_filter_limit`` total so the sensor never processes more
    than that many records per tick regardless of how many event types are
    in the filter set.

    :param instance: active instance to get records from
    :param event_type: event type(s) to filter by
    :param last_storage_id: storage id to use as after cursor filter
    :param record_filter_limit: per-type fetch cap and total result cap (0 = unlimited)
    :return: list of Dagster event log records ordered by storage_id
    """
    event_type_set = event_type if isinstance(event_type, set) else {event_type}
    event_records: list[EventLogRecord] = []
    for item in event_type_set:
        event_records += instance.get_event_records(
            EventRecordsFilter(event_type=item, after_cursor=last_storage_id),
            limit=record_filter_limit,
        )
    merged = sorted(event_records, key=lambda record: record.storage_id)
    return merged[:record_filter_limit] if record_filter_limit else merged


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
