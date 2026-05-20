# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import time
import uuid
from unittest.mock import patch

from openlineage.client.uuid import generate_new_uuid

from dagster import (
    DagsterEvent,
    DagsterEventType,
    EventLogEntry,
    EventLogRecord,
    EventRecordsFilter,
)
from dagster_openlineage.compat import PIPELINE_EVENTS, STEP_EVENTS
from dagster_openlineage.utils import (
    get_event_log_records,
    get_repository_name,
    make_step_job_name,
    make_step_run_id,
    to_utc_iso_8601,
)

from .conftest import make_pipeline_run_with_external_pipeline_origin


def test_to_utc_iso_8601():
    assert to_utc_iso_8601(1640995200) == "2022-01-01T00:00:00.000000Z"


def test_make_step_run_id():
    run_id = make_step_run_id()
    assert uuid.UUID(run_id).version == 7


def test_make_step_job_name():
    pipeline_name = "test_job"
    step_key = "test_graph.test_op"
    assert make_step_job_name(pipeline_name, step_key) == "test_job.test_graph.test_op"


@patch("dagster_openlineage.utils.DagsterInstance")
def test_get_event_log_records(mock_instance):
    last_storage_id = 100
    record_filter_limit = 100
    event_types = PIPELINE_EVENTS | STEP_EVENTS
    get_event_log_records(
        mock_instance, event_types, last_storage_id, record_filter_limit
    )
    for event_type in event_types:
        mock_instance.get_event_records.assert_any_call(
            EventRecordsFilter(event_type=event_type, after_cursor=last_storage_id),
            limit=record_filter_limit,
        )


@patch("dagster_openlineage.utils.DagsterInstance")
def test_get_event_log_records_sorted_by_storage_id(mock_instance):
    """Merged records must come out in storage_id order, not timestamp order."""
    now = time.time()

    def _make_record(storage_id: int, timestamp: float) -> EventLogRecord:
        entry = EventLogEntry(
            error_info=None,
            level="debug",
            user_message="",
            run_id="r",
            timestamp=timestamp,
            step_key=None,
            job_name="j",
            dagster_event=DagsterEvent(
                event_type_value=DagsterEventType.RUN_START.value,
                job_name="j",
            ),
        )
        return EventLogRecord(storage_id=storage_id, event_log_entry=entry)

    # Two records from different type queries: storage_id ordering differs from timestamp.
    rec_early_storage = _make_record(
        storage_id=1, timestamp=now + 10
    )  # older ID, newer ts
    rec_late_storage = _make_record(storage_id=5, timestamp=now)  # newer ID, older ts

    mock_instance.get_event_records.side_effect = [
        [rec_late_storage],
        [rec_early_storage],
    ]

    result = list(
        get_event_log_records(
            mock_instance,
            {DagsterEventType.RUN_START, DagsterEventType.RUN_SUCCESS},
            0,
            10,
        )
    )
    assert [r.storage_id for r in result] == [1, 5]


@patch("dagster_openlineage.utils.DagsterInstance")
def test_get_event_log_records_applies_total_limit(mock_instance):
    """Total returned records must not exceed record_filter_limit even when
    multiple event types each return that many records."""
    now = time.time()
    limit = 3

    def _make_record(storage_id: int) -> EventLogRecord:
        entry = EventLogEntry(
            error_info=None,
            level="debug",
            user_message="",
            run_id="r",
            timestamp=now,
            step_key=None,
            job_name="j",
            dagster_event=DagsterEvent(
                event_type_value=DagsterEventType.RUN_START.value,
                job_name="j",
            ),
        )
        return EventLogRecord(storage_id=storage_id, event_log_entry=entry)

    # Each type returns `limit` records — without a total cap we'd get 2*limit.
    mock_instance.get_event_records.side_effect = [
        [_make_record(i) for i in range(limit)],
        [_make_record(i + limit) for i in range(limit)],
    ]

    result = list(
        get_event_log_records(
            mock_instance,
            {DagsterEventType.RUN_START, DagsterEventType.RUN_SUCCESS},
            0,
            limit,
        )
    )
    assert len(result) == limit


@patch("dagster_openlineage.utils.DagsterInstance")
def test_get_repository_name(mock_instance):
    expected_repo = "test_repo"
    pipeline_run_id = str(generate_new_uuid())
    pipeline_run = make_pipeline_run_with_external_pipeline_origin(expected_repo)
    mock_instance.get_run_by_id.return_value = pipeline_run
    actual_repo = get_repository_name(mock_instance, pipeline_run_id)

    assert expected_repo == actual_repo
    mock_instance.get_run_by_id.assert_called_once_with(pipeline_run_id)
