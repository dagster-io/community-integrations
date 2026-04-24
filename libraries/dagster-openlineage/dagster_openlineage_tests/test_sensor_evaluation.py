# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import tempfile
import time
from unittest import mock
from unittest.mock import call, patch

from openlineage.client.uuid import generate_new_uuid
from dagster_openlineage.sensor import openlineage_sensor

from dagster_openlineage.compat import DagsterEventType
from dagster import (
    AssetKey,
    AssetMaterialization,
    DagsterEvent,
    EventLogEntry,
    EventLogRecord,
    build_sensor_context,
    execute_job,
    job,
    op,
    reconstructable,
)
from dagster._core.events import (
    AssetMaterializationPlannedData,
    StepMaterializationData,
)

from dagster.core.test_utils import instance_for_test

from .conftest import make_test_event_log_record


def _make_asset_event_record(
    event_type: DagsterEventType,
    run_id: str,
    asset_key: AssetKey,
    storage_id: int = 1,
    partition: str | None = None,
) -> EventLogRecord:
    if event_type == DagsterEventType.ASSET_MATERIALIZATION_PLANNED:
        esd = AssetMaterializationPlannedData(asset_key=asset_key, partition=partition)
    elif event_type == DagsterEventType.ASSET_MATERIALIZATION:
        esd = StepMaterializationData(
            materialization=AssetMaterialization(
                asset_key=asset_key, partition=partition
            ),
        )
    else:
        raise NotImplementedError(event_type)

    entry = EventLogEntry(
        error_info=None,
        level="debug",
        user_message="",
        run_id=run_id,
        timestamp=time.time(),
        step_key="an_op",
        job_name="a_job",
        dagster_event=DagsterEvent(
            event_type_value=event_type.value,
            job_name="a_job",
            step_key="an_op",
            event_specific_data=esd,
        ),
    )
    return EventLogRecord(storage_id=storage_id, event_log_entry=entry)


@job
def a_job():
    @op
    def an_op():
        pass

    an_op()


@patch("dagster_openlineage.sensor.get_repository_name")
@patch("dagster_openlineage.sensor.make_step_run_id")
@patch("dagster_openlineage.sensor._ADAPTER")
def test_sensor_with_complete_job_run_and_repository(
    mock_adapter, mock_step_run_id, mock_get_repository_name
):
    with tempfile.TemporaryDirectory() as temp_dir:
        with instance_for_test(
            temp_dir=temp_dir,
            overrides={  # to avoid run-sharded event log storage warning
                "event_log_storage": {
                    "module": "dagster.core.storage.event_log",
                    "class": "ConsolidatedSqliteEventLogStorage",
                    "config": {"base_dir": temp_dir},
                },
            },
        ) as instance:
            repository_name = "a_repository"
            pipeline_name = "a_job"
            step_key = "an_op"
            step_run_id = str(generate_new_uuid())
            mock_step_run_id.return_value = step_run_id
            mock_get_repository_name.return_value = repository_name

            result = execute_job(
                job=reconstructable(a_job), instance=instance, raise_on_error=False
            )
            pipeline_run_id = result.run_id

            context = build_sensor_context(instance=instance)
            previous_cursor = None
            while True:
                openlineage_sensor().evaluate_tick(context)
                if context.cursor == previous_cursor:
                    break
                else:
                    previous_cursor = context.cursor

            mock_adapter.assert_has_calls(
                [
                    call.start_pipeline(
                        pipeline_name, pipeline_run_id, mock.ANY, repository_name
                    ),
                    call.start_step(
                        pipeline_name,
                        pipeline_run_id,
                        mock.ANY,
                        step_run_id,
                        step_key,
                        repository_name,
                    ),
                    call.complete_step(
                        pipeline_name,
                        pipeline_run_id,
                        mock.ANY,
                        step_run_id,
                        step_key,
                        repository_name,
                    ),
                    call.complete_pipeline(
                        pipeline_name, pipeline_run_id, mock.ANY, repository_name
                    ),
                ]
            )


@patch("dagster_openlineage.sensor._ADAPTER")
@patch("dagster_openlineage.sensor.get_event_log_records")
def test_sensor_start_pipeline(mock_event_log_records, mock_adapter):
    with instance_for_test() as instance:
        pipeline_name = "a_job"
        pipeline_run_id = str(generate_new_uuid())
        timestamp = time.time()
        mock_event_log_records.return_value = [
            make_test_event_log_record(
                DagsterEventType.RUN_START, pipeline_name, pipeline_run_id, timestamp
            )
        ]

        context = build_sensor_context(instance=instance)
        openlineage_sensor().evaluate_tick(context)

        mock_adapter.assert_has_calls(
            [call.start_pipeline(pipeline_name, pipeline_run_id, timestamp, None)]
        )


@patch("dagster_openlineage.sensor._ADAPTER")
@patch("dagster_openlineage.sensor.get_event_log_records")
def test_sensor_complete_pipeline(mock_event_log_records, mock_adapter):
    with instance_for_test() as instance:
        pipeline_name = "a_job"
        pipeline_run_id = str(generate_new_uuid())
        timestamp = time.time()
        mock_event_log_records.return_value = [
            make_test_event_log_record(
                DagsterEventType.RUN_SUCCESS, pipeline_name, pipeline_run_id, timestamp
            )
        ]

        context = build_sensor_context(instance=instance)
        openlineage_sensor().evaluate_tick(context)

        mock_adapter.assert_has_calls(
            [call.complete_pipeline(pipeline_name, pipeline_run_id, timestamp, None)]
        )


@patch("dagster_openlineage.sensor._ADAPTER")
@patch("dagster_openlineage.sensor.get_event_log_records")
def test_sensor_fail_pipeline(mock_event_log_records, mock_adapter):
    with instance_for_test() as instance:
        pipeline_name = "a_job"
        pipeline_run_id = str(generate_new_uuid())
        timestamp = time.time()
        mock_event_log_records.return_value = [
            make_test_event_log_record(
                DagsterEventType.RUN_FAILURE, pipeline_name, pipeline_run_id, timestamp
            )
        ]

        context = build_sensor_context(instance=instance)
        openlineage_sensor().evaluate_tick(context)

        mock_adapter.assert_has_calls(
            [call.fail_pipeline(pipeline_name, pipeline_run_id, timestamp, None)]
        )


@patch("dagster_openlineage.sensor._ADAPTER")
@patch("dagster_openlineage.sensor.get_event_log_records")
def test_sensor_cancel_pipeline(mock_event_log_records, mock_adapter):
    with instance_for_test() as instance:
        pipeline_name = "a_job"
        pipeline_run_id = str(generate_new_uuid())
        timestamp = time.time()
        mock_event_log_records.return_value = [
            make_test_event_log_record(
                DagsterEventType.RUN_CANCELED, pipeline_name, pipeline_run_id, timestamp
            )
        ]

        context = build_sensor_context(instance=instance)
        openlineage_sensor().evaluate_tick(context)

        mock_adapter.assert_has_calls(
            [call.cancel_pipeline(pipeline_name, pipeline_run_id, timestamp, None)]
        )


@patch("dagster_openlineage.sensor.make_step_run_id")
@patch("dagster_openlineage.sensor._ADAPTER")
@patch("dagster_openlineage.sensor.get_event_log_records")
def test_sensor_start_step(mock_event_log_records, mock_adapter, mock_new_step_run_id):
    with instance_for_test() as instance:
        pipeline_name = "a_job"
        pipeline_run_id = str(generate_new_uuid())
        timestamp = time.time()
        step_run_id = str(generate_new_uuid())
        step_key = "an_op"
        mock_event_log_records.return_value = [
            make_test_event_log_record(
                DagsterEventType.STEP_START,
                pipeline_name,
                pipeline_run_id,
                timestamp,
                step_key,
            )
        ]
        mock_new_step_run_id.return_value = step_run_id

        context = build_sensor_context(instance=instance)
        openlineage_sensor().evaluate_tick(context)

        mock_adapter.assert_has_calls(
            [
                call.start_step(
                    pipeline_name,
                    pipeline_run_id,
                    timestamp,
                    step_run_id,
                    step_key,
                    None,
                )
            ]
        )


@patch("dagster_openlineage.sensor.make_step_run_id")
@patch("dagster_openlineage.sensor._ADAPTER")
@patch("dagster_openlineage.sensor.get_event_log_records")
def test_sensor_complete_step(
    mock_event_log_records, mock_adapter, mock_new_step_run_id
):
    with instance_for_test() as instance:
        pipeline_name = "a_job"
        pipeline_run_id = str(generate_new_uuid())
        timestamp = time.time()
        step_run_id = str(generate_new_uuid())
        step_key = "an_op"
        mock_new_step_run_id.return_value = step_run_id
        mock_event_log_records.return_value = [
            make_test_event_log_record(
                DagsterEventType.STEP_SUCCESS,
                pipeline_name,
                pipeline_run_id,
                timestamp,
                step_key,
            )
        ]

        context = build_sensor_context(instance=instance)
        openlineage_sensor().evaluate_tick(context)

        mock_adapter.assert_has_calls(
            [
                call.complete_step(
                    pipeline_name,
                    pipeline_run_id,
                    timestamp,
                    step_run_id,
                    step_key,
                    None,
                )
            ]
        )


@patch("dagster_openlineage.sensor.make_step_run_id")
@patch("dagster_openlineage.sensor._ADAPTER")
@patch("dagster_openlineage.sensor.get_event_log_records")
def test_sensor_fail_step(mock_event_log_records, mock_adapter, mock_new_step_run_id):
    with instance_for_test() as instance:
        pipeline_name = "a_job"
        pipeline_run_id = str(generate_new_uuid())
        timestamp = time.time()
        step_run_id = str(generate_new_uuid())
        step_key = "an_op"
        mock_event_log_records.return_value = [
            make_test_event_log_record(
                DagsterEventType.STEP_FAILURE,
                pipeline_name,
                pipeline_run_id,
                timestamp,
                step_key,
            )
        ]
        mock_new_step_run_id.return_value = step_run_id

        context = build_sensor_context(instance=instance)
        openlineage_sensor().evaluate_tick(context)

        mock_adapter.assert_has_calls(
            [
                call.fail_step(
                    pipeline_name,
                    pipeline_run_id,
                    timestamp,
                    step_run_id,
                    step_key,
                    None,
                )
            ]
        )


@patch("dagster_openlineage.sensor._ADAPTER")
@patch("dagster_openlineage.sensor.get_event_log_records")
def test_sensor_default_does_not_emit_asset_events(
    mock_event_log_records, mock_adapter
):
    """R9: include_asset_events defaults to False — v0.1 parity preserved."""
    with instance_for_test() as instance:
        run_id = str(generate_new_uuid())
        mock_event_log_records.return_value = [
            _make_asset_event_record(
                DagsterEventType.ASSET_MATERIALIZATION,
                run_id,
                AssetKey(["orders"]),
            )
        ]

        context = build_sensor_context(instance=instance)
        openlineage_sensor().evaluate_tick(context)

        mock_adapter.asset_materialization.assert_not_called()


@patch("dagster_openlineage.sensor._ADAPTER")
@patch("dagster_openlineage.sensor.get_event_log_records")
def test_sensor_opt_in_dispatches_asset_materialization(
    mock_event_log_records, mock_adapter
):
    with instance_for_test() as instance:
        run_id = str(generate_new_uuid())
        mock_event_log_records.return_value = [
            _make_asset_event_record(
                DagsterEventType.ASSET_MATERIALIZATION,
                run_id,
                AssetKey(["orders"]),
            )
        ]

        context = build_sensor_context(instance=instance)
        openlineage_sensor(include_asset_events=True).evaluate_tick(context)

        mock_adapter.asset_materialization.assert_called_once()


@patch("dagster_openlineage.sensor._ADAPTER")
@patch("dagster_openlineage.sensor.get_event_log_records")
def test_sensor_opt_in_synthesizes_failure_for_planned_without_materialization(
    mock_event_log_records, mock_adapter
):
    with instance_for_test() as instance:
        run_id = str(generate_new_uuid())
        mock_event_log_records.return_value = [
            _make_asset_event_record(
                DagsterEventType.ASSET_MATERIALIZATION_PLANNED,
                run_id,
                AssetKey(["orders"]),
                storage_id=1,
            ),
            make_test_event_log_record(
                DagsterEventType.RUN_FAILURE,
                pipeline_run_id=run_id,
                storage_id=2,
            ),
        ]

        context = build_sensor_context(instance=instance)
        openlineage_sensor(
            include_asset_events=True, record_filter_limit=10
        ).evaluate_tick(context)

        mock_adapter.asset_materialization_planned.assert_called_once()
        mock_adapter.asset_failed_to_materialize.assert_called_once()
        failed_key = mock_adapter.asset_failed_to_materialize.call_args.args[0]
        assert failed_key == AssetKey(["orders"])


@patch("dagster_openlineage.sensor._ADAPTER")
@patch("dagster_openlineage.sensor.get_event_log_records")
def test_sensor_opt_in_no_synthesis_when_planned_materializes(
    mock_event_log_records, mock_adapter
):
    with instance_for_test() as instance:
        run_id = str(generate_new_uuid())
        mock_event_log_records.return_value = [
            _make_asset_event_record(
                DagsterEventType.ASSET_MATERIALIZATION_PLANNED,
                run_id,
                AssetKey(["orders"]),
                storage_id=1,
            ),
            _make_asset_event_record(
                DagsterEventType.ASSET_MATERIALIZATION,
                run_id,
                AssetKey(["orders"]),
                storage_id=2,
            ),
            make_test_event_log_record(
                DagsterEventType.RUN_FAILURE,
                pipeline_run_id=run_id,
                storage_id=3,
            ),
        ]

        context = build_sensor_context(instance=instance)
        openlineage_sensor(
            include_asset_events=True, record_filter_limit=10
        ).evaluate_tick(context)

        mock_adapter.asset_failed_to_materialize.assert_not_called()


@patch("dagster_openlineage.sensor._ADAPTER")
@patch("dagster_openlineage.sensor.get_event_log_records")
def test_sensor_opt_in_synthesis_selective_per_asset(
    mock_event_log_records, mock_adapter
):
    with instance_for_test() as instance:
        run_id = str(generate_new_uuid())
        mock_event_log_records.return_value = [
            _make_asset_event_record(
                DagsterEventType.ASSET_MATERIALIZATION_PLANNED,
                run_id,
                AssetKey(["a"]),
                storage_id=1,
            ),
            _make_asset_event_record(
                DagsterEventType.ASSET_MATERIALIZATION_PLANNED,
                run_id,
                AssetKey(["b"]),
                storage_id=2,
            ),
            _make_asset_event_record(
                DagsterEventType.ASSET_MATERIALIZATION,
                run_id,
                AssetKey(["a"]),
                storage_id=3,
            ),
            make_test_event_log_record(
                DagsterEventType.RUN_FAILURE,
                pipeline_run_id=run_id,
                storage_id=4,
            ),
        ]

        context = build_sensor_context(instance=instance)
        openlineage_sensor(
            include_asset_events=True, record_filter_limit=10
        ).evaluate_tick(context)

        assert mock_adapter.asset_failed_to_materialize.call_count == 1
        failed_key = mock_adapter.asset_failed_to_materialize.call_args.args[0]
        assert failed_key == AssetKey(["b"])
