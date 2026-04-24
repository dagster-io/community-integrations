# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Any, Dict, Optional, Set, Tuple, Union

import dagster
from dagster import AssetKey

from dagster_openlineage.adapter import OpenLineageAdapter
from dagster_openlineage.compat import (
    DagsterEventType,
    DEFAULT_SENSOR_DAEMON_INTERVAL,
    PIPELINE_EVENTS,
    STEP_EVENTS,
    SensorDefinition,
)
from dagster_openlineage.cursor import (
    OpenLineageCursor,
    RunningPipeline,
    RunningStep,
)
from dagster_openlineage.utils import (
    get_event_log_records,
    get_repository_name,
    make_step_run_id,
)

_has_sensor_context = hasattr(dagster, "SensorEvaluationContext")
_has_skip_reason = hasattr(dagster, "SkipReason")
_has_sensor = hasattr(dagster, "sensor")

if _has_sensor_context and _has_skip_reason and _has_sensor:
    from dagster import SensorEvaluationContext, SkipReason, sensor
else:
    from dagster._core.definitions.run_request import (  # pyright: ignore[reportMissingImports]
        SkipReason,
    )
    from dagster._core.definitions.sensor_definition import (  # pyright: ignore[reportMissingImports]
        SensorEvaluationContext,
        sensor,
    )

_ADAPTER = OpenLineageAdapter()

log = logging.getLogger(__name__)

_ASSET_EVENT_TYPES: Set[DagsterEventType] = {
    DagsterEventType.ASSET_MATERIALIZATION,
    DagsterEventType.ASSET_MATERIALIZATION_PLANNED,
    DagsterEventType.ASSET_FAILED_TO_MATERIALIZE,
    DagsterEventType.ASSET_OBSERVATION,
    DagsterEventType.ASSET_CHECK_EVALUATION,
    DagsterEventType.ASSET_CHECK_EVALUATION_PLANNED,
}

# Event types outside the default PIPELINE | STEP filter that the asset path
# needs to observe. The others are already members of STEP_EVENTS.
_ASSET_ONLY_FILTER_EXTRAS: Set[DagsterEventType] = {
    DagsterEventType.ASSET_MATERIALIZATION_PLANNED,
    DagsterEventType.ASSET_FAILED_TO_MATERIALIZE,
    DagsterEventType.ASSET_CHECK_EVALUATION_PLANNED,
}

_RUN_TERMINATION_TYPES: Set[DagsterEventType] = {
    DagsterEventType.RUN_SUCCESS,
    DagsterEventType.RUN_FAILURE,
    DagsterEventType.RUN_CANCELED,
}


def openlineage_sensor(
    name: Optional[str] = "openlineage_sensor",
    description: Optional[str] = "OpenLineage sensor tails Dagster event logs, "
    "converts Dagster events into OpenLineage events, "
    "and emits them to an OpenLineage backend.",
    concerned_event_types: Optional[
        Union[DagsterEventType, Set[DagsterEventType]]
    ] = None,
    minimum_interval_seconds: Optional[int] = DEFAULT_SENSOR_DAEMON_INTERVAL,
    record_filter_limit: Optional[int] = 30,
    after_storage_id: Optional[int] = 0,
    include_asset_events: bool = False,
) -> SensorDefinition:
    """Build the OpenLineage sensor.

    ``include_asset_events=False`` (the v0.2 default) preserves v0.1 behavior —
    only pipeline and step events reach the dispatcher. Set it to ``True`` to
    opt in to asset-level emission via the sensor (Mechanism B). The flag is
    the opt-in switch per the v0.2 plan; a one-shot WARN at sensor
    construction reminds operators to configure at most one mechanism.

    v0.3 will flip the default to ``True``.
    """
    if concerned_event_types is None:
        filter_set = set(PIPELINE_EVENTS) | set(STEP_EVENTS)
        if include_asset_events:
            filter_set |= _ASSET_ONLY_FILTER_EXTRAS
        concerned_event_types = filter_set

    if include_asset_events:
        log.warning(
            "openlineage_sensor: include_asset_events=True. Configure at most "
            "one of (a) OpenLineageEventLogStorage wrapper, (b) this sensor "
            "with include_asset_events=True — running both emits duplicate OL "
            "events (OL has no client-side idempotency and backend dedup is "
            "not spec-defined)."
        )

    @sensor(  # pyright: ignore[reportCallIssue]
        name=name,
        minimum_interval_seconds=minimum_interval_seconds,
        description=description,
    )
    def _openlineage_sensor(context: SensorEvaluationContext):
        ol_cursor = (
            OpenLineageCursor.from_json(context.cursor)
            if context.cursor
            else OpenLineageCursor(after_storage_id)
        )
        last_storage_id = ol_cursor.last_storage_id
        running_pipelines = ol_cursor.running_pipelines

        event_log_records = get_event_log_records(
            context.instance,
            concerned_event_types,
            last_storage_id,
            record_filter_limit,
        )

        raised_exception = None
        for record in event_log_records:
            entry = record.event_log_entry
            if entry.is_dagster_event:
                try:
                    pipeline_name = entry.job_name
                    pipeline_run_id = entry.run_id
                    timestamp = entry.timestamp
                    dagster_event = entry.get_dagster_event()
                    dagster_event_type = dagster_event.event_type
                    step_key = dagster_event.step_key

                    running_pipeline = running_pipelines.get(pipeline_run_id)
                    repository_name = (
                        running_pipeline.repository_name
                        if running_pipeline
                        else get_repository_name(context.instance, pipeline_run_id)
                    )

                    if (
                        include_asset_events
                        and dagster_event_type in _ASSET_EVENT_TYPES
                    ):
                        _handle_asset_event(
                            running_pipelines,
                            dagster_event_type,
                            pipeline_run_id,
                            timestamp,
                            entry,
                        )
                    elif dagster_event_type in PIPELINE_EVENTS:
                        # Drain synthesis BEFORE the pipeline handler so the
                        # planned-asset set is still reachable; the handler
                        # pops the run entry on termination.
                        if (
                            include_asset_events
                            and dagster_event_type in _RUN_TERMINATION_TYPES
                        ):
                            _drain_planned_as_failures(
                                running_pipelines,
                                pipeline_run_id,
                                timestamp,
                                terminal=False,
                            )
                        _handle_pipeline_event(
                            running_pipelines,
                            dagster_event_type,
                            pipeline_name,
                            pipeline_run_id,
                            timestamp,
                            repository_name,
                        )
                    elif dagster_event_type in STEP_EVENTS:
                        _handle_step_event(
                            running_pipelines,
                            dagster_event_type,
                            pipeline_name,
                            pipeline_run_id,
                            timestamp,
                            step_key,
                            repository_name,
                        )
                except Exception as e:
                    raised_exception = e
                    break
            last_storage_id = record.storage_id

        _update_cursor(context, last_storage_id, running_pipelines)

        if not raised_exception:
            msg = f"Last cursor: {context.cursor}"
        else:
            msg = (
                f"Sensor run failed with error: {raised_exception}. "
                f"Last cursor: {context.cursor}"
            )
        log.info(msg)
        yield SkipReason(msg)

    return _openlineage_sensor


def _handle_pipeline_event(
    running_pipelines: Dict[str, RunningPipeline],
    dagster_event_type: DagsterEventType,
    pipeline_name: str,
    pipeline_run_id: str,
    timestamp: float,
    repository_name: Optional[str],
):
    if dagster_event_type == DagsterEventType.RUN_START:
        _ADAPTER.start_pipeline(
            pipeline_name, pipeline_run_id, timestamp, repository_name
        )
        running_pipelines.setdefault(
            pipeline_run_id, RunningPipeline(repository_name=repository_name)
        )
    elif dagster_event_type == DagsterEventType.RUN_SUCCESS:
        _ADAPTER.complete_pipeline(
            pipeline_name, pipeline_run_id, timestamp, repository_name
        )
        running_pipelines.pop(pipeline_run_id, None)
    elif dagster_event_type == DagsterEventType.RUN_FAILURE:
        _ADAPTER.fail_pipeline(
            pipeline_name, pipeline_run_id, timestamp, repository_name
        )
        running_pipelines.pop(pipeline_run_id, None)
    elif dagster_event_type == DagsterEventType.RUN_CANCELED:
        _ADAPTER.cancel_pipeline(
            pipeline_name, pipeline_run_id, timestamp, repository_name
        )
        running_pipelines.pop(pipeline_run_id, None)


def _handle_step_event(
    running_pipelines: Dict[str, RunningPipeline],
    dagster_event_type: DagsterEventType,
    pipeline_name: str,
    pipeline_run_id: str,
    timestamp: float,
    step_key: str,
    repository_name: Optional[str],
):
    running_pipeline = running_pipelines.get(
        pipeline_run_id, RunningPipeline(repository_name=repository_name)
    )
    running_steps = running_pipeline.running_steps
    running_step = running_steps.get(step_key, RunningStep(make_step_run_id()))
    step_run_id = running_step.step_run_id

    if dagster_event_type == DagsterEventType.STEP_START:
        _ADAPTER.start_step(
            pipeline_name,
            pipeline_run_id,
            timestamp,
            step_run_id,
            step_key,
            repository_name,
        )
        running_steps[step_key] = running_step
    elif dagster_event_type == DagsterEventType.STEP_SUCCESS:
        _ADAPTER.complete_step(
            pipeline_name,
            pipeline_run_id,
            timestamp,
            step_run_id,
            step_key,
            repository_name,
        )
        running_steps.pop(step_key, None)
    elif dagster_event_type == DagsterEventType.STEP_FAILURE:
        _ADAPTER.fail_step(
            pipeline_name,
            pipeline_run_id,
            timestamp,
            step_run_id,
            step_key,
            repository_name,
        )
        running_steps.pop(step_key, None)
    running_pipelines[pipeline_run_id] = running_pipeline


def _handle_asset_event(
    running_pipelines: Dict[str, RunningPipeline],
    dagster_event_type: DagsterEventType,
    pipeline_run_id: str,
    timestamp: float,
    entry: Any,
):
    dagster_event = entry.get_dagster_event()
    data = dagster_event.event_specific_data
    running_pipeline = running_pipelines.setdefault(pipeline_run_id, RunningPipeline())
    planned_paths: Set[Tuple[str, ...]] = running_pipeline.planned_asset_paths

    if dagster_event_type == DagsterEventType.ASSET_MATERIALIZATION_PLANNED:
        asset_key = data.asset_key
        planned_paths.add(tuple(asset_key.path))
        _ADAPTER.asset_materialization_planned(asset_key, pipeline_run_id, timestamp)

    elif dagster_event_type == DagsterEventType.ASSET_MATERIALIZATION:
        mat = data.materialization
        asset_key = mat.asset_key
        planned_paths.discard(tuple(asset_key.path))
        _ADAPTER.asset_materialization(
            asset_key,
            pipeline_run_id,
            timestamp,
            metadata=mat.metadata,
            partition_key=mat.partition,
        )

    elif dagster_event_type == DagsterEventType.ASSET_FAILED_TO_MATERIALIZE:
        asset_key = data.asset_key
        planned_paths.discard(tuple(asset_key.path))
        _ADAPTER.asset_failed_to_materialize(
            asset_key,
            pipeline_run_id,
            timestamp,
            partition_key=data.partition,
        )

    elif dagster_event_type == DagsterEventType.ASSET_OBSERVATION:
        obs = data.asset_observation
        _ADAPTER.asset_observation(
            obs.asset_key,
            pipeline_run_id,
            timestamp,
            metadata=obs.metadata,
            partition_key=obs.partition,
        )

    elif dagster_event_type == DagsterEventType.ASSET_CHECK_EVALUATION_PLANNED:
        _ADAPTER.asset_check_evaluation_planned(
            data.asset_key,
            check_name=data.check_name,
            run_id=pipeline_run_id,
            timestamp=timestamp,
            standalone=True,
        )

    elif dagster_event_type == DagsterEventType.ASSET_CHECK_EVALUATION:
        _ADAPTER.asset_check_evaluation(
            data.asset_key,
            [data],
            run_id=pipeline_run_id,
            timestamp=timestamp,
        )


def _drain_planned_as_failures(
    running_pipelines: Dict[str, RunningPipeline],
    pipeline_run_id: str,
    timestamp: float,
    *,
    terminal: bool,
) -> None:
    running_pipeline = running_pipelines.get(pipeline_run_id)
    if running_pipeline is None:
        return
    remaining = running_pipeline.planned_asset_paths
    for path in list(remaining):
        _ADAPTER.asset_failed_to_materialize(
            AssetKey(list(path)),
            pipeline_run_id,
            timestamp,
            error_message="asset planned for run, run ended before materialization",
        )
    running_pipeline.planned_asset_paths = set()
    if terminal:
        running_pipelines.pop(pipeline_run_id, None)


def _update_cursor(
    context: SensorEvaluationContext,
    last_storage_id: int,
    running_pipelines: Dict[str, RunningPipeline],
):
    context.update_cursor(
        OpenLineageCursor(
            last_storage_id=last_storage_id, running_pipelines=running_pipelines
        ).to_json()
    )
