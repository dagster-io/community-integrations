# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

# pyright: reportAbstractUsage=false
# OpenLineageEventLogStorage.__abstractmethods__ is cleared at runtime by the
# setattr delegation loop in storage.py; pyright cannot see dynamic assignments
# to __abstractmethods__ and reports every direct instantiation as abstract.
# test_e2e_pipes.py carries the same suppression for the same reason.

"""End-to-end tests for v0.2 asset-centric OpenLineage emission.

Exercises the full stack — real OpenLineageAdapter, real facet builders,
real OpenLineageEmitter — with FakeOpenLineageTransport capturing emitted
events in memory instead of shipping them. No mock.patch on the adapter.

Structure:
  Mechanism A (direct store_event) — verify OL output for each asset event
  type via OpenLineageEventLogStorage.store_event directly.

  Mechanism A (real execution) — verify OL emission when a real Dagster
  asset is materialized via materialize() with a wrapped DagsterInstance.

  Mechanism B (sensor) — verify the sensor with include_asset_events=True
  emits OL events from a real event log populated by materialize().
"""

import tempfile
import time
from unittest.mock import patch

from dagster import (
    AssetKey,
    AssetMaterialization,
    AssetObservation,
    DagsterEvent,
    DagsterEventType,
    EventLogEntry,
    TableColumn,
    TableColumnDep,
    TableColumnLineage,
    TableSchema,
    asset,
    build_sensor_context,
    materialize,
)
from dagster._core.events import (
    AssetMaterializationPlannedData,
    AssetObservationData,
    StepMaterializationData,
)
from dagster._core.execution.plan.objects import StepFailureData
from dagster._core.instance import DagsterInstance
from dagster._core.storage.event_log import InMemoryEventLogStorage
from dagster.core.test_utils import instance_for_test
from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import DatasetEvent, RunEvent, RunState
from openlineage.client.generated.schema_dataset import SchemaDatasetFacet
from openlineage.client.uuid import generate_new_uuid

from dagster_openlineage.adapter import OpenLineageAdapter
from dagster_openlineage.emitter import OpenLineageEmitter
from dagster_openlineage.sensor import openlineage_sensor
from dagster_openlineage.storage import OpenLineageEventLogStorage

from .fakes import FakeOpenLineageTransport


def _rid() -> str:
    return str(generate_new_uuid())


def _make_wrapper(
    transport: FakeOpenLineageTransport,
    inner: InMemoryEventLogStorage | None = None,
) -> OpenLineageEventLogStorage:
    client = OpenLineageClient(transport=transport)
    adapter = OpenLineageAdapter(emitter=OpenLineageEmitter(client=client))
    return OpenLineageEventLogStorage(
        wrapped=inner or InMemoryEventLogStorage(), adapter=adapter
    )


def _planned_entry(
    run_id: str, asset_key: AssetKey, job_name: str = "a_job"
) -> EventLogEntry:
    return EventLogEntry(
        error_info=None,
        level="debug",
        user_message="",
        run_id=run_id,
        timestamp=time.time(),
        step_key=None,
        job_name=job_name,
        dagster_event=DagsterEvent(
            event_type_value=DagsterEventType.ASSET_MATERIALIZATION_PLANNED.value,
            job_name=job_name,
            event_specific_data=AssetMaterializationPlannedData(asset_key=asset_key),
        ),
    )


def _materialization_entry(
    run_id: str,
    asset_key: AssetKey,
    job_name: str = "a_job",
    metadata: dict | None = None,
    partition: str | None = None,
) -> EventLogEntry:
    mat = AssetMaterialization(
        asset_key=asset_key,
        metadata=metadata or {},
        partition=partition,
    )
    return EventLogEntry(
        error_info=None,
        level="debug",
        user_message="",
        run_id=run_id,
        timestamp=time.time(),
        step_key="an_op",
        job_name=job_name,
        dagster_event=DagsterEvent(
            event_type_value=DagsterEventType.ASSET_MATERIALIZATION.value,
            job_name=job_name,
            step_key="an_op",
            event_specific_data=StepMaterializationData(materialization=mat),
        ),
    )


def _step_failure_entry(run_id: str, job_name: str = "a_job") -> EventLogEntry:
    return EventLogEntry(
        error_info=None,
        level="error",
        user_message="",
        run_id=run_id,
        timestamp=time.time(),
        step_key="an_op",
        job_name=job_name,
        dagster_event=DagsterEvent(
            event_type_value=DagsterEventType.STEP_FAILURE.value,
            job_name=job_name,
            step_key="an_op",
            event_specific_data=StepFailureData(error=None, user_failure_data=None),
        ),
    )


def _observation_entry(run_id: str, asset_key: AssetKey) -> EventLogEntry:
    obs = AssetObservation(asset_key=asset_key)
    return EventLogEntry(
        error_info=None,
        level="debug",
        user_message="",
        run_id=run_id,
        timestamp=time.time(),
        step_key="an_op",
        job_name="a_job",
        dagster_event=DagsterEvent(
            event_type_value=DagsterEventType.ASSET_OBSERVATION.value,
            job_name="a_job",
            step_key="an_op",
            event_specific_data=AssetObservationData(asset_observation=obs),
        ),
    )


# ---------------------------------------------------------------------------
# Mechanism A — direct store_event tests
# ---------------------------------------------------------------------------


def test_planned_then_complete_emits_start_complete_pair():
    """ASSET_MATERIALIZATION_PLANNED → START, ASSET_MATERIALIZATION → COMPLETE."""
    transport = FakeOpenLineageTransport()
    wrapper = _make_wrapper(transport)
    run_id = _rid()
    wrapper.store_event(_planned_entry(run_id, AssetKey(["orders"])))
    wrapper.store_event(_materialization_entry(run_id, AssetKey(["orders"])))
    run_events = [e for e in transport.events if isinstance(e, RunEvent)]
    assert [e.eventType for e in run_events] == [RunState.START, RunState.COMPLETE]
    assert all(e.job.name == "orders" for e in run_events)
    assert all(
        e.outputs is not None and e.outputs[0].name == "orders" for e in run_events
    )


def test_materialization_schema_facet_on_output():
    """TableSchema metadata produces a ``schema`` facet on the output dataset."""
    transport = FakeOpenLineageTransport()
    wrapper = _make_wrapper(transport)
    schema = TableSchema(
        columns=[TableColumn("id", "int"), TableColumn("name", "string")]
    )
    wrapper.store_event(
        _materialization_entry(
            _rid(), AssetKey(["orders"]), metadata={"dagster/column_schema": schema}
        )
    )
    event: RunEvent = transport.run_events_of_type(RunState.COMPLETE)[0]
    assert event.outputs is not None
    output_facets = event.outputs[0].facets
    assert output_facets is not None
    assert "schema" in output_facets
    schema_facet = output_facets["schema"]
    assert (
        isinstance(schema_facet, SchemaDatasetFacet) and schema_facet.fields is not None
    )
    field_names = [f.name for f in schema_facet.fields]
    assert field_names == ["id", "name"]


def test_materialization_nominal_time_from_partition():
    """Date-shaped partition_key resolves to a ``nominalTime`` run facet."""
    transport = FakeOpenLineageTransport()
    wrapper = _make_wrapper(transport)
    wrapper.store_event(
        _materialization_entry(_rid(), AssetKey(["orders"]), partition="2026-04-23")
    )
    event: RunEvent = transport.run_events_of_type(RunState.COMPLETE)[0]
    assert event.run.facets is not None
    assert "nominalTime" in event.run.facets


def test_materialization_column_lineage_facet():
    """TableColumnLineage metadata produces a ``columnLineage`` facet on the output dataset."""
    transport = FakeOpenLineageTransport()
    wrapper = _make_wrapper(transport)
    lineage = TableColumnLineage(
        deps_by_column={
            "out": [TableColumnDep(asset_key=AssetKey("raw"), column_name="src")]
        }
    )
    wrapper.store_event(
        _materialization_entry(
            _rid(), AssetKey(["orders"]), metadata={"dagster/column_lineage": lineage}
        )
    )
    event: RunEvent = transport.run_events_of_type(RunState.COMPLETE)[0]
    assert event.outputs is not None
    output_facets = event.outputs[0].facets
    assert output_facets is not None
    assert "columnLineage" in output_facets


def test_failure_synthesis_emits_fail_for_unmaterilaized_asset():
    """Planned asset that never materializes gets a synthesized FAIL when step fails."""
    transport = FakeOpenLineageTransport()
    wrapper = _make_wrapper(transport)
    run_id = _rid()
    wrapper.store_event(_planned_entry(run_id, AssetKey(["orders"])))
    wrapper.store_event(_step_failure_entry(run_id))
    fails = transport.run_events_of_type(RunState.FAIL)
    assert len(fails) == 1
    assert fails[0].job.name == "orders"
    assert fails[0].run.facets is not None
    assert "errorMessage" in fails[0].run.facets


def test_failure_synthesis_only_targets_unmaterilaized_assets():
    """Asset A materializes before step failure; only asset B gets a synthesized FAIL."""
    transport = FakeOpenLineageTransport()
    wrapper = _make_wrapper(transport)
    run_id = _rid()
    wrapper.store_event(_planned_entry(run_id, AssetKey(["a"])))
    wrapper.store_event(_planned_entry(run_id, AssetKey(["b"])))
    wrapper.store_event(_materialization_entry(run_id, AssetKey(["a"])))
    wrapper.store_event(_step_failure_entry(run_id))
    fails = transport.run_events_of_type(RunState.FAIL)
    assert len(fails) == 1
    assert fails[0].job.name == "b"


def test_no_failure_synthesis_when_all_assets_materialized():
    """Planned + materialized before step failure → no synthesized FAIL events."""
    transport = FakeOpenLineageTransport()
    wrapper = _make_wrapper(transport)
    run_id = _rid()
    wrapper.store_event(_planned_entry(run_id, AssetKey(["orders"])))
    wrapper.store_event(_materialization_entry(run_id, AssetKey(["orders"])))
    wrapper.store_event(_step_failure_entry(run_id))
    assert transport.run_events_of_type(RunState.FAIL) == []


def test_observation_emits_dataset_event():
    """ASSET_OBSERVATION emits a DatasetEvent, not a RunEvent."""
    transport = FakeOpenLineageTransport()
    wrapper = _make_wrapper(transport)
    wrapper.store_event(_observation_entry(_rid(), AssetKey(["orders"])))
    dataset_events = [e for e in transport.events if isinstance(e, DatasetEvent)]
    run_events = [e for e in transport.events if isinstance(e, RunEvent)]
    assert len(dataset_events) == 1
    assert dataset_events[0].dataset.name == "orders"
    assert run_events == []


def test_datasource_facet_from_path_metadata():
    """``path`` metadata key produces a ``dataSource`` facet on the output dataset."""
    transport = FakeOpenLineageTransport()
    wrapper = _make_wrapper(transport)
    wrapper.store_event(
        _materialization_entry(
            _rid(), AssetKey(["orders"]), metadata={"path": "/data/orders.parquet"}
        )
    )
    event: RunEvent = transport.run_events_of_type(RunState.COMPLETE)[0]
    assert event.outputs is not None
    output_facets = event.outputs[0].facets
    assert output_facets is not None
    assert "dataSource" in output_facets


def test_multiple_runs_tracked_independently():
    """Events for different run_ids do not cross-contaminate synthesis state."""
    transport = FakeOpenLineageTransport()
    wrapper = _make_wrapper(transport)
    run_a, run_b = _rid(), _rid()
    wrapper.store_event(_planned_entry(run_a, AssetKey(["a"])))
    wrapper.store_event(_planned_entry(run_b, AssetKey(["b"])))
    # Only run_a fails; run_b's planned asset must not be synthesized as FAIL.
    wrapper.store_event(_step_failure_entry(run_a))
    fails = transport.run_events_of_type(RunState.FAIL)
    assert len(fails) == 1
    assert fails[0].job.name == "a"


# ---------------------------------------------------------------------------
# Mechanism A — real Dagster execution via materialize()
# ---------------------------------------------------------------------------


@asset
def _e2e_orders():
    pass


def test_real_asset_materialize_emits_start_and_complete():
    """Full materialize() flow with a wrapped DagsterInstance produces START+COMPLETE."""
    transport = FakeOpenLineageTransport()
    instance = DagsterInstance.ephemeral()
    original_storage = instance.event_log_storage
    client = OpenLineageClient(transport=transport)
    adapter = OpenLineageAdapter(emitter=OpenLineageEmitter(client=client))
    wrapper = OpenLineageEventLogStorage(wrapped=original_storage, adapter=adapter)
    wrapper.register_instance(instance)
    instance._event_storage = wrapper

    result = materialize([_e2e_orders], instance=instance)
    assert result.success

    asset_starts = [
        e
        for e in transport.run_events_of_type(RunState.START)
        if e.job.name == "_e2e_orders"
    ]
    asset_completes = [
        e
        for e in transport.run_events_of_type(RunState.COMPLETE)
        if e.job.name == "_e2e_orders"
    ]
    assert len(asset_starts) == 1
    assert len(asset_completes) == 1
    assert asset_completes[0].outputs is not None
    assert asset_completes[0].outputs[0].name == "_e2e_orders"


# ---------------------------------------------------------------------------
# Mechanism B — sensor with real event log populated by materialize()
# ---------------------------------------------------------------------------


@asset
def _e2e_sensor_orders():
    pass


def test_sensor_emits_asset_ol_events_from_real_execution():
    """Sensor with include_asset_events=True emits OL events from a real event log."""
    transport = FakeOpenLineageTransport()
    client = OpenLineageClient(transport=transport)
    adapter = OpenLineageAdapter(emitter=OpenLineageEmitter(client=client))

    with tempfile.TemporaryDirectory() as temp_dir:
        with instance_for_test(
            temp_dir=temp_dir,
            overrides={
                "event_log_storage": {
                    "module": "dagster.core.storage.event_log",
                    "class": "ConsolidatedSqliteEventLogStorage",
                    "config": {"base_dir": temp_dir},
                },
            },
        ) as instance:
            result = materialize([_e2e_sensor_orders], instance=instance)
            assert result.success

            sensor = openlineage_sensor(include_asset_events=True)
            context = build_sensor_context(instance=instance)
            with patch("dagster_openlineage.sensor._ADAPTER", adapter):
                prev_cursor = None
                while True:
                    sensor.evaluate_tick(context)
                    if context.cursor == prev_cursor:
                        break
                    prev_cursor = context.cursor

            asset_starts = [
                e
                for e in transport.run_events_of_type(RunState.START)
                if e.job.name == "_e2e_sensor_orders"
            ]
            asset_completes = [
                e
                for e in transport.run_events_of_type(RunState.COMPLETE)
                if e.job.name == "_e2e_sensor_orders"
            ]
            assert len(asset_starts) == 1
            assert len(asset_completes) == 1
