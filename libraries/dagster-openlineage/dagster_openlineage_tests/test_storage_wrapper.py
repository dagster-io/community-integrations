# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

# pyright: reportOptionalSubscript=false, reportOperatorIssue=false, reportAbstractUsage=false

import threading
import time
from unittest.mock import MagicMock

import pytest
from dagster import AssetKey
from dagster._core.storage.event_log import InMemoryEventLogStorage
from dagster._core.storage.event_log.base import EventLogStorage
from openlineage.client.event_v2 import RunEvent, RunState
from openlineage.client.uuid import generate_new_uuid

from dagster_openlineage.adapter import OpenLineageAdapter
from dagster_openlineage.emitter import OpenLineageEmitter
from dagster_openlineage.storage import OpenLineageEventLogStorage


def _rid() -> str:
    return str(generate_new_uuid())


def _make_wrapper_with_capture(
    inner: EventLogStorage | None = None,
) -> tuple[OpenLineageEventLogStorage, MagicMock]:
    client = MagicMock()
    adapter = OpenLineageAdapter(emitter=OpenLineageEmitter(client=client))
    wrapper = OpenLineageEventLogStorage(
        wrapped=inner or InMemoryEventLogStorage(), adapter=adapter
    )
    return wrapper, client


def _planned_event(run_id: str, asset_key: AssetKey):
    from dagster import DagsterEvent, DagsterEventType, EventLogEntry
    from dagster._core.events import AssetMaterializationPlannedData

    return EventLogEntry(
        error_info=None,
        level="debug",
        user_message="",
        run_id=run_id,
        timestamp=time.time(),
        step_key=None,
        job_name="a_job",
        dagster_event=DagsterEvent(
            event_type_value=DagsterEventType.ASSET_MATERIALIZATION_PLANNED.value,
            job_name="a_job",
            event_specific_data=AssetMaterializationPlannedData(asset_key=asset_key),
        ),
    )


def _materialization_event(run_id: str, asset_key: AssetKey, partition=None):
    from dagster import (
        AssetMaterialization,
        DagsterEvent,
        DagsterEventType,
        EventLogEntry,
    )
    from dagster._core.events import StepMaterializationData

    mat = AssetMaterialization(asset_key=asset_key, partition=partition)
    return EventLogEntry(
        error_info=None,
        level="debug",
        user_message="",
        run_id=run_id,
        timestamp=time.time(),
        step_key="an_op",
        job_name="a_job",
        dagster_event=DagsterEvent(
            event_type_value=DagsterEventType.ASSET_MATERIALIZATION.value,
            job_name="a_job",
            step_key="an_op",
            event_specific_data=StepMaterializationData(materialization=mat),
        ),
    )


def _step_failure_event(run_id: str):
    from dagster import DagsterEvent, DagsterEventType, EventLogEntry
    from dagster._core.execution.plan.objects import StepFailureData

    return EventLogEntry(
        error_info=None,
        level="error",
        user_message="",
        run_id=run_id,
        timestamp=time.time(),
        step_key="an_op",
        job_name="a_job",
        dagster_event=DagsterEvent(
            event_type_value=DagsterEventType.STEP_FAILURE.value,
            job_name="a_job",
            step_key="an_op",
            event_specific_data=StepFailureData(error=None, user_failure_data=None),
        ),
    )


def test_construction_wraps_arbitrary_event_log_storage():
    inner = InMemoryEventLogStorage()
    wrapper = OpenLineageEventLogStorage(wrapped=inner)
    assert wrapper._wrapped is inner
    assert wrapper.__class__.__abstractmethods__ == frozenset()


def test_from_config_value_rehydrates_inner_storage():
    wrapper = OpenLineageEventLogStorage.from_config_value(
        inst_data=None,
        config_value={
            "wrapped": {
                "module": "dagster._core.storage.event_log.in_memory",
                "class": "InMemoryEventLogStorage",
                "config": {},
            },
            "namespace": "multi_tenant",
        },
    )
    assert isinstance(wrapper._wrapped, InMemoryEventLogStorage)
    # adapter should have picked up the namespace.
    assert wrapper._adapter._namespace == "multi_tenant"


def test_inst_data_preserved_through_round_trip():
    from dagster._serdes import ConfigurableClassData

    inst_data = ConfigurableClassData(
        module_name="dagster_openlineage.storage",
        class_name="OpenLineageEventLogStorage",
        config_yaml="",
    )
    wrapper = OpenLineageEventLogStorage(
        wrapped=InMemoryEventLogStorage(), inst_data=inst_data
    )
    # Auto-delegation must NOT shadow inst_data with a delegate pointing at
    # the wrapped storage's inst_data (which would be None).
    assert wrapper.inst_data is inst_data


def test_auto_delegation_resolves_every_abstract_method():
    wrapper, _ = _make_wrapper_with_capture()
    for name in EventLogStorage.__abstractmethods__:
        assert hasattr(wrapper, name), f"missing {name}"


def test_is_persistent_property_delegates_to_wrapped():
    wrapper, _ = _make_wrapper_with_capture()
    # InMemoryEventLogStorage.is_persistent is False.
    assert wrapper.is_persistent is False


def test_store_event_calls_wrapped_then_emits_ol():
    wrapper, client = _make_wrapper_with_capture()
    run_id = _rid()
    wrapper.store_event(_materialization_event(run_id, AssetKey(["orders"])))
    # Inner storage saw the event.
    records = wrapper._wrapped.get_records_for_run(run_id).records
    assert len(records) == 1
    # OL emitted a RunEvent(COMPLETE) for the asset.
    assert any(
        isinstance(c.args[0], RunEvent) and c.args[0].eventType == RunState.COMPLETE
        for c in client.emit.call_args_list
    )


def test_store_event_batch_loops_per_event():
    wrapper, client = _make_wrapper_with_capture()
    run_id = _rid()
    events = [
        _planned_event(run_id, AssetKey(["orders"])),
        _materialization_event(run_id, AssetKey(["orders"])),
    ]
    wrapper.store_event_batch(events)
    # One OL emit per asset event (PLANNED + MATERIALIZATION).
    assert client.emit.call_count == 2


def test_wrapped_store_event_raise_leaves_no_emit():
    inner = MagicMock(spec=EventLogStorage)
    inner.store_event.side_effect = RuntimeError("disk full")
    wrapper, client = _make_wrapper_with_capture(inner=inner)
    with pytest.raises(RuntimeError):
        wrapper.store_event(_materialization_event(_rid(), AssetKey(["orders"])))
    client.emit.assert_not_called()


def test_store_event_swallows_ol_emission_failure():
    client = MagicMock()
    client.emit.side_effect = ConnectionError("backend down")
    adapter = OpenLineageAdapter(emitter=OpenLineageEmitter(client=client))
    wrapper = OpenLineageEventLogStorage(
        wrapped=InMemoryEventLogStorage(), adapter=adapter
    )
    # Storage write succeeded; emitter swallows — no exception escapes.
    wrapper.store_event(_materialization_event(_rid(), AssetKey(["orders"])))


def test_store_event_propagates_base_exception_from_emit():
    client = MagicMock()
    client.emit.side_effect = SystemExit("shutdown")
    adapter = OpenLineageAdapter(emitter=OpenLineageEmitter(client=client))
    wrapper = OpenLineageEventLogStorage(
        wrapped=InMemoryEventLogStorage(), adapter=adapter
    )
    with pytest.raises(SystemExit):
        wrapper.store_event(_materialization_event(_rid(), AssetKey(["orders"])))


def test_failure_synthesis_on_step_failure():
    wrapper, client = _make_wrapper_with_capture()
    run_id = _rid()
    wrapper.store_event(_planned_event(run_id, AssetKey(["orders"])))
    wrapper.store_event(_step_failure_event(run_id))
    fail_events = [
        c.args[0]
        for c in client.emit.call_args_list
        if isinstance(c.args[0], RunEvent) and c.args[0].eventType == RunState.FAIL
    ]
    assert len(fail_events) == 1
    assert fail_events[0].job.name == "orders"


def test_no_synthesis_when_materialization_follows_planned():
    wrapper, client = _make_wrapper_with_capture()
    run_id = _rid()
    wrapper.store_event(_planned_event(run_id, AssetKey(["orders"])))
    wrapper.store_event(_materialization_event(run_id, AssetKey(["orders"])))
    wrapper.store_event(_step_failure_event(run_id))
    fail_events = [
        c.args[0]
        for c in client.emit.call_args_list
        if isinstance(c.args[0], RunEvent) and c.args[0].eventType == RunState.FAIL
    ]
    assert fail_events == []


def test_synthesis_per_asset_when_only_some_materialized():
    wrapper, client = _make_wrapper_with_capture()
    run_id = _rid()
    wrapper.store_event(_planned_event(run_id, AssetKey(["a"])))
    wrapper.store_event(_planned_event(run_id, AssetKey(["b"])))
    wrapper.store_event(_materialization_event(run_id, AssetKey(["a"])))
    wrapper.store_event(_step_failure_event(run_id))
    failed_jobs = {
        c.args[0].job.name
        for c in client.emit.call_args_list
        if isinstance(c.args[0], RunEvent) and c.args[0].eventType == RunState.FAIL
    }
    assert failed_jobs == {"b"}


def test_synthesis_lock_safe_under_concurrent_store_event():
    # Use a mock wrapped storage — we are testing the synthesis-state lock,
    # not the concurrency behavior of InMemoryEventLogStorage (which is
    # sqlite-backed and will surface "database table is locked" under
    # multi-threaded writes).
    inner = MagicMock(spec=EventLogStorage)
    wrapper, client = _make_wrapper_with_capture(inner=inner)
    run_id = _rid()
    # Alternating PLANNED + MATERIALIZATION across 4 threads. Invariant: no
    # KeyError, no exceptions, and planned_assets collapses to empty for the
    # run once every planned key has a matching materialization.
    asset_count = 25
    planned = [_planned_event(run_id, AssetKey([f"a{i}"])) for i in range(asset_count)]
    materialized = [
        _materialization_event(run_id, AssetKey([f"a{i}"])) for i in range(asset_count)
    ]

    def _run(events):
        for ev in events:
            wrapper.store_event(ev)

    threads = [
        threading.Thread(target=_run, args=(planned[:12],)),
        threading.Thread(target=_run, args=(planned[12:],)),
        threading.Thread(target=_run, args=(materialized[:12],)),
        threading.Thread(target=_run, args=(materialized[12:],)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert wrapper._planned_assets.get(run_id, set()) == set()
    assert client.emit.call_count == asset_count * 2
