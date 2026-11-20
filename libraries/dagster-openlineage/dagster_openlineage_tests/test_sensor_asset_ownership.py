# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

# ty: ignore
# OpenLineage facet values are a dynamic union; ty cannot see `.owners` on the
# resolved ownership facet, and Job.facets is Optional. Asserted at runtime instead.

"""Mechanism B (sensor): per-asset ownership, exclusion, and emission-scope.

Exercises the sensor against a real event log (populated by materialize()) with
FakeOpenLineageTransport capturing emitted events. Covers the behaviour the
storage wrapper cannot provide — reading each asset's native Dagster ``owners``
from the asset graph — plus exclusion and asset-only emission parity.
"""

import tempfile
from unittest.mock import patch

from dagster import (
    AssetKey,
    DefaultSensorStatus,
    Definitions,
    asset,
    build_sensor_context,
    materialize,
)
from dagster._core.test_utils import instance_for_test
from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import RunState

from dagster_openlineage.adapter import OpenLineageAdapter
from dagster_openlineage.emitter import OpenLineageEmitter
from dagster_openlineage.sensor import openlineage_sensor

from .fakes import FakeOpenLineageTransport


@asset(key=AssetKey(["shop.orders"]), owners=["team:analytics", "dev@example.com"])
def _owned_orders():
    return 1


@asset(key=AssetKey(["shop.model_dbt"]))
def _dbt_like():
    return 2


def _sqlite_overrides(temp_dir: str) -> dict:
    return {
        "event_log_storage": {
            "module": "dagster._core.storage.event_log",
            "class": "ConsolidatedSqliteEventLogStorage",
            "config": {"base_dir": temp_dir},
        },
    }


def _drain(sensor, context) -> None:
    prev_cursor = None
    while True:
        sensor.evaluate_tick(context)
        if context.cursor == prev_cursor:
            break
        prev_cursor = context.cursor


def _run(assets, sensor, *, with_repository: bool):
    """Materialise ``assets`` then drain ``sensor`` over the event log; return the
    capturing transport. ``with_repository`` decides whether the sensor context
    exposes the asset graph (the source of native owners)."""
    transport = FakeOpenLineageTransport()
    adapter = OpenLineageAdapter(
        emitter=OpenLineageEmitter(client=OpenLineageClient(transport=transport))
    )
    defs = Definitions(assets=assets)
    with tempfile.TemporaryDirectory() as temp_dir:
        with instance_for_test(
            temp_dir=temp_dir, overrides=_sqlite_overrides(temp_dir)
        ) as instance:
            assert materialize(assets, instance=instance).success
            context = build_sensor_context(
                instance=instance,
                repository_def=defs.get_repository_def() if with_repository else None,
            )
            with patch("dagster_openlineage.sensor._ADAPTER", adapter):
                _drain(sensor, context)
    return transport


def _job_names(transport) -> set:
    events = transport.run_events_of_type(RunState.START) + (
        transport.run_events_of_type(RunState.COMPLETE)
    )
    return {e.job.name for e in events}


def _ownership(transport, job_name: str):
    for e in transport.run_events_of_type(RunState.COMPLETE):
        if e.job.name == job_name:
            facet = (e.job.facets or {}).get("ownership")
            return [o.name for o in facet.owners] if facet else []
    raise AssertionError(f"no COMPLETE event for job {job_name}")


def test_sensor_emits_native_asset_owners():
    sensor = openlineage_sensor(
        include_asset_events=True, emit_pipeline_step_events=False
    )
    transport = _run([_owned_orders], sensor, with_repository=True)

    assert _ownership(transport, "shop.orders") == ["team:analytics", "dev@example.com"]


def test_sensor_owners_fall_back_to_default_team_without_repository():
    # no asset graph on the context -> owners can't be resolved, so the adapter's
    # default team is used instead
    transport = FakeOpenLineageTransport()
    adapter = OpenLineageAdapter(
        team="fallback",
        emitter=OpenLineageEmitter(client=OpenLineageClient(transport=transport)),
    )
    defs = Definitions(assets=[_owned_orders])
    _ = defs
    sensor = openlineage_sensor(
        include_asset_events=True, emit_pipeline_step_events=False
    )
    with tempfile.TemporaryDirectory() as temp_dir:
        with instance_for_test(
            temp_dir=temp_dir, overrides=_sqlite_overrides(temp_dir)
        ) as instance:
            assert materialize([_owned_orders], instance=instance).success
            context = build_sensor_context(instance=instance)
            with patch("dagster_openlineage.sensor._ADAPTER", adapter):
                _drain(sensor, context)

    assert _ownership(transport, "shop.orders") == ["team:fallback"]


def test_sensor_excludes_asset_keys():
    sensor = openlineage_sensor(
        include_asset_events=True,
        exclude_asset_keys=["*dbt*"],
        emit_pipeline_step_events=False,
    )
    transport = _run([_owned_orders, _dbt_like], sensor, with_repository=True)

    names = _job_names(transport)
    assert "shop.orders" in names
    assert "shop.model_dbt" not in names


def test_sensor_asset_only_emits_no_pipeline_or_step_events():
    sensor = openlineage_sensor(
        include_asset_events=True, emit_pipeline_step_events=False
    )
    transport = _run([_owned_orders], sensor, with_repository=True)

    # only the asset job appears — no pipeline (`__ASSET_JOB…`) or step (`….<step>`) jobs
    assert _job_names(transport) == {"shop.orders"}


def test_sensor_default_emits_pipeline_and_step_events():
    sensor = openlineage_sensor(include_asset_events=True)  # emit flag defaults True
    transport = _run([_owned_orders], sensor, with_repository=True)

    names = _job_names(transport)
    assert "shop.orders" in names
    # at least one non-asset (pipeline/step) job is present in default mode
    assert any(n != "shop.orders" for n in names)


def test_default_status_passthrough():
    assert openlineage_sensor().default_status == DefaultSensorStatus.STOPPED
    assert (
        openlineage_sensor(default_status=DefaultSensorStatus.RUNNING).default_status
        == DefaultSensorStatus.RUNNING
    )
