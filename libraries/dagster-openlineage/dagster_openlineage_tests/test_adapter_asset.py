# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

# pyright: reportOptionalSubscript=false, reportOperatorIssue=false, reportOptionalIterable=false

import time
from unittest.mock import MagicMock

from dagster import (
    AssetCheckEvaluation,
    AssetCheckSeverity,
    AssetKey,
    AssetMaterialization,
    TableColumn,
    TableColumnDep,
    TableColumnLineage,
    TableSchema,
)
from openlineage.client.event_v2 import DatasetEvent, RunEvent, RunState
from openlineage.client.uuid import generate_new_uuid

from dagster_openlineage.adapter import OpenLineageAdapter
from dagster_openlineage.emitter import OpenLineageEmitter


def _rid() -> str:
    return str(generate_new_uuid())


def _make_adapter(**kwargs) -> tuple[OpenLineageAdapter, MagicMock]:
    client = MagicMock()
    adapter = OpenLineageAdapter(emitter=OpenLineageEmitter(client=client), **kwargs)
    return adapter, client


def test_asset_materialization_planned_emits_run_start():
    adapter, client = _make_adapter()
    adapter.asset_materialization_planned(
        AssetKey(["orders"]), run_id=_rid(), timestamp=time.time()
    )
    client.emit.assert_called_once()
    event = client.emit.call_args.args[0]
    assert isinstance(event, RunEvent)
    assert event.eventType == RunState.START
    assert event.job.name == "orders"
    assert len(event.outputs) == 1
    assert event.outputs[0].name == "orders"


def test_asset_materialization_emits_schema_facet():
    adapter, client = _make_adapter()
    mat = AssetMaterialization(
        asset_key=AssetKey(["orders"]),
        metadata={
            "dagster/column_schema": TableSchema(columns=[TableColumn("x", "int")]),
        },
    )
    adapter.asset_materialization(
        AssetKey(["orders"]),
        run_id=_rid(),
        timestamp=time.time(),
        metadata=mat.metadata,
    )
    event: RunEvent = client.emit.call_args.args[0]
    assert event.eventType == RunState.COMPLETE
    assert event.job.name == "orders"
    output = event.outputs[0]
    assert "schema" in output.facets


def test_asset_materialization_emits_column_lineage_facet():
    adapter, client = _make_adapter()
    mat = AssetMaterialization(
        asset_key=AssetKey(["orders"]),
        metadata={
            "dagster/column_lineage": TableColumnLineage(
                deps_by_column={
                    "out": [
                        TableColumnDep(asset_key=AssetKey("raw"), column_name="src")
                    ]
                }
            ),
        },
    )
    adapter.asset_materialization(
        AssetKey(["orders"]),
        run_id=_rid(),
        timestamp=time.time(),
        metadata=mat.metadata,
    )
    event: RunEvent = client.emit.call_args.args[0]
    output = event.outputs[0]
    assert "columnLineage" in output.facets


def test_asset_materialization_emits_datasource_facet_from_path():
    adapter, client = _make_adapter()
    mat = AssetMaterialization(
        asset_key=AssetKey(["orders"]),
        metadata={"path": "/data/orders.parquet"},
    )
    adapter.asset_materialization(
        AssetKey(["orders"]),
        run_id=_rid(),
        timestamp=time.time(),
        metadata=mat.metadata,
    )
    event: RunEvent = client.emit.call_args.args[0]
    assert "dataSource" in event.outputs[0].facets


def test_asset_materialization_emits_nominal_time_from_partition():
    adapter, client = _make_adapter()
    adapter.asset_materialization(
        AssetKey(["orders"]),
        run_id=_rid(),
        timestamp=time.time(),
        partition_key="2026-04-23",
    )
    event: RunEvent = client.emit.call_args.args[0]
    assert "nominalTime" in event.run.facets


def test_asset_materialization_attaches_upstream_input_datasets():
    adapter, client = _make_adapter()
    adapter.asset_materialization(
        AssetKey(["orders"]),
        run_id=_rid(),
        timestamp=time.time(),
        upstream_asset_keys=[AssetKey(["raw"]), AssetKey(["ref"])],
    )
    event: RunEvent = client.emit.call_args.args[0]
    assert [ds.name for ds in event.inputs] == ["raw", "ref"]


def test_asset_check_evaluation_places_assertions_on_input_dataset():
    adapter, client = _make_adapter()
    evals = [
        AssetCheckEvaluation(
            asset_key=AssetKey(["orders"]),
            check_name="nonempty",
            passed=True,
            severity=AssetCheckSeverity.ERROR,
        ),
        AssetCheckEvaluation(
            asset_key=AssetKey(["orders"]),
            check_name="unique_id",
            passed=False,
            severity=AssetCheckSeverity.ERROR,
        ),
    ]
    adapter.asset_check_evaluation(
        AssetKey(["orders"]),
        evals,
        run_id=_rid(),
        timestamp=time.time(),
    )
    event: RunEvent = client.emit.call_args.args[0]
    assert event.eventType == RunState.COMPLETE
    assert event.job.name == "orders__checks"
    assert len(event.inputs) == 1
    assertions_facet = event.inputs[0].inputFacets["dataQualityAssertions"]
    assert [a.assertion for a in assertions_facet.assertions] == [
        "nonempty",
        "unique_id",
    ]
    assert "dagster_asset_check" in event.run.facets


def test_asset_check_evaluation_planned_standalone_emits_start():
    adapter, client = _make_adapter()
    adapter.asset_check_evaluation_planned(
        AssetKey(["orders"]),
        check_name="nonempty",
        run_id=_rid(),
        timestamp=time.time(),
        standalone=True,
    )
    client.emit.assert_called_once()
    event: RunEvent = client.emit.call_args.args[0]
    assert event.eventType == RunState.START
    assert event.job.name == "orders__checks"
    assert event.inputs[0].name == "orders"


def test_asset_check_evaluation_planned_non_standalone_emits_nothing():
    adapter, client = _make_adapter()
    adapter.asset_check_evaluation_planned(
        AssetKey(["orders"]),
        check_name="nonempty",
        run_id=_rid(),
        timestamp=time.time(),
        standalone=False,
    )
    client.emit.assert_not_called()


def test_asset_observation_emits_dataset_event_without_nominal_time():
    adapter, client = _make_adapter()
    adapter.asset_observation(
        AssetKey(["orders"]),
        run_id=_rid(),
        timestamp=time.time(),
    )
    event = client.emit.call_args.args[0]
    assert isinstance(event, DatasetEvent)
    assert event.dataset.name == "orders"


def test_asset_failed_to_materialize_emits_error_facet():
    adapter, client = _make_adapter()
    adapter.asset_failed_to_materialize(
        AssetKey(["orders"]),
        run_id=_rid(),
        timestamp=time.time(),
        error_message="boom",
        stack_trace="Traceback...",
    )
    event: RunEvent = client.emit.call_args.args[0]
    assert event.eventType == RunState.FAIL
    assert "errorMessage" in event.run.facets
    assert event.run.facets["errorMessage"].message == "boom"


def test_adapter_method_does_not_raise_when_emit_fails():
    client = MagicMock()
    client.emit.side_effect = ConnectionError("down")
    adapter = OpenLineageAdapter(emitter=OpenLineageEmitter(client=client))
    # Should be swallowed by emitter — adapter method returns cleanly.
    adapter.asset_materialization(
        AssetKey(["orders"]), run_id=_rid(), timestamp=time.time()
    )


def test_check_job_name_does_not_collide_with_asset_named_checks():
    adapter, client = _make_adapter()
    # Asset literally named "__checks" — adapter must still produce a distinct
    # check-job name via the to_user_string separator.
    adapter.asset_check_evaluation(
        AssetKey(["__checks"]),
        [
            AssetCheckEvaluation(
                asset_key=AssetKey(["__checks"]),
                check_name="x",
                passed=True,
            )
        ],
        run_id=_rid(),
        timestamp=time.time(),
    )
    event: RunEvent = client.emit.call_args.args[0]
    # Output job name is {asset}__checks. Collision is structural (suffix) but
    # the job name still differs from the bare asset.
    assert event.job.name == "__checks__checks"
