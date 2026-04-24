# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Cursor round-trip + back-compat tests for the v0.2 asset extension."""

import json

from dagster_openlineage.cursor import (
    OpenLineageCursor,
    RunningPipeline,
    RunningStep,
)


def test_cursor_round_trip_preserves_planned_asset_paths():
    cursor = OpenLineageCursor(
        last_storage_id=42,
        running_pipelines={
            "run-1": RunningPipeline(
                planned_asset_paths={("orders", "daily"), ("customers",)}
            )
        },
    )
    restored = OpenLineageCursor.from_json(cursor.to_json())
    assert restored.running_pipelines["run-1"].planned_asset_paths == {
        ("orders", "daily"),
        ("customers",),
    }


def test_cursor_round_trip_preserves_tuple_types():
    """Probe 26: cattr.structure rebuilds tuples when typed as Tuple[str, ...]."""
    cursor = OpenLineageCursor(
        last_storage_id=1,
        running_pipelines={
            "r": RunningPipeline(planned_asset_paths={("a", "b")}),
        },
    )
    restored = OpenLineageCursor.from_json(cursor.to_json())
    for path in restored.running_pipelines["r"].planned_asset_paths:
        assert isinstance(path, tuple)
        for element in path:
            assert isinstance(element, str)


def test_cursor_back_compat_v0_1_json_has_no_planned_asset_paths_field():
    """v0.1 cursors with no planned_asset_paths key must restructure cleanly."""
    v0_1_cursor = {
        "last_storage_id": 100,
        "running_pipelines": {
            "run-1": {
                "running_steps": {
                    "step-1": {
                        "step_run_id": "sid",
                        "input_datasets": [],
                        "output_datasets": [],
                    }
                },
                "repository_name": "repo-a",
            }
        },
    }
    restored = OpenLineageCursor.from_json(json.dumps(v0_1_cursor))
    running = restored.running_pipelines["run-1"]
    assert running.planned_asset_paths == set()
    assert running.repository_name == "repo-a"
    assert running.running_steps["step-1"] == RunningStep(step_run_id="sid")
