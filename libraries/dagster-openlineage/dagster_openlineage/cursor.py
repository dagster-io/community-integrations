# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import json
from typing import Dict, List, Optional, Set, Tuple

import attr
import cattr
from openlineage.client.run import InputDataset, OutputDataset


@attr.define
class RunningStep:
    step_run_id: str
    input_datasets: List[InputDataset] = attr.field(factory=list)
    output_datasets: List[OutputDataset] = attr.field(factory=list)


@attr.define
class RunningPipeline:
    running_steps: Dict[str, RunningStep] = attr.field(factory=dict)
    repository_name: Optional[str] = None
    # Planned-but-not-materialized asset paths per run. Stored as tuple paths
    # rather than AssetKey: AssetKey is a @record (not attrs-compatible) —
    # attr.asdict raises NotAnAttrsClassError and cattr.structure without a
    # custom hook corrupts path elements character-by-character.
    # factory=set lets v0.1 cursor JSON (missing this field) round-trip
    # cleanly — cattrs fills the default on structure.
    planned_asset_paths: Set[Tuple[str, ...]] = attr.field(factory=set)


@attr.define
class OpenLineageCursor:
    last_storage_id: int
    running_pipelines: Dict[str, RunningPipeline] = attr.field(factory=dict)

    def to_json(self):
        return json.dumps(attr.asdict(self), default=list)

    @staticmethod
    def from_json(json_str: str):
        return cattr.structure(json.loads(json_str), OpenLineageCursor)
