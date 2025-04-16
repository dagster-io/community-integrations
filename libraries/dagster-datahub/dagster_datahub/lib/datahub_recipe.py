from dagster._core.definitions.definitions_load_context import (
    StateBackedDefinitionsLoader,
)

from dagster import (
    AssetSpec,
    FreshnessPolicy,
    AutoMaterializePolicy,
    Definitions,
)

from typing import Union, Iterable, Any, Optional, Mapping, Sequence
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)
from datahub.emitter.mcp import (
    MetadataChangeProposalWrapper,
)
from datahub.utilities.urns.dataset_urn import DatasetUrn
from datahub.utilities.urns.urn import Urn
from datahub.configuration.config_loader import load_config_file
from pathlib import Path
import json
from dataclasses import dataclass
import re
from datahub.metadata.schema_classes import (
    UpstreamLineageClass,
    DatasetPropertiesClass,
    _Aspect,
)
from tempfile import TemporaryDirectory
import subprocess
from dagster.components import (
    Component,
    ComponentLoadContext,
    Resolvable,
)
from dagster.components.resolved.core_models import (
    AssetPostProcessor,
)

DatahubEvent = Union[
    MetadataChangeEvent, MetadataChangeProposal, MetadataChangeProposalWrapper
]


def parse_events_json(events_jsons) -> Iterable[DatahubEvent]:
    for events_json in events_jsons:
        events = json.loads(events_json)
        for event in events:
            parsed = None
            for cls in (
                MetadataChangeEvent,
                MetadataChangeProposal,
                MetadataChangeProposalWrapper,
            ):
                try:
                    candidate = cls.from_obj(event)
                    if not candidate.validate():
                        continue
                    parsed = candidate
                    break
                except Exception:
                    continue
            if parsed is None:
                # TODO: throw?
                continue
            yield parsed


@dataclass
class DatahubAssetSpec:
    asset_key: str
    deps: list[str]
    metadata: dict[str, Any]
    description: Optional[str]

    def to_asset_spec(
        self,
        deps: Optional[Iterable[str]] = None,
        description: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
        skippable: bool = False,
        group_name: Optional[str] = None,
        code_version: Optional[str] = None,
        freshness_policy: Optional[FreshnessPolicy] = None,
        auto_materialize_policy: Optional[AutoMaterializePolicy] = None,
        owners: Optional[Sequence[str]] = None,
        tags: Optional[Mapping[str, str]] = None,
    ) -> AssetSpec:
        deps_set = set(self.deps)
        if deps is not None:
            deps_set.update(deps)
        merged_metadata = self.metadata.copy()
        if metadata:
            merged_metadata.update(metadata)
        return AssetSpec(
            self.asset_key,
            deps=list(deps_set),
            metadata=merged_metadata,
            description=description or self.description,
            skippable=skippable,
            group_name=group_name,
            code_version=code_version,
            freshness_policy=freshness_policy,
            auto_materialize_policy=auto_materialize_policy,
            owners=owners,
            tags=tags,
        )


class DagsterDatahubTranslator:
    def get_asset_key(self, entity_urn: str) -> str:
        return re.sub(
            "[^A-Za-z0-9]+",
            "_",
            DatasetUrn.from_string(entity_urn).name,
        )


def load_datahub_assets(
    events: Iterable[DatahubEvent],
    translator: Optional[DagsterDatahubTranslator] = None,
) -> dict[str, DatahubAssetSpec]:
    if translator is None:
        translator = DagsterDatahubTranslator()

    rv: dict[str, DatahubAssetSpec] = {}

    def handle_aspect(asset_key: str, aspect: _Aspect):
        if asset_key not in rv:
            rv[asset_key] = DatahubAssetSpec(
                asset_key=asset_key, deps=[], metadata={}, description=None
            )
        spec = rv[asset_key]
        if isinstance(aspect, UpstreamLineageClass):
            spec.deps = [
                translator.get_asset_key(upstream.dataset)
                for upstream in aspect.upstreams
            ]
        if isinstance(aspect, DatasetPropertiesClass):
            spec.description = aspect.description

    for event in events:
        if isinstance(event, MetadataChangeEvent):
            if Urn.from_string(event.proposedSnapshot.urn).entity_type != "dataset":
                continue

            asset_key = translator.get_asset_key(event.proposedSnapshot.urn)
            for aspect in event.proposedSnapshot.aspects:
                handle_aspect(asset_key, aspect)
        elif isinstance(event, MetadataChangeProposalWrapper):
            if event.entityType != "dataset":
                continue
            if event.changeType not in ("UPSERT", "CREATE", "UPDATE"):
                continue
            if event.entityUrn is None or event.aspect is None:
                continue
            handle_aspect(translator.get_asset_key(event.entityUrn), event.aspect)

    return rv


@dataclass
class DatahubIngestRun:
    report: Mapping[str, Any]
    events_json: str


def run_datahub_ingest(
    source_config: Mapping[str, Any],
    datahub_cli_path: str = "datahub",
    cwd: Optional[str] = None,
) -> DatahubIngestRun:
    if "type" not in source_config:
        raise RuntimeError("source_config must contain a 'type' key")
    with TemporaryDirectory() as d:
        root = Path(d)
        (root / "source.yml").write_text(
            json.dumps(
                {
                    "source": source_config,
                    "sink": {
                        "type": "file",
                        "config": {"filename": str(root / "output.json")},
                    },
                }
            )
        )
        subprocess.run(
            [
                datahub_cli_path,
                "ingest",
                "-c",
                root / "source.yml",
                "--report-to",
                root / "report.json",
            ],
            cwd=cwd,
        ).check_returncode()
        return DatahubIngestRun(
            report=json.loads((root / "report.json").read_text()),
            events_json=Path(root / "output.json").read_text(),
        )


def parse_datahub_recipe(path: str) -> Mapping[str, Any]:
    data = load_config_file(path)
    return data["source"]


@dataclass
class DatahubDefsLoader(StateBackedDefinitionsLoader):
    recipes: Sequence[str]
    translator: Optional[DagsterDatahubTranslator] = None
    datahub_cli_path: str = "datahub"
    cwd: Optional[str] = None

    @property
    def defs_key(self) -> str:
        return f"datahub_{','.join(self.recipes)}_{self.cwd}"

    def fetch_state(self) -> Sequence[str]:
        all_events_jsons: list[str] = []
        for recipe in self.recipes:
            source = parse_datahub_recipe(recipe)
            all_events_jsons.append(
                run_datahub_ingest(source, self.datahub_cli_path, self.cwd).events_json
            )
        return all_events_jsons

    def defs_from_state(self, state: Sequence[str]):
        all_events = parse_events_json(state)
        return Definitions(
            assets=[
                datahub_asset.to_asset_spec()
                for datahub_asset in load_datahub_assets(
                    all_events, self.translator
                ).values()
            ]
        )


@dataclass
class DatahubRecipe(Component, Resolvable):
    """Expose an Evidence.dev dashboard as a Dagster asset.


    This component assumes that you have already created an Evidence.dev project. Read [the Evidence docs](https://docs.evidence.dev/) for more information.

    Args:
        project_path: The path to the Evidence.dev project.
        asset: The asset to expose.
        asset_post_processors: A list of asset post processors to apply to the asset.
        deploy_command: The command to run to deploy the static assets somewhere. The $EVIDENCE_BUILD_PATH environment variable will be set to the path of the build output.
        npm_executable: The executable to use to run npm commands.
    """

    recipe_path: str
    asset_post_processors: Optional[Sequence[AssetPostProcessor]] = None
    datahub_executable: str = "datahub"

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        loader = DatahubDefsLoader(
            recipes=[str(context.path / self.recipe_path)],
            translator=DagsterDatahubTranslator(),
            datahub_cli_path=self.datahub_executable,
            cwd=str(context.path),
        )
        defs = loader.defs_from_state(loader.fetch_state())
        for post_processor in self.asset_post_processors or []:
            defs = post_processor(defs)

        return defs
