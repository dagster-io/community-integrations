"""Evidence.dev project component for Dagster."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Optional, Union

import dagster as dg
from dagster.components import Resolvable, Resolver, StateBackedComponent
from dagster.components.utils.defs_state import (
    DefsStateConfig,
    DefsStateConfigArgs,
    ResolvedDefsStateConfig,
)

from .projects import (
    BaseEvidenceProject,
    EvidenceProjectData,
    EvidenceStudioProjectArgs,
    LocalEvidenceProjectArgs,
    resolve_evidence_project,
)


@dataclass
class EvidenceProjectComponentV2(StateBackedComponent, Resolvable):
    """Evidence.dev project component with state-backed definitions.

    This component manages Evidence.dev dashboard projects and their deployments.
    """

    evidence_project: Annotated[
        BaseEvidenceProject,
        Resolver(
            resolve_evidence_project,
            model_field_name="evidence_project",
            model_field_type=Union[
                LocalEvidenceProjectArgs.model(), EvidenceStudioProjectArgs.model()
            ],
            description="Evidence project configuration.",
            examples=[],
        ),
    ]

    defs_state: ResolvedDefsStateConfig = field(
        default_factory=DefsStateConfigArgs.legacy_code_server_snapshots
    )

    async def write_state_to_path(self, state_path: Path) -> None:
        # Fetch the workspace data
        evidence_project_data = self.evidence_project.parse_evidence_project()

        # Serialize and write to path
        state_path.write_text(dg.serialize_value(evidence_project_data))

    @property
    def defs_state_config(self) -> DefsStateConfig:
        default_key = f"{self.__class__.__name__}[{self.evidence_project.get_evidence_project_name()}]"
        return DefsStateConfig.from_args(self.defs_state, default_key=default_key)

    def build_defs_from_state(
        self, context: dg.ComponentLoadContext, state_path: Optional[Path]
    ) -> dg.Definitions:
        """Builds Dagster definitions from the cached Evidence project state."""
        if state_path is None:
            return dg.Definitions()

        # Deserialize evidence project data
        evidence_project_data = dg.deserialize_value(state_path.read_text(), EvidenceProjectData)

        assets = self.evidence_project.load_evidence_project_assets(evidence_project_data)

        return dg.Definitions(
            assets=assets,
            resources={"pipes_subprocess_client": dg.PipesSubprocessClient()},
        )
