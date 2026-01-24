"""Evidence.dev project component for Dagster.

This module provides the main component class for integrating Evidence.dev
projects with Dagster, automatically discovering sources and generating assets.
"""

from dataclasses import dataclass, field
from functools import cached_property
from pathlib import Path
from typing import Annotated, Optional, Union

import dagster as dg
from dagster._annotations import beta, public
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
from .sources import EvidenceProjectTranslatorData, EvidenceSourceTranslatorData
from .translator import DagsterEvidenceTranslator


@beta
@public
@dataclass
class EvidenceProjectComponentV2(StateBackedComponent, Resolvable):
    """Evidence.dev project component with state-backed definitions.

    This component manages Evidence.dev dashboard projects and their deployments,
    automatically discovering sources and generating Dagster assets for each
    source query and the main project.

    Attributes:
        evidence_project: Evidence project configuration (local or studio).
        defs_state: Configuration for state management.

    Example:

        Basic local project with GitHub Pages deployment:

        .. code-block:: yaml

            # defs.yaml
            type: dagster_evidence.EvidenceProjectComponentV2
            attributes:
              evidence_project:
                project_type: local
                project_path: ./my-evidence-project
                project_deployment:
                  type: github_pages
                  github_repo: my-org/my-dashboard
                  branch: gh-pages

        Local project with custom deployment command:

        .. code-block:: yaml

            # defs.yaml
            type: dagster_evidence.EvidenceProjectComponentV2
            attributes:
              evidence_project:
                project_type: local
                project_path: ./my-evidence-project
                project_deployment:
                  type: custom
                  deploy_command: "rsync -avz build/ user@server:/var/www/dashboard/"

        Customizing asset specs by subclassing and overriding get_asset_spec:

        .. code-block:: python

            from dagster_evidence import (
                EvidenceSourceTranslatorData,
                EvidenceProjectComponentV2,
            )

            class CustomEvidenceComponent(EvidenceProjectComponentV2):
                def get_asset_spec(self, data):
                    spec = super().get_asset_spec(data)
                    if isinstance(data, EvidenceSourceTranslatorData):
                        return spec.replace_attributes(
                            key=spec.key.with_prefix("evidence"),
                        )
                    return spec
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

    @cached_property
    def _base_translator(self) -> DagsterEvidenceTranslator:
        """Return the translator instance for this component."""
        return DagsterEvidenceTranslator()

    @public
    def get_asset_spec(
        self, data: Union[EvidenceSourceTranslatorData, EvidenceProjectTranslatorData]
    ) -> dg.AssetSpec:
        """Get asset spec for an Evidence object using the configured translator.

        Override this method in a subclass to customize how Evidence sources
        and projects are translated to Dagster AssetSpecs.

        Args:
            data: Either EvidenceSourceTranslatorData for source queries
                  or EvidenceProjectTranslatorData for the main project asset.

        Returns:
            An AssetSpec that represents the Evidence object in Dagster.

        Example:

            Override this method to add custom metadata:

            .. code-block:: python

                from dagster_evidence import EvidenceProjectComponentV2

                class CustomEvidenceComponent(EvidenceProjectComponentV2):
                    def get_asset_spec(self, data):
                        spec = super().get_asset_spec(data)
                        return spec.replace_attributes(
                            metadata={"custom_key": "custom_value"},
                        )
        """
        return self._base_translator.get_asset_spec(data)

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
        evidence_project_data = dg.deserialize_value(
            state_path.read_text(), EvidenceProjectData
        )

        assets, sensors = self.evidence_project.load_evidence_project_assets(
            evidence_project_data,
            translator=self._base_translator,
        )

        return dg.Definitions(
            assets=assets,
            sensors=sensors,
            resources={"pipes_subprocess_client": dg.PipesSubprocessClient()},
        )
