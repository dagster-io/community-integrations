import os
import sys
import subprocess
from typing import Optional, Sequence, List
import dagster as dg
from pydantic import Field
from dagster import AssetSpec, multi_asset
from dataclasses import dataclass
from pathlib import Path


class EvidenceResource(dg.ConfigurableResource):
    """A Dagster resource for managing Evidence.dev projects.

    This resource provides functionality to build and deploy Evidence.dev dashboards.
    It assumes that you have already created an Evidence.dev project. Read [the Evidence docs](https://docs.evidence.dev/) for more information.

    Examples:
        .. code-block:: python
            import dagster as dg
            from dagster_evidence import EvidenceResource, load_evidence_asset_specs

            @dg.asset
            def evidence_dashboard(evidence: EvidenceResource):
                evidence.build()

            defs = dg.Definitions(
                assets=evidence_specs,
                resources={"evidence": evidence_resource}
            )

        .. code-block:: python

            import dagster as dg
            from dagster_evidence import EvidenceResource, load_evidence_asset_specs

            evidence_resource = EvidenceResource(
                project_path="/path/to/evidence/project",
                deploy_command="your-deploy-command",
            )

            evidence_specs = load_evidence_asset_specs(evidence_resource)

            defs = dg.Definitions(
                assets=evidence_specs,
                resources={"evidence": evidence_resource}
            )

    """

    project_path: str = Field(..., description="The path to the Evidence.dev project")
    deploy_command: Optional[str] = Field(
        None, description="Command to deploy the built assets"
    )
    npm_executable: str = Field(
        default="npm", description="The executable to use for npm commands"
    )

    def _run_cmd(self, cmd: Sequence[str]):
        print(
            f"{self.project_path}$ {' '.join(cmd)}",
            file=sys.stderr,
        )
        subprocess.run(
            cmd,
            cwd=self.project_path,
            check=True,
            capture_output=False,
            env=os.environ,
        )

    def build(self) -> None:
        """Build the Evidence project by running the sources and build commands."""
        self._run_cmd([self.npm_executable, "run", "sources"])
        self._run_cmd([self.npm_executable, "run", "build"])


@dataclass
class EvidenceFilter:
    """Filter for Evidence assets.

    Args:
        dashboard_directories: List of dashboard directory paths to include. If None, all dashboards are included.
        only_fetch_dashboards: If True, only fetch dashboards and skip other assets.
    """

    dashboard_directories: Optional[List[str]] = None
    only_fetch_dashboards: bool = False


def load_evidence_asset_specs(
    evidence_resource: EvidenceResource,
    evidence_filter: Optional[EvidenceFilter] = None,
) -> List[dg.AssetsDefinition]:
    """Load Evidence assets as Dagster asset specs.

    Args:
        evidence_resource: The Evidence resource to use.
        evidence_filter: Optional filter to limit which assets are loaded.

    Returns:
        List of Dagster asset definitions.
    """
    evidence_filter = evidence_filter or EvidenceFilter()

    project_path = Path(evidence_resource.project_path)
    pages_dir = project_path / "pages"

    specs = []
    for md_file in pages_dir.rglob("*.md"):
        # Skip files not in filtered directories if filter is provided
        if evidence_filter.dashboard_directories:
            relative_path = md_file.relative_to(pages_dir)
            parent_dir = relative_path.parent
            if str(parent_dir) not in evidence_filter.dashboard_directories:
                continue

        asset_key = md_file.with_suffix("").name

        specs.append(
            AssetSpec(
                key=asset_key,
                group_name="evidence",
                description=f"Evidence page: {md_file.relative_to(pages_dir)}",
            )
        )

    if not evidence_filter.only_fetch_dashboards:
        specs.extend(
            [
                AssetSpec(
                    key="evidence_build",
                    group_name="evidence",
                    description="Build Evidence project",
                ),
            ]
        )

    @multi_asset(
        specs=specs,
    )
    def evidence_assets(evidence_resource: EvidenceResource):
        """Evidence assets for building and deploying dashboards."""
        evidence_resource.build()
        return None

    return [evidence_assets]
