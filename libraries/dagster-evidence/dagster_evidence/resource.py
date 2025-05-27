import os
import sys
import subprocess
from typing import Optional, Sequence, List
import dagster as dg
from pydantic import Field
from dagster import AssetSpec, multi_asset
from dataclasses import dataclass


class EvidenceResource(dg.ConfigurableResource):
    """A Dagster resource for managing Evidence.dev projects.

    This resource provides functionality to build and deploy Evidence.dev dashboards.
    It assumes that you have already created an Evidence.dev project. Read [the Evidence docs](https://docs.evidence.dev/) for more information.

    Examples:
        .. code-block:: python
            import dagster as dg
            from dagster_evidence import EvidenceResource

            @dg.asset
            def evidence_application(evidence: EvidenceResource):
                evidence.build()

            evidence_resource = EvidenceResource(
                project_path="/path/to/evidence/project",
                deploy_command="your-deploy-command",
            )

            defs = dg.Definitions(
                assets=[evidence_application],
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
        """Build the Evidence project."""
        self._run_cmd([self.npm_executable, "run", "build"])

    def sources(self, source: Optional[str] = None) -> None:
        """Run sources from the specified source.

        Args:
            source: The name of the source to run. If None, runs all sources.
        """
        cmd = [self.npm_executable, "run", "sources"]

        if source:
            cmd.append(f"--sources={source}")

        self._run_cmd(cmd)


@dataclass
class EvidenceFilter:
    """Filter for Evidence assets.

    Args:
        sources: List of source names to include. If None, all sources are included.
        only_fetch_dashboards: If True, only fetch dashboards and skip other assets.
    """

    sources: Optional[List[str]] = None
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

    specs = []
    if evidence_filter.sources is not None:
        for source in evidence_filter.sources:
            specs.append(
                AssetSpec(
                    key=source,
                    group_name="evidence",
                    description=f"Evidence source: {source}",
                )
            )

    @multi_asset(
        specs=specs,
    )
    def evidence_assets(evidence_resource: EvidenceResource):
        """Evidence assets for running sources and building dashboards."""
        if evidence_filter.sources is not None:
            for source in evidence_filter.sources:
                evidence_resource.sources(source=source)
        evidence_resource.build()
        return None

    return [evidence_assets]
