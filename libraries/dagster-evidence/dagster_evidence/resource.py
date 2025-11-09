import os
import sys
import subprocess
from collections.abc import Sequence
import dagster as dg
from pydantic import Field


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
                deploy_command="optional-deploy-command",  # eg. `aws s3 cp ...`
            )

            defs = dg.Definitions(
                assets=[evidence_application],
                resources={"evidence": evidence_resource}
            )
    """

    project_path: str = Field(..., description="The path to the Evidence.dev project")
    deploy_command: str | None = Field(
        None, description="Command to deploy the built assets"
    )
    executable: str = Field(
        default="npm",
        description="The executable to use for commands (npm, yarn, etc.)",
    )

    def _run_cmd(self, cmd: Sequence[str]):
        """Run a command in the project directory.

        Args:
            cmd: The command to run as a sequence of strings.
        """
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
        """Build the Evidence project.

        1. Run all sources to fetch data
        2. Build the project to generate the dashboard
        """
        self._run_cmd([self.executable, "run", "sources"])
        self._run_cmd([self.executable, "run", "build"])
