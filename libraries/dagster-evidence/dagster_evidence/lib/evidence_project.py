import os
import shlex
import sys
from string import Template
from tempfile import TemporaryDirectory
from dagster import Definitions, multi_asset
from dagster.components import (
    Component,
    ComponentLoadContext,
    Resolvable,
)
from dagster.components.resolved.core_models import (
    AssetPostProcessor,
    ResolvedAssetSpec,
)
from dataclasses import dataclass
from typing import Optional
from collections.abc import Sequence
import subprocess


@dataclass
class EvidenceProject(Component, Resolvable):
    """Expose an Evidence.dev dashboard as a Dagster asset.


    This component assumes that you have already created an Evidence.dev project. Read [the Evidence docs](https://docs.evidence.dev/) for more information.

    Args:
        project_path: The path to the Evidence.dev project.
        asset: The asset to expose.
        asset_post_processors: A list of asset post processors to apply to the asset.
        deploy_command: The command to run to deploy the static assets somewhere. The $EVIDENCE_BUILD_PATH environment variable will be set to the path of the build output.
            The command is split into arguments with shell quoting rules and run without a shell, so `$VAR` and `${VAR}`
            references are expanded but pipes, `&&`, redirection and globs are not. Wrap the command in `sh -c '...'` or a script to use them.
        npm_executable: The executable to use to run npm commands.
    """

    project_path: str
    asset: ResolvedAssetSpec
    asset_post_processors: Sequence[AssetPostProcessor] | None = None
    deploy_command: str | None = None
    npm_executable: str = "npm"

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        project_path = os.path.abspath(context.path / self.project_path)

        def _run_cmd(cmd: Sequence[str]) -> None:
            print(
                f"{project_path}$ {' '.join(cmd)}",
                file=sys.stderr,
            )
            subprocess.run(
                cmd,
                cwd=project_path,
                check=True,
                capture_output=False,
                env=dict(os.environ),
            )

        @multi_asset(specs=[self.asset])
        def evidence_project():
            _run_cmd([self.npm_executable, "run", "sources"])
            _run_cmd([self.npm_executable, "run", "build"])

            if self.deploy_command is not None:
                env = os.environ.copy()
                env["EVIDENCE_BUILD_PATH"] = os.path.join(project_path, "build")

                # No shell: config values must not be able to start extra commands.
                deploy_argv = [
                    Template(arg).safe_substitute(env)
                    for arg in shlex.split(self.deploy_command)
                ]
                with TemporaryDirectory() as temp_dir:
                    print(f"{temp_dir}$ {shlex.join(deploy_argv)}", file=sys.stderr)
                    subprocess.run(
                        deploy_argv,
                        cwd=temp_dir,
                        check=True,
                        capture_output=False,
                        env=env,
                    )

        defs = Definitions(assets=[evidence_project])
        for post_processor in self.asset_post_processors or []:
            defs = post_processor(defs)

        return defs
