"""Project classes for Evidence projects."""
import shutil

import os
from abc import abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Annotated, Literal, Optional, Union

import yaml
import dagster as dg
from pydantic import BaseModel, Field
from dagster._serdes import whitelist_for_serdes
from dagster.components import Resolver
from dagster.components.resolved.base import resolve_fields

from .deployments import (
    BaseEvidenceProjectDeployment,
    CustomEvidenceProjectDeploymentArgs,
    EvidenceProjectNetlifyDeploymentArgs,
    GithubPagesEvidenceProjectDeploymentArgs,
    resolve_evidence_project_deployment,
)
from .sources import BaseEvidenceProjectSource


@whitelist_for_serdes
@dataclass
class EvidenceProjectData:
    project_name: str
    sources_by_id: dict


class BaseEvidenceProject(dg.ConfigurableResource):

    @abstractmethod
    def get_evidence_project_name(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def load_evidence_project_assets(self, evidence_project_data: EvidenceProjectData) -> Sequence[dg.AssetSpec]:
        raise NotImplementedError()

    def load_source_assets(self, evidence_project_data: EvidenceProjectData) -> Sequence[dg.AssetSpec]:
        source_assets: list[dg.AssetSpec] = []
        for source_group, source_content in evidence_project_data.sources_by_id.items():
            source = BaseEvidenceProjectSource.resolve_source_type(source_content)
            source_assets = source_assets + source.get_source_asset_specs(source_group)

        return source_assets

    @abstractmethod
    def parse_evidence_project_sources(self) -> dict:
        raise NotImplementedError()

    def parse_evidence_project(self) -> EvidenceProjectData:
        sources = self.parse_evidence_project_sources()

        return EvidenceProjectData(
            project_name=self.get_evidence_project_name(),
            sources_by_id=sources
        )


class LocalEvidenceProject(BaseEvidenceProject):
    project_path: str
    project_deployment: BaseEvidenceProjectDeployment
    npm_executable: str = "npm"

    def parse_evidence_project_sources(self) -> dict:
        """Read sources folder from Evidence project and build source dictionary.

        Returns:
            dict: Dictionary mapping folder names to their connection config and queries.
                  Format: {folder_name: {connection: dict, queries: list[dict]}}
        """
        sources_path = Path(self.project_path) / "sources"

        if not sources_path.exists():
            raise FileNotFoundError(f"Sources folder not found: {sources_path}")

        result = {}

        for folder in sources_path.iterdir():
            if not folder.is_dir():
                continue

            # Read connection.yaml (required)
            connection_file = folder / "connection.yaml"
            if not connection_file.exists():
                raise FileNotFoundError(f"connection.yaml not found in {folder}")

            with open(connection_file, 'r') as f:
                connection = yaml.safe_load(f)

            # Read all .sql files
            queries = []
            for sql_file in folder.glob("*.sql"):
                query_name = sql_file.stem  # filename without extension
                query_content = sql_file.read_text()
                queries.append({
                    'name': query_name,
                    'content': query_content
                })

            result[folder.name] = {
                'connection': connection,
                'queries': queries
            }

        return result

    def get_evidence_project_name(self) -> str:
        return Path(self.project_path).name

    def _get_base_path(self) -> str:
        """Get the build output path from evidence.config.yaml.

        Returns:
            The build path. If basePath is configured, returns 'build/{basePath}'.
            Otherwise returns 'build'.
        """
        config_path = Path(self.project_path) / "evidence.config.yaml"
        if not config_path.exists():
            return "build"

        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        base_path = config.get("deployment", {}).get("basePath")
        if not base_path:
            return "build"

        # Remove leading slash and prepend with build/
        return f"build/{base_path.lstrip('/')}"

    def load_evidence_project_assets(self, evidence_project_data: EvidenceProjectData) -> Sequence[dg.AssetSpec]:
        source_assets = self.load_source_assets(evidence_project_data)

        @dg.asset(
            key=[self.get_evidence_project_name()],
            kinds={"evidence"},
            deps=[each_source_asset.key for each_source_asset in source_assets]
        )
        def build_and_deploy_evidence_project(
            context: dg.AssetExecutionContext,
            pipes_subprocess_client: dg.PipesSubprocessClient,
        ):
            with TemporaryDirectory() as temp_dir:
                temp_dir = temp_dir + "/project"
                shutil.copytree(self.project_path,temp_dir, ignore=shutil.ignore_patterns('logs', '.git', '*.tmp', "node_modules"))
                # Get base path for GitHub Pages (repo name)
                base_path = self._get_base_path()
                
                build_output_dir = base_path
                os.makedirs(build_output_dir, exist_ok=True)

                build_env = os.environ.copy()
                build_env["EVIDENCE_BUILD_DIR"] = build_output_dir
                build_env["EVIDENCE_PROJECT_TMP_DIR"] = temp_dir
                build_env["EVIDENCE_PROJECT_PATH"] = self.project_path 

                context.log.info(f"Building Evidence project to: {build_output_dir}")
                context.log.info(f"Evidence project path: {self.project_path}")

                self._run_cmd(
                    context=context,
                    pipes_subprocess_client=pipes_subprocess_client,
                    project_path=temp_dir,
                    cmd=[self.npm_executable, "install"],
                    env=build_env,
                )
                # Run sources command and yield results
                self._run_cmd(
                    context=context,
                    pipes_subprocess_client=pipes_subprocess_client,
                    project_path=temp_dir,
                    cmd=[self.npm_executable, "run", "sources"],
                    env=build_env,
                )

                # Run build command and yield results
                self._run_cmd(
                    context=context,
                    pipes_subprocess_client=pipes_subprocess_client,
                    project_path=temp_dir,
                    cmd=[self.npm_executable, "run", "build"],
                    env=build_env,
                )

                # Deploy from the build output folder and yield results
                self.project_deployment.deploy_evidence_project(
                    evidence_project_build_path=os.path.join(temp_dir,build_output_dir),
                    context=context,
                    pipes_subprocess_client=pipes_subprocess_client,
                    env=build_env,
                )
                return dg.MaterializeResult(metadata={"status": "success"})

                 

        return source_assets + [build_and_deploy_evidence_project]

    def _run_cmd(
        self,
        context: dg.AssetExecutionContext,
        pipes_subprocess_client: dg.PipesSubprocessClient,
        project_path: str,
        cmd: Sequence[str],
        env: Optional[dict] = None,
    ):
        context.log.info(f"{project_path}$ {' '.join(cmd)}")
        invocation = pipes_subprocess_client.run(
            command=cmd,
            cwd=project_path,
            context=context,
            env=env or os.environ,
        )

class LocalEvidenceProjectArgs(dg.Model, dg.Resolvable):
    """Arguments for configuring a local Evidence project."""

    project_type: Literal["local"] = Field(
        default="local",
        description="Project type identifier.",
    )
    project_path: str = Field(
        ...,
        description="Path to the Evidence project directory.",
    )
    project_deployment: Annotated[
        BaseEvidenceProjectDeployment,
        Resolver(
            resolve_evidence_project_deployment,
            model_field_name="project_deployment",
            model_field_type=Union[
                GithubPagesEvidenceProjectDeploymentArgs.model(),
                EvidenceProjectNetlifyDeploymentArgs.model(),
                CustomEvidenceProjectDeploymentArgs.model()
            ],
            description="Deployment configuration for the Evidence project.",
            examples=[],
        ),
    ]


class EvidenceStudioProject(BaseEvidenceProject):
    evidence_studio_url: str

    def parse_evidence_project_sources(self) -> dict:
        raise NotImplementedError()

    def get_evidence_project_name(self) -> str:
        raise NotImplementedError()

    def load_evidence_project_assets(self, evidence_project_data: EvidenceProjectData) -> Sequence[dg.AssetSpec]:
        raise NotImplementedError()


class EvidenceStudioProjectArgs(dg.Model, dg.Resolvable):
    """Arguments for configuring an Evidence Studio project."""

    project_type: Literal["evidence_studio"] = Field(
        default="evidence_studio",
        description="Project type identifier.",
    )
    evidence_studio_url: str = Field(
        ...,
        description="Evidence Studio URL.",
    )
    evidence_project_git_url: str = Field(
        default="no_url",
        description="Git URL for the Evidence project.",
    )


def resolve_evidence_project(
    context: dg.ResolutionContext, model: BaseModel
) -> BaseEvidenceProject:
    """Resolve project configuration to a project instance."""
    # First, check which type we're dealing with
    project_type = (
        model.get("project_type", "local") if isinstance(model, dict) else getattr(model, "project_type", "local")
    )

    if project_type == "local":
        resolved = resolve_fields(
            model=model, resolved_cls=LocalEvidenceProjectArgs, context=context
        )
        return LocalEvidenceProject(
            project_path=str(context.resolve_source_relative_path(resolved["project_path"])),
            project_deployment=resolved["project_deployment"]
        )
    elif project_type == "evidence_studio":
        resolved = resolve_fields(
            model=model, resolved_cls=EvidenceStudioProjectArgs, context=context
        )
        return EvidenceStudioProject(
            evidence_studio_url=resolved["evidence_studio_url"]
        )
    else:
        raise NotImplementedError(f"Unknown project type: {project_type}")
