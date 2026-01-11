from re import I
from pathlib import Path
import os
import sys
from tempfile import TemporaryDirectory
import yaml
import dagster as dg
from dataclasses import dataclass, field
from dagster.components.utils.defs_state import (
    DefsStateConfig,
    DefsStateConfigArgs,
    ResolvedDefsStateConfig,
)
from dagster.components import (
    StateBackedComponent,
    ComponentLoadContext,
    Resolvable,
    Resolver,
    ComponentLoadContext,
    Model
)
from dagster._serdes import whitelist_for_serdes
from dagster.components.resolved.base import resolve_fields
from abc import abstractmethod
from dataclasses import dataclass
from typing import Annotated, Literal, Optional, Union
from collections.abc import Sequence
import subprocess
from pydantic import Field, BaseModel

class BaseEvidenceProjectDeployment(dg.ConfigurableResource):
    
    @abstractmethod
    def deploy_evidence_project(self, evidence_project_build_path: str):
        raise NotImplementedError()

class CustomEvidenceProjectDeployment(BaseEvidenceProjectDeployment):
    deploy_command: str

    def deploy_evidence_project(self, evidence_project_build_path: str):
        if self.deploy_command is not None:
            env = os.environ.copy()
            env["EVIDENCE_BUILD_PATH"] = evidence_project_build_path

            with TemporaryDirectory() as temp_dir:
                print(f"{temp_dir}$ {self.deploy_command}", file=sys.stderr)
                subprocess.run(
                    self.deploy_command,
                    cwd=temp_dir,
                    check=True,
                    capture_output=False,
                    env=env,
                    shell=True,
                )


class CustomEvidenceProjectDeploymentArgs(dg.Model, dg.Resolvable):
    """Arguments for configuring a Tableau Cloud workspace connection."""

    type: Literal["custom"] = Field(
        default="custom",
        description="",
    )
    deploy_command: str = Field(
        ...,
        description="",
    )


class GithubPagesEvidenceProjectDeployment(BaseEvidenceProjectDeployment):
    def deploy_evidence_project(self, evidence_project_build_path: str):
        raise NotImplementedError()

class GithubPagesEvidenceProjectDeploymentArgs(dg.Model, dg.Resolvable):
    """Arguments for configuring a Tableau Cloud workspace connection."""

    type: Literal["github_pages"] = Field(
        default="github_pages",
        description="",
    )
    githab_project_url: str = Field(
        ...,
        description="",
    )

class EvidenceProjectNetlifyDeployment(BaseEvidenceProjectDeployment):
    def deploy_evidence_project(self, evidence_project_build_path: str):
        raise NotImplementedError()


class EvidenceProjectNetlifyDeploymentArgs(dg.Model, dg.Resolvable):
    """Arguments for configuring a Tableau Cloud workspace connection."""

    type: Literal["netlify"] = Field(
        default="netlify",
        description="",
    )
    netlify_project_url: str = Field(
        ...,
        description="",
    )


def _resolve_evidence_project_deployment(
    context: dg.ResolutionContext, model: BaseModel
) -> BaseEvidenceProjectDeployment:
    # First, check which type we're dealing with
    deployment_type = (
        model.get("type", "custom") if isinstance(model, dict) else getattr(model, "type", "custom")
    )

    if deployment_type == "github_pages":
        resolved = resolve_fields(
            model=model, resolved_cls=GithubPagesEvidenceProjectDeploymentArgs, context=context
        )
        return GithubPagesEvidenceProjectDeployment(
            githab_project_url=resolved["githab_project_url"] 
        )
    elif deployment_type == "netlify" :
        resolved = resolve_fields(
            model=model, resolved_cls=EvidenceProjectNetlifyDeploymentArgs, context=context
        )
        return EvidenceProjectNetlifyDeployment(
            netlify_project_url=resolved["netlify_project_url"],
        )
    elif deployment_type == "custom" :
        resolved = resolve_fields(
            model=model, resolved_cls=CustomEvidenceProjectDeploymentArgs, context=context
        )
        return CustomEvidenceProjectDeployment(
            deploy_command=resolved["deploy_command"],
        )
    else:
        raise NotImplementedError()

@dataclass
class BaseEvidenceProjectSource:
    source_content: dict

    @staticmethod
    @abstractmethod
    def get_source_type():
        raise NotImplementedError()

    @abstractmethod
    def get_source_asset_specs(self, source_group: str):
        raise NotImplementedError()

    @staticmethod
    def resolve_source_type(source_content) -> "BaseEvidenceProjectSource":
        source_type =source_content.get("connection").get("type") 
        if source_type == DuckdbEvidenceProjectSource.get_source_type():
           return DuckdbEvidenceProjectSource(source_content)

        if source_type == MotherDuckEvidenceProjectSource.get_source_type():
           return MotherDuckEvidenceProjectSource(source_content)
        
        if source_type == BigQueryEvidenceProjectSource.get_source_type():
           return BigQueryEvidenceProjectSource(source_content)

        raise NotImplementedError()
 
class DuckdbEvidenceProjectSource(BaseEvidenceProjectSource):
    @staticmethod
    def get_source_type():
        return 'duckdb'
    
    def get_source_asset_specs(self, source_group: str):
        source_asset_specs = []
        for each_query in self.source_content.get("queries", []):
            source_asset_specs.append(dg.AssetSpec(each_query.get("name"), kinds={"evidence", "source", self.get_source_type()}))
        return source_asset_specs

class MotherDuckEvidenceProjectSource(BaseEvidenceProjectSource):
    @staticmethod
    def get_source_type():
        return 'motherduck'
    
    def get_source_asset_specs(self, source_group: str):
        source_asset_specs = []
        for each_query in self.source_content.get("queries", []):
            source_asset_specs.append(dg.AssetSpec(each_query.get("name"), kinds={"evidence", "source", self.get_source_type()}))
        return source_asset_specs

class BigQueryEvidenceProjectSource(BaseEvidenceProjectSource):
    @staticmethod
    def get_source_type():
        return 'bigquery'
    
    def get_source_asset_specs(self, source_group: str):
        source_asset_specs = []
        for each_query in self.source_content.get("queries", []):
            source_asset_specs.append(dg.AssetSpec(each_query.get("name"), kinds={"evidence", "source", self.get_source_type()}))
        return source_asset_specs

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
        source_assets:list[dg.AssetSpec] = []
        for source_group, source_content in evidence_project_data.sources_by_id.items():
            source = BaseEvidenceProjectSource.resolve_source_type(source_content)
            source_assets = source_assets + source.get_source_asset_specs(source_group)
        
        return source_assets

    @abstractmethod 
    def parse_evidence_project_sources(self)-> dict:
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
    
    def load_evidence_project_assets(self, evidence_project_data: EvidenceProjectData) -> Sequence[dg.AssetSpec]:
        source_assets = self.load_source_assets(evidence_project_data)
        #TODO add deps to asset bellow
        @dg.asset(
            key=[self.get_evidence_project_name()],
            kinds={"evidence"},
            deps= [each_source_asset.key for each_source_asset in source_assets]
        )
        def build_and_deploy_evidence_project():
            self._run_cmd(self.project_path, cmd=[self.npm_executable, "run", "sources"])
            self._run_cmd(self.project_path, cmd=[self.npm_executable, "run", "build"])
            evidence_project_build_path = os.path.join(self.project_path, "build")
            self.project_deployment.deploy_evidence_project(evidence_project_build_path)

        return source_assets + [build_and_deploy_evidence_project]
    
    def _run_cmd(self, project_path, cmd: Sequence[str]):
        print(
            f"{project_path}$ {' '.join(cmd)}",
            file=sys.stderr,
        )
        subprocess.run(
            cmd,
            cwd=project_path,
            check=True,
            capture_output=False,
            env=os.environ,
        )

class LocalEvidenceProjectArgs(dg.Model, dg.Resolvable):

    project_type: Literal["local"] = Field(
        default="local",
        description="",
    )
    project_path: str = Field(
        ...,
        description="",
    )
    project_deployment: Annotated[
        BaseEvidenceProjectDeployment,
        Resolver(
            _resolve_evidence_project_deployment,
            model_field_name="project_deployment",
            model_field_type=Union[
                GithubPagesEvidenceProjectDeploymentArgs.model(), 
                EvidenceProjectNetlifyDeploymentArgs.model(), 
                CustomEvidenceProjectDeploymentArgs.model()
            ],
            description="",
            examples=[
            ],
        ),
    ] 


class EvidenceStudioProject(BaseEvidenceProject):
    evidence_studio_url: str

    def get_evidence_project_sources(self) -> dict:
        raise NotImplementedError()
    
    def get_evidence_project_name(self) -> str:
        raise NotImplementedError()

class EvidenceStudioProjectArgs(dg.Model, dg.Resolvable):
    project_type: Literal["evidence_studio"] = Field(
        default="evidence_studio",
        description="",
    )
    evidence_studio_url: str = Field(
        ...,
        description="",
    )
    evidence_project_git_url: str = Field(
        default="no_url",
        description="",
    )

def _resolve_evidence_project(
    context: dg.ResolutionContext, model: BaseModel
) -> BaseEvidenceProject:
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
    elif project_type == "evidence_studio" :
        resolved = resolve_fields(
            model=model, resolved_cls=EvidenceStudioProjectArgs, context=context
        )
        return EvidenceStudioProject(
            evidence_studio_url=resolved["evidence_studio_url"]
        )
    else:
        raise NotImplementedError()

@dataclass
class EvidenceProjectComponentV2(StateBackedComponent, Resolvable):
    """Evidence.dev project component with state-backed definitions.

    This component manages Evidence.dev dashboard projects and their deployments.
    """

    evidence_project: Annotated[
        BaseEvidenceProject,
        Resolver(
            _resolve_evidence_project,
            model_field_name="evidence_project",
            model_field_type=Union[
                LocalEvidenceProjectArgs.model(), EvidenceStudioProjectArgs.model()
            ],
            description="",
            examples=[
            ],
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
        """Builds Dagster definitions from the cached Tableau workspace state."""
        if state_path is None:
            return dg.Definitions()
        
        # Deserialize evidence project data
        evidence_project_data = dg.deserialize_value(state_path.read_text(), EvidenceProjectData)

        assets = self.evidence_project.load_evidence_project_assets(evidence_project_data)

        return dg.Definitions(assets=assets)

