"""Project classes for Evidence projects.

This module defines the project types for Evidence projects, including
local file-based projects and Evidence Studio cloud projects.
"""

import os
import shutil
from abc import abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Annotated, Literal, Optional, Union

import dagster as dg
import yaml
from dagster._annotations import beta, public
from dagster._serdes import whitelist_for_serdes
from dagster.components import Resolver
from dagster.components.resolved.base import resolve_fields
from pydantic import BaseModel, Field

from .deployments import (
    BaseEvidenceProjectDeployment,
    CustomEvidenceProjectDeploymentArgs,
    EvidenceProjectNetlifyDeploymentArgs,
    GithubPagesEvidenceProjectDeploymentArgs,
    resolve_evidence_project_deployment,
)
from .sources import (
    EvidenceProjectTranslatorData,
    EvidenceSourceTranslatorData,
    SourceContent,
)

if TYPE_CHECKING:
    from .translator import DagsterEvidenceTranslator


@beta
@public
@whitelist_for_serdes
@dataclass
class EvidenceProjectData:
    """Parsed data from an Evidence project.

    Attributes:
        project_name: The name of the Evidence project.
        sources_by_id: Dictionary mapping source folder names to their content.
    """

    project_name: str
    sources_by_id: dict[str, SourceContent]


@beta
@public
class BaseEvidenceProject(dg.ConfigurableResource):
    """Base class for Evidence project configurations.

    This abstract class defines the interface for Evidence projects.
    Implementations include LocalEvidenceProject for local file-based
    projects and EvidenceStudioProject for cloud-hosted projects.

    Subclass this class to implement custom project types.
    """

    @public
    @abstractmethod
    def get_evidence_project_name(self) -> str:
        """Get the name of the Evidence project.

        Returns:
            The project name string.
        """
        raise NotImplementedError()

    @public
    @abstractmethod
    def load_evidence_project_assets(
        self,
        evidence_project_data: EvidenceProjectData,
        translator: "DagsterEvidenceTranslator",
    ) -> Sequence[dg.AssetsDefinition | dg.AssetSpec]:
        """Load and build Dagster assets for this Evidence project.

        Args:
            evidence_project_data: Parsed project data containing sources.
            translator: Translator instance for converting Evidence objects to AssetSpecs.

        Returns:
            A sequence of AssetSpecs and AssetsDefinitions.
        """
        raise NotImplementedError()

    @public
    def load_source_assets(
        self,
        evidence_project_data: EvidenceProjectData,
        translator: "DagsterEvidenceTranslator",
    ) -> list[dg.AssetsDefinition]:
        """Load source assets using the translator.

        Args:
            evidence_project_data: Parsed project data containing sources.
            translator: Translator instance for converting Evidence objects to assets.

        Returns:
            List of AssetsDefinition for all source queries.
        """
        source_assets: list[dg.AssetsDefinition] = []
        for source_group, source_content in evidence_project_data.sources_by_id.items():
            # Use translator to get source class (validates source type is known)
            source_type = source_content.connection.type
            source_class = translator.get_source_class(source_type)
            # Instantiate source (useful for potential future source-specific logic)
            _source = source_class(source_content)

            # Generate asset specs via translator
            for query in source_content.queries:
                # First create data without extracted_data
                initial_data = EvidenceSourceTranslatorData(
                    source_content=source_content,
                    source_group=source_group,
                    query=query,
                )
                # Extract data using source class
                extracted = source_class.extract_data_from_source(initial_data)
                # Create final data with extracted_data
                data = EvidenceSourceTranslatorData(
                    source_content=source_content,
                    source_group=source_group,
                    query=query,
                    extracted_data=extracted,
                )
                source_assets.append(translator.get_asset_spec(data))

        return source_assets

    @public
    @abstractmethod
    def parse_evidence_project_sources(self) -> dict[str, SourceContent]:
        """Parse the sources from the Evidence project.

        Returns:
            Dictionary mapping source folder names to their SourceContent.
        """
        raise NotImplementedError()

    @public
    def parse_evidence_project(self) -> EvidenceProjectData:
        """Parse the full Evidence project into structured data.

        Returns:
            EvidenceProjectData containing project name and sources.
        """
        sources = self.parse_evidence_project_sources()

        return EvidenceProjectData(
            project_name=self.get_evidence_project_name(), sources_by_id=sources
        )


@beta
@public
class LocalEvidenceProject(BaseEvidenceProject):
    """Local Evidence project backed by a file system directory.

    This project type reads sources from a local Evidence project directory,
    builds the project using npm, and deploys using the configured deployment.

    Attributes:
        project_path: Path to the Evidence project directory containing
            sources/ folder and package.json.
        project_deployment: Deployment configuration (GitHub Pages, Netlify, or custom).
        npm_executable: Path to npm executable (default: "npm").

    Example:

        Using in a Dagster component configuration:

        .. code-block:: yaml

            # defs.yaml
            type: dagster_evidence.EvidenceProjectComponentV2
            attributes:
              evidence_project:
                project_type: local
                project_path: ./evidence-dashboards/sales-dashboard
                project_deployment:
                  type: github_pages
                  github_repo: my-org/sales-dashboard
                  branch: gh-pages

        Project structure expected:

        .. code-block:: text

            my-evidence-project/
            ├── package.json
            ├── evidence.config.yaml
            └── sources/
                ├── orders_db/
                │   ├── connection.yaml
                │   ├── orders.sql
                │   └── customers.sql
                └── metrics_db/
                    ├── connection.yaml
                    └── daily_metrics.sql
    """

    project_path: str
    project_deployment: BaseEvidenceProjectDeployment
    npm_executable: str = "npm"

    def parse_evidence_project_sources(self) -> dict[str, SourceContent]:
        """Read sources folder from Evidence project and build source dictionary.

        Returns:
            Dictionary mapping folder names to their SourceContent.

        Raises:
            FileNotFoundError: If sources folder or connection.yaml files are missing.
        """
        sources_path = Path(self.project_path) / "sources"

        if not sources_path.exists():
            raise FileNotFoundError(f"Sources folder not found: {sources_path}")

        result: dict[str, SourceContent] = {}

        for folder in sources_path.iterdir():
            if not folder.is_dir():
                continue

            # Read connection.yaml (required)
            connection_file = folder / "connection.yaml"
            if not connection_file.exists():
                raise FileNotFoundError(f"connection.yaml not found in {folder}")

            with open(connection_file) as f:
                connection = yaml.safe_load(f)

            # Read all .sql files
            queries = []
            for sql_file in folder.glob("*.sql"):
                query_name = sql_file.stem  # filename without extension
                query_content = sql_file.read_text()
                queries.append({"name": query_name, "content": query_content})

            # For gsheets, synthesize queries from sheets config (no SQL files)
            if connection.get("type") == "gsheets":
                from .sources import GSheetsEvidenceProjectSource

                queries = GSheetsEvidenceProjectSource.build_queries_from_sheets_config(
                    connection
                )

            result[folder.name] = SourceContent.from_dict(
                {"connection": connection, "queries": queries}
            )

        return result

    def get_evidence_project_name(self) -> str:
        return Path(self.project_path).name

    def load_evidence_project_assets(
        self,
        evidence_project_data: EvidenceProjectData,
        translator: "DagsterEvidenceTranslator",
    ) -> Sequence[dg.AssetsDefinition | dg.AssetSpec]:
        # Get source assets via translator
        source_assets = self.load_source_assets(evidence_project_data, translator)

        # Get project asset spec via translator
        project_data = EvidenceProjectTranslatorData(
            project_name=self.get_evidence_project_name(),
            sources_by_id=evidence_project_data.sources_by_id,
            source_deps=[asset.key for asset in source_assets],
        )
        project_spec = translator.get_asset_spec(project_data)

        # Create automation condition that triggers when any direct dependency is updated
        # Source assets now have their own automation conditions that cascade updates
        automation_condition = dg.AutomationCondition.any_deps_match(
            dg.AutomationCondition.newly_updated()
        )

        # Create the executable asset using the translated spec
        @dg.asset(
            key=project_spec.key,
            kinds=project_spec.kinds or {"evidence"},
            deps=[each_source_asset.key for each_source_asset in source_assets],
            automation_condition=automation_condition,
        )
        def build_and_deploy_evidence_project(
            context: dg.AssetExecutionContext,
            pipes_subprocess_client: dg.PipesSubprocessClient,
        ):
            with TemporaryDirectory() as temp_dir:
                temp_dir = temp_dir + "/project"
                shutil.copytree(
                    self.project_path,
                    temp_dir,
                    ignore=shutil.ignore_patterns(
                        "logs", ".git", "*.tmp", "node_modules"
                    ),
                )
                # Get base path from deployment (e.g., GitHub Pages needs basePath config)
                base_path = self.project_deployment.get_base_path(self.project_path)

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
                    evidence_project_build_path=os.path.join(
                        temp_dir, build_output_dir
                    ),
                    context=context,
                    pipes_subprocess_client=pipes_subprocess_client,
                    env=build_env,
                )
                return dg.MaterializeResult(metadata={"status": "success"})

        return list(source_assets) + [build_and_deploy_evidence_project]

    def _run_cmd(
        self,
        context: dg.AssetExecutionContext,
        pipes_subprocess_client: dg.PipesSubprocessClient,
        project_path: str,
        cmd: Sequence[str],
        env: Optional[dict[str, str]] = None,
    ) -> None:
        context.log.info(f"{project_path}$ {' '.join(cmd)}")
        pipes_subprocess_client.run(
            command=cmd,
            cwd=project_path,
            context=context,
            env=env or dict(os.environ),
        )


@beta
@public
class LocalEvidenceProjectArgs(dg.Model, dg.Resolvable):
    """Arguments for configuring a local Evidence project.

    Example:

        .. code-block:: yaml

            evidence_project:
              project_type: local
              project_path: ./my-evidence-project
              project_deployment:
                type: github_pages
                github_repo: owner/repo

    Attributes:
        project_type: Must be "local" to use this project type.
        project_path: Path to the Evidence project directory.
        project_deployment: Deployment configuration for the built project.
    """

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
                CustomEvidenceProjectDeploymentArgs.model(),
            ],
            description="Deployment configuration for the Evidence project.",
            examples=[],
        ),
    ]


@beta
@public
class EvidenceStudioProject(BaseEvidenceProject):
    """Evidence Studio cloud-hosted project.

    This project type connects to Evidence Studio to fetch project
    configuration and sources from the cloud.

    Note:
        This project type is not yet fully implemented.

    Attributes:
        evidence_studio_url: URL of the Evidence Studio workspace.

    Example:

        .. code-block:: yaml

            evidence_project:
              project_type: evidence_studio
              evidence_studio_url: https://evidence.studio/my-workspace
    """

    evidence_studio_url: str

    def parse_evidence_project_sources(self) -> dict[str, SourceContent]:
        raise NotImplementedError()

    def get_evidence_project_name(self) -> str:
        raise NotImplementedError()

    def load_evidence_project_assets(
        self,
        evidence_project_data: EvidenceProjectData,
        translator: "DagsterEvidenceTranslator",
    ) -> Sequence[dg.AssetsDefinition | dg.AssetSpec]:
        raise NotImplementedError()


@beta
@public
class EvidenceStudioProjectArgs(dg.Model, dg.Resolvable):
    """Arguments for configuring an Evidence Studio project.

    Example:

        .. code-block:: yaml

            evidence_project:
              project_type: evidence_studio
              evidence_studio_url: https://evidence.studio/my-workspace
              evidence_project_git_url: https://github.com/org/evidence-project.git

    Attributes:
        project_type: Must be "evidence_studio" to use this project type.
        evidence_studio_url: URL of the Evidence Studio workspace.
        evidence_project_git_url: Git URL for the underlying project repository.
    """

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


@public
def resolve_evidence_project(
    context: dg.ResolutionContext, model: BaseModel
) -> BaseEvidenceProject:
    """Resolve project configuration to a concrete project instance.

    This function is used internally by the component resolution system
    to convert YAML configuration into project instances.

    Args:
        context: The resolution context providing access to paths and config.
        model: The parsed configuration model.

    Returns:
        A BaseEvidenceProject instance (LocalEvidenceProject or EvidenceStudioProject).

    Raises:
        NotImplementedError: If an unknown project_type is specified.
    """
    # First, check which type we're dealing with
    project_type = (
        model.get("project_type", "local")
        if isinstance(model, dict)
        else getattr(model, "project_type", "local")
    )

    if project_type == "local":
        resolved = resolve_fields(
            model=model, resolved_cls=LocalEvidenceProjectArgs, context=context
        )
        return LocalEvidenceProject(
            project_path=str(
                context.resolve_source_relative_path(resolved["project_path"])
            ),
            project_deployment=resolved["project_deployment"],
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
