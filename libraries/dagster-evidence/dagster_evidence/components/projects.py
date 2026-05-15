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
    ProjectDagsterMetadata,
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
    ) -> tuple[
        Sequence[dg.AssetsDefinition | dg.AssetSpec], Sequence[dg.SensorDefinition]
    ]:
        """Load and build Dagster assets for this Evidence project.

        Args:
            evidence_project_data: Parsed project data containing sources.
            translator: Translator instance for converting Evidence objects to AssetSpecs.

        Returns:
            A tuple containing:
            - A sequence of AssetSpecs and AssetsDefinitions
            - A sequence of SensorDefinitions for source change detection
        """
        raise NotImplementedError()

    @public
    def load_source_assets(
        self,
        evidence_project_data: EvidenceProjectData,
        translator: "DagsterEvidenceTranslator",
    ) -> tuple[list[dg.AssetsDefinition], list[dg.AssetKey], list[dg.SensorDefinition]]:
        """Load source assets using the translator.

        Args:
            evidence_project_data: Parsed project data containing sources.
            translator: Translator instance for converting Evidence objects to assets.

        Returns:
            A tuple containing:
            - List of AssetsDefinition for source queries (may be empty if hiding is enabled)
            - List of AssetKeys for project dependencies (source asset keys or table_deps keys)
            - List of SensorDefinition for source change detection
        """
        source_assets: list[dg.AssetsDefinition] = []
        project_deps: list[dg.AssetKey] = []
        source_sensors: list[dg.SensorDefinition] = []

        for source_group, source_content in evidence_project_data.sources_by_id.items():
            # Use translator to get source class (validates source type is known)
            source_type = source_content.connection.type
            source_class = translator.get_source_class(source_type)
            # Instantiate source to use instance methods for per-source metadata overrides
            source = source_class(source_content)

            # Check if this source should hide its assets (uses instance method for overrides)
            should_hide = source.get_hide_source_asset()

            # Get the source path for resolving relative paths
            source_path = self.get_source_path(source_group)

            # Generate asset specs via translator
            for query in source_content.queries:
                # First create data without extracted_data
                initial_data = EvidenceSourceTranslatorData(
                    source_content=source_content,
                    source_group=source_group,
                    query=query,
                    source_path=source_path,
                )
                # Extract data using source class
                extracted = source_class.extract_data_from_source(initial_data)
                # Create final data with extracted_data
                data = EvidenceSourceTranslatorData(
                    source_content=source_content,
                    source_group=source_group,
                    query=query,
                    extracted_data=extracted,
                    source_path=source_path,
                )

                if should_hide:
                    # Don't create source asset, but add table_deps directly to project_deps
                    table_deps = extracted.get("table_deps", [])
                    for ref in table_deps:
                        if ref.get("table"):
                            project_deps.append(dg.AssetKey([ref["table"]]))
                else:
                    # Create the source asset and add its key to project_deps
                    # For source data, translator returns AssetsDefinition
                    asset = translator.get_asset_spec(data)
                    assert isinstance(asset, dg.AssetsDefinition)
                    source_assets.append(asset)
                    project_deps.append(asset.key)

                    # Check if we should create a sensor for this source (uses instance method)
                    should_create_sensor = source.get_source_sensor_enabled()

                    if should_create_sensor:
                        sensor = source_class.get_source_sensor(data, asset.key)
                        if sensor is not None:
                            source_sensors.append(sensor)

        return source_assets, project_deps, source_sensors

    @public
    @abstractmethod
    def parse_evidence_project_sources(self) -> dict[str, SourceContent]:
        """Parse the sources from the Evidence project.

        Returns:
            Dictionary mapping source folder names to their SourceContent.
        """
        raise NotImplementedError()

    @public
    def get_source_path(self, source_group: str) -> str | None:
        """Get the absolute path to a source directory.

        This method is used to resolve relative paths in source configurations
        (e.g., DuckDB database file paths).

        Args:
            source_group: The source folder name (e.g., "orders_db").

        Returns:
            The absolute path to the source directory, or None if not applicable.
        """
        return None

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

    def get_source_path(self, source_group: str) -> str | None:
        """Get the absolute path to a source directory.

        Args:
            source_group: The source folder name (e.g., "orders_db").

        Returns:
            The absolute path to the source directory.
        """
        return str(Path(self.project_path) / "sources" / source_group)

    def _parse_project_dagster_metadata(self) -> ProjectDagsterMetadata:
        """Parse Dagster metadata from evidence.config.yaml.

        Returns:
            ProjectDagsterMetadata instance with parsed values,
            or default metadata if config doesn't exist or lacks dagster section.
        """
        config_path = Path(self.project_path) / "evidence.config.yaml"
        if not config_path.exists():
            return ProjectDagsterMetadata()

        with open(config_path, "r") as f:
            config = yaml.safe_load(f) or {}

        meta = config.get("meta", {})
        dagster_meta = meta.get("dagster", {})
        return ProjectDagsterMetadata(
            group_name=dagster_meta.get("group_name"),
        )

    def load_evidence_project_assets(
        self,
        evidence_project_data: EvidenceProjectData,
        translator: "DagsterEvidenceTranslator",
    ) -> tuple[
        Sequence[dg.AssetsDefinition | dg.AssetSpec], Sequence[dg.SensorDefinition]
    ]:
        # Get source assets, project dependencies, and sensors via translator
        source_assets, source_deps, source_sensors = self.load_source_assets(
            evidence_project_data, translator
        )

        # Parse project-level Dagster metadata from evidence.config.yaml
        project_metadata = self._parse_project_dagster_metadata()

        # Get project asset spec via translator
        project_data = EvidenceProjectTranslatorData(
            project_name=self.get_evidence_project_name(),
            sources_by_id=evidence_project_data.sources_by_id,
            source_deps=source_deps,
            dagster_metadata=project_metadata,
        )
        project_spec = translator.get_asset_spec(project_data)
        # For project data, translator returns AssetSpec
        assert isinstance(project_spec, dg.AssetSpec)

        # Create automation condition that triggers when any direct dependency is updated
        # Source assets now have their own automation conditions that cascade updates
        automation_condition = dg.AutomationCondition.any_deps_match(
            dg.AutomationCondition.newly_updated()
        )

        # Create the executable asset using the translated spec
        # Use the returned source_deps which may contain table_deps directly
        # when source asset hiding is enabled
        @dg.asset(
            key=project_spec.key,
            kinds=project_spec.kinds or {"evidence"},
            deps=source_deps,
            automation_condition=automation_condition,
            group_name=project_spec.group_name,
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

        return list(source_assets) + [build_and_deploy_evidence_project], source_sensors

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

    **Coming Soon** - This project type is planned but not yet implemented.

    This project type will connect to Evidence Studio to fetch project
    configuration and sources from the cloud, enabling seamless integration
    between Dagster pipelines and Evidence Studio hosted dashboards.

    Planned Features:
        - Fetch source configurations from Evidence Studio API
        - Sync local data sources with cloud project
        - Trigger cloud builds from Dagster
        - Monitor deployment status

    Attributes:
        evidence_studio_url: URL of the Evidence Studio workspace.

    Example:

        .. code-block:: yaml

            evidence_project:
              project_type: evidence_studio
              evidence_studio_url: https://evidence.studio/my-workspace

    Note:
        If you need Evidence Studio integration, please open an issue on GitHub
        to help prioritize this feature.
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
    ) -> tuple[
        Sequence[dg.AssetsDefinition | dg.AssetSpec], Sequence[dg.SensorDefinition]
    ]:
        raise NotImplementedError()


@beta
@public
class EvidenceStudioProjectArgs(dg.Model, dg.Resolvable):
    """Arguments for configuring an Evidence Studio project.

    **Coming Soon** - This project type is planned but not yet implemented.

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

    Note:
        Use the ``local`` project type with a git clone of your Evidence project
        as a workaround until Evidence Studio integration is implemented.
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
