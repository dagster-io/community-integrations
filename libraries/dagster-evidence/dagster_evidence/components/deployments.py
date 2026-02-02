"""Deployment classes for Evidence projects.

This module provides deployment targets for Evidence projects, including
GitHub Pages, Netlify, and custom deployment commands.
"""

import os
from abc import abstractmethod
from pathlib import Path
from typing import Literal, Optional

import dagster as dg
import yaml
from dagster._annotations import beta, public
from dagster.components.resolved.base import resolve_fields
from pydantic import BaseModel, Field


@beta
@public
class BaseEvidenceProjectDeployment(dg.ConfigurableResource):
    """Base class for Evidence project deployment configurations.

    Subclass this to implement custom deployment targets.

    Example:

        Implementing a custom S3 deployment:

        .. code-block:: python

            from dagster_evidence.components.deployments import (
                BaseEvidenceProjectDeployment,
            )
            import dagster as dg

            class S3EvidenceProjectDeployment(BaseEvidenceProjectDeployment):
                s3_bucket: str
                s3_prefix: str = ""

                def deploy_evidence_project(
                    self,
                    evidence_project_build_path: str,
                    context: dg.AssetExecutionContext,
                    pipes_subprocess_client: dg.PipesSubprocessClient,
                    env: dict[str, str] | None = None,
                ) -> None:
                    context.log.info(
                        f"Deploying to s3://{self.s3_bucket}/{self.s3_prefix}"
                    )
                    import os
                    pipes_subprocess_client.run(
                        command=[
                            "aws", "s3", "sync",
                            evidence_project_build_path,
                            f"s3://{self.s3_bucket}/{self.s3_prefix}",
                        ],
                        cwd=evidence_project_build_path,
                        context=context,
                        env=env or os.environ.copy(),
                    )
    """

    @public
    @abstractmethod
    def deploy_evidence_project(
        self,
        evidence_project_build_path: str,
        context: dg.AssetExecutionContext,
        pipes_subprocess_client: dg.PipesSubprocessClient,
        env: Optional[dict[str, str]] = None,
    ) -> None:
        """Deploy the built Evidence project to the target destination.

        Args:
            evidence_project_build_path: Path to the built Evidence project output.
            context: The Dagster asset execution context.
            pipes_subprocess_client: Client for running subprocess commands.
            env: Optional environment variables for the deployment process.
        """
        raise NotImplementedError()

    @public
    def get_base_path(self, project_path: str) -> str:
        """Get the build output path for this deployment.

        Args:
            project_path: Path to the Evidence project directory.

        Returns:
            The build output path. Default is 'build'.
        """
        return "build"


@beta
@public
class CustomEvidenceProjectDeployment(BaseEvidenceProjectDeployment):
    """Custom deployment using a shell command.

    Use this deployment type when you need to run a custom deployment script
    or command that is not covered by the built-in deployment types.

    Attributes:
        deploy_command: The shell command to run for deployment.

    Example:

        .. code-block:: yaml

            project_deployment:
              type: custom
              deploy_command: "rsync -avz ./ user@server:/var/www/dashboard/"

        Or using a deployment script:

        .. code-block:: yaml

            project_deployment:
              type: custom
              deploy_command: "./deploy.sh production"
    """

    deploy_command: str

    def deploy_evidence_project(
        self,
        evidence_project_build_path: str,
        context: dg.AssetExecutionContext,
        pipes_subprocess_client: dg.PipesSubprocessClient,
        env: Optional[dict[str, str]] = None,
    ) -> None:
        if self.deploy_command is not None:
            context.log.info(f"Running deploy command: {self.deploy_command}")
            pipes_subprocess_client.run(
                command=self.deploy_command,
                cwd=evidence_project_build_path,
                context=context,
                env=env or os.environ.copy(),
            )


@beta
@public
class CustomEvidenceProjectDeploymentArgs(dg.Model, dg.Resolvable):
    """Arguments for configuring a custom deployment command.

    Example:

        .. code-block:: yaml

            project_deployment:
              type: custom
              deploy_command: "aws s3 sync build/ s3://my-bucket/dashboard/"

    Attributes:
        type: Must be "custom" to use this deployment type.
        deploy_command: Shell command to execute for deployment.
    """

    type: Literal["custom"] = Field(
        default="custom",
        description="Deployment type identifier.",
    )
    deploy_command: str = Field(
        ...,
        description="Custom command to run for deployment.",
    )


@beta
@public
class GithubPagesEvidenceProjectDeployment(BaseEvidenceProjectDeployment):
    """Deploy Evidence project to GitHub Pages.

    This deployment type pushes the built Evidence project to a GitHub Pages
    branch, enabling automatic hosting via GitHub.

    Requirements:
        - evidence.config.yaml must have deployment.basePath configured
        - A GitHub token with repo write access

    Attributes:
        github_repo: Repository in "owner/repo" format.
        branch: Target branch for deployment (default: "gh-pages").
        github_token: GitHub token for authentication.

    Example:

        .. code-block:: yaml

            project_deployment:
              type: github_pages
              github_repo: my-org/analytics-dashboard
              branch: gh-pages
              github_token: ${GITHUB_TOKEN}

        Your evidence.config.yaml should include:

        .. code-block:: yaml

            deployment:
              basePath: /analytics-dashboard
    """

    github_repo: str  # e.g., "milicevica23/food-tracking-serving"
    branch: str = "gh-pages"
    github_token: str  # Token resolved at component load time (from config or GITHUB_TOKEN env var)

    def get_base_path(self, project_path: str) -> str:
        """Get the build output path from evidence.config.yaml for GitHub Pages.

        Args:
            project_path: Path to the Evidence project directory.

        Returns:
            The build path in format 'build/{basePath}'.

        Raises:
            FileNotFoundError: If evidence.config.yaml is not found.
            ValueError: If basePath is not configured in deployment section.
        """
        config_path = Path(project_path) / "evidence.config.yaml"
        if not config_path.exists():
            raise FileNotFoundError(
                f"evidence.config.yaml not found at {config_path}. "
                "See: https://docs.evidence.dev/deployment/self-host/github-pages/"
            )

        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        deployment_config = config.get("deployment", {})
        if not deployment_config:
            raise ValueError(
                "Missing 'deployment' section in evidence.config.yaml. "
                "GitHub Pages deployment requires 'deployment.basePath' to be configured. "
                "See: https://docs.evidence.dev/deployment/self-host/github-pages/"
            )

        base_path = deployment_config.get("basePath")
        if not base_path:
            raise ValueError(
                "Missing 'basePath' in deployment section of evidence.config.yaml. "
                "GitHub Pages deployment requires 'deployment.basePath' to be configured. "
                "See: https://docs.evidence.dev/deployment/self-host/github-pages/"
            )

        # Remove leading slash and prepend with build/
        return f"build/{base_path.lstrip('/')}"

    def deploy_evidence_project(
        self,
        evidence_project_build_path: str,
        context: dg.AssetExecutionContext,
        pipes_subprocess_client: dg.PipesSubprocessClient,
        env: Optional[dict[str, str]] = None,
    ) -> None:
        """Deploy Evidence project build to GitHub Pages using GitPython."""
        from datetime import datetime, timezone

        try:
            from git import Repo
        except ImportError:
            raise ImportError(
                "GitPython is required for GitHub Pages deployment. "
                "Install it with: pip install dagster-evidence[github-pages]"
            ) from None

        if not os.path.isdir(evidence_project_build_path):
            raise FileNotFoundError(
                f"Build directory not found: {evidence_project_build_path}"
            )

        context.log.info("Deploying to GitHub Pages...")
        context.log.info(f"  Repo: {self.github_repo}")
        context.log.info(f"  Branch: {self.branch}")
        context.log.info(f"  Build path: {evidence_project_build_path}")

        # Initialize git repo directly in the build directory
        repo = Repo.init(evidence_project_build_path, initial_branch="main")

        # Add remote with token auth (token is never logged)
        remote_url = f"https://x-access-token:{self.github_token}@github.com/{self.github_repo}.git"
        origin = repo.create_remote("origin", remote_url)

        # Disable Jekyll processing (required for files/folders starting with _)
        Path(evidence_project_build_path, ".nojekyll").touch()

        # Configure git user
        repo.config_writer().set_value(
            "user", "name", "Dagster Evidence Deploy"
        ).release()
        repo.config_writer().set_value("user", "email", "dagster@localhost").release()

        # Add all files
        repo.index.add("*")
        repo.index.add(".nojekyll")

        # Commit
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H:%M:%S")
        repo.index.commit(f"Deploy Evidence dashboard {timestamp}")

        # Create and checkout branch
        new_branch = repo.create_head(self.branch)
        new_branch.checkout()

        # Force push
        origin.push(refspec=f"{self.branch}:{self.branch}", force=True)

        context.log.info("Deployment complete!")


@beta
@public
class GithubPagesEvidenceProjectDeploymentArgs(dg.Model, dg.Resolvable):
    """Arguments for deploying Evidence project to GitHub Pages.

    Example:

        .. code-block:: yaml

            project_deployment:
              type: github_pages
              github_repo: owner/repo-name
              branch: gh-pages

        With explicit token (not recommended - prefer env var):

        .. code-block:: yaml

            project_deployment:
              type: github_pages
              github_repo: owner/repo-name
              github_token: ghp_xxxxxxxxxxxx

    Attributes:
        type: Must be "github_pages" to use this deployment type.
        github_repo: Repository in "owner/repo" format.
        branch: Target branch (default: "gh-pages").
        github_token: GitHub token. Falls back to GITHUB_TOKEN env var if not provided.
    """

    type: Literal["github_pages"] = Field(
        default="github_pages",
        description="Deployment type identifier.",
    )
    github_repo: str = Field(
        ...,
        description="GitHub repository in format 'owner/repo' (e.g., 'milicevica23/my-dashboard').",
    )
    branch: str = Field(
        default="gh-pages",
        description="Branch to deploy to (default: gh-pages).",
    )
    github_token: Optional[str] = Field(
        default=None,
        description="GitHub token for authentication. If not provided, falls back to GITHUB_TOKEN env var.",
    )


@beta
@public
class EvidenceProjectNetlifyDeployment(BaseEvidenceProjectDeployment):
    """Deploy Evidence project to Netlify.

    **Coming Soon** - This deployment type is planned but not yet implemented.

    This deployment type will push the built Evidence project to Netlify,
    enabling automatic hosting and CDN distribution.

    Planned Features:
        - Deploy via Netlify CLI or API
        - Support for deploy previews
        - Environment variable configuration
        - Build hooks integration

    Attributes:
        netlify_project_url: URL of the Netlify project.

    Example:

        .. code-block:: yaml

            project_deployment:
              type: netlify
              netlify_project_url: https://app.netlify.com/sites/my-dashboard
              netlify_auth_token: ${NETLIFY_AUTH_TOKEN}

    Note:
        If you need Netlify deployment support, please open an issue on GitHub
        to help prioritize this feature. In the meantime, you can use the
        ``custom`` deployment type with the Netlify CLI.
    """

    netlify_project_url: str

    def deploy_evidence_project(
        self,
        evidence_project_build_path: str,
        context: dg.AssetExecutionContext,
        pipes_subprocess_client: dg.PipesSubprocessClient,
        env: Optional[dict[str, str]] = None,
    ) -> None:
        raise NotImplementedError()


@beta
@public
class EvidenceProjectNetlifyDeploymentArgs(dg.Model, dg.Resolvable):
    """Arguments for deploying Evidence project to Netlify.

    **Coming Soon** - This deployment type is planned but not yet implemented.

    Example:

        .. code-block:: yaml

            project_deployment:
              type: netlify
              netlify_project_url: https://app.netlify.com/sites/my-dashboard

    Attributes:
        type: Must be "netlify" to use this deployment type.
        netlify_project_url: URL of the Netlify project.

    Note:
        Use the ``custom`` deployment type with Netlify CLI as a workaround:

        .. code-block:: yaml

            project_deployment:
              type: custom
              deploy_command: "netlify deploy --prod --dir=."
    """

    type: Literal["netlify"] = Field(
        default="netlify",
        description="Deployment type identifier.",
    )
    netlify_project_url: str = Field(
        ...,
        description="Netlify project URL.",
    )


@public
def resolve_evidence_project_deployment(
    context: dg.ResolutionContext, model: BaseModel
) -> BaseEvidenceProjectDeployment:
    """Resolve deployment configuration to a concrete deployment instance.

    This function is used internally by the component resolution system
    to convert YAML configuration into deployment instances.

    Args:
        context: The resolution context.
        model: The parsed configuration model.

    Returns:
        A BaseEvidenceProjectDeployment instance.

    Raises:
        NotImplementedError: If an unknown deployment type is specified.
        ValueError: If required configuration is missing (e.g., github_token).
    """
    # First, check which type we're dealing with
    deployment_type = (
        model.get("type", "custom")
        if isinstance(model, dict)
        else getattr(model, "type", "custom")
    )

    if deployment_type == "github_pages":
        resolved = resolve_fields(
            model=model,
            resolved_cls=GithubPagesEvidenceProjectDeploymentArgs,
            context=context,
        )
        # Resolve token: use provided value or fall back to env var
        github_token = resolved.get("github_token") or os.environ.get("GITHUB_TOKEN")
        if not github_token:
            raise ValueError(
                "GitHub token is required for github_pages deployment. "
                "Provide 'github_token' in config or set GITHUB_TOKEN environment variable."
            )
        return GithubPagesEvidenceProjectDeployment(
            github_repo=resolved["github_repo"],
            branch=resolved.get(
                "branch",
                GithubPagesEvidenceProjectDeploymentArgs.model_fields["branch"].default,
            ),
            github_token=github_token,
        )
    elif deployment_type == "netlify":
        resolved = resolve_fields(
            model=model,
            resolved_cls=EvidenceProjectNetlifyDeploymentArgs,
            context=context,
        )
        return EvidenceProjectNetlifyDeployment(
            netlify_project_url=resolved["netlify_project_url"],
        )
    elif deployment_type == "custom":
        resolved = resolve_fields(
            model=model,
            resolved_cls=CustomEvidenceProjectDeploymentArgs,
            context=context,
        )
        return CustomEvidenceProjectDeployment(
            deploy_command=resolved["deploy_command"],
        )
    else:
        raise NotImplementedError(f"Unknown deployment type: {deployment_type}")
