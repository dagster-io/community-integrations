"""Deployment classes for Evidence projects."""

import os
from pathlib import Path
from typing import Literal, Optional

import dagster as dg
from pydantic import BaseModel, Field
from dagster.components.resolved.base import resolve_fields
from abc import abstractmethod


class BaseEvidenceProjectDeployment(dg.ConfigurableResource):

    @abstractmethod
    def deploy_evidence_project(
        self,
        evidence_project_build_path: str,
        context: dg.AssetExecutionContext,
        pipes_subprocess_client: dg.PipesSubprocessClient,
        env: Optional[dict] = None,
    ):
        raise NotImplementedError()


class CustomEvidenceProjectDeployment(BaseEvidenceProjectDeployment):
    deploy_command: str

    def deploy_evidence_project(
        self,
        evidence_project_build_path: str,
        context: dg.AssetExecutionContext,
        pipes_subprocess_client: dg.PipesSubprocessClient,
        env: Optional[dict] = None,
    ):
        if self.deploy_command is not None:
            context.log.info(f"Running deploy command: {self.deploy_command}")
            pipes_subprocess_client.run(
                command=self.deploy_command,
                cwd=evidence_project_build_path,
                context=context,
                env=env or os.environ.copy(),
            )


class CustomEvidenceProjectDeploymentArgs(dg.Model, dg.Resolvable):
    """Arguments for configuring a custom deployment command."""

    type: Literal["custom"] = Field(
        default="custom",
        description="Deployment type identifier.",
    )
    deploy_command: str = Field(
        ...,
        description="Custom command to run for deployment.",
    )


class GithubPagesEvidenceProjectDeployment(BaseEvidenceProjectDeployment):
    github_repo: str  # e.g., "milicevica23/food-tracking-serving"
    branch: str = "gh-pages"
    github_token: str  # Token resolved at component load time (from config or GITHUB_TOKEN env var)

    def deploy_evidence_project(
        self,
        evidence_project_build_path: str,
        context: dg.AssetExecutionContext,
        pipes_subprocess_client: dg.PipesSubprocessClient,
        env: Optional[dict] = None,
    ):
        """Deploy Evidence project build to GitHub Pages using GitPython."""
        from datetime import datetime, timezone
        from git import Repo

        if not os.path.isdir(evidence_project_build_path):
            raise FileNotFoundError(f"Build directory not found: {evidence_project_build_path}")

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
        repo.config_writer().set_value("user", "name", "Dagster Evidence Deploy").release()
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


class GithubPagesEvidenceProjectDeploymentArgs(dg.Model, dg.Resolvable):
    """Arguments for deploying Evidence project to GitHub Pages."""

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


class EvidenceProjectNetlifyDeployment(BaseEvidenceProjectDeployment):
    netlify_project_url: str

    def deploy_evidence_project(
        self,
        evidence_project_build_path: str,
        context: dg.AssetExecutionContext,
        pipes_subprocess_client: dg.PipesSubprocessClient,
        env: Optional[dict] = None,
    ):
        raise NotImplementedError()
        yield from ()  # Make it a generator for consistency


class EvidenceProjectNetlifyDeploymentArgs(dg.Model, dg.Resolvable):
    """Arguments for deploying Evidence project to Netlify."""

    type: Literal["netlify"] = Field(
        default="netlify",
        description="Deployment type identifier.",
    )
    netlify_project_url: str = Field(
        ...,
        description="Netlify project URL.",
    )


def resolve_evidence_project_deployment(
    context: dg.ResolutionContext, model: BaseModel
) -> BaseEvidenceProjectDeployment:
    """Resolve deployment configuration to a deployment instance."""
    # First, check which type we're dealing with
    deployment_type = (
        model.get("type", "custom") if isinstance(model, dict) else getattr(model, "type", "custom")
    )

    if deployment_type == "github_pages":
        resolved = resolve_fields(
            model=model, resolved_cls=GithubPagesEvidenceProjectDeploymentArgs, context=context
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
            branch=resolved.get("branch", GithubPagesEvidenceProjectDeploymentArgs.model_fields["branch"].default),
            github_token=github_token,
        )
    elif deployment_type == "netlify":
        resolved = resolve_fields(
            model=model, resolved_cls=EvidenceProjectNetlifyDeploymentArgs, context=context
        )
        return EvidenceProjectNetlifyDeployment(
            netlify_project_url=resolved["netlify_project_url"],
        )
    elif deployment_type == "custom":
        resolved = resolve_fields(
            model=model, resolved_cls=CustomEvidenceProjectDeploymentArgs, context=context
        )
        return CustomEvidenceProjectDeployment(
            deploy_command=resolved["deploy_command"],
        )
    else:
        raise NotImplementedError(f"Unknown deployment type: {deployment_type}")