"""Tests for Evidence project deployment classes."""

import os
from unittest.mock import MagicMock

import pytest

from dagster_evidence.components.deployments import (
    BaseEvidenceProjectDeployment,
    CustomEvidenceProjectDeployment,
    CustomEvidenceProjectDeploymentArgs,
    EvidenceProjectNetlifyDeployment,
    EvidenceProjectNetlifyDeploymentArgs,
    GithubPagesEvidenceProjectDeployment,
    GithubPagesEvidenceProjectDeploymentArgs,
    resolve_evidence_project_deployment,
)


class TestDeploymentArgs:
    """Tests for deployment argument classes."""

    def test_custom_deployment_args_type(self):
        """Verify CustomEvidenceProjectDeploymentArgs has correct type."""
        args = CustomEvidenceProjectDeploymentArgs(deploy_command="npm run deploy")
        assert args.type == "custom"
        assert args.deploy_command == "npm run deploy"

    def test_github_pages_deployment_args_defaults(self):
        """Verify GithubPagesEvidenceProjectDeploymentArgs has correct defaults."""
        args = GithubPagesEvidenceProjectDeploymentArgs(github_repo="user/repo")
        assert args.type == "github_pages"
        assert args.github_repo == "user/repo"
        assert args.branch == "gh-pages"
        assert args.github_token is None

    def test_github_pages_deployment_args_custom_branch(self):
        """Verify GithubPagesEvidenceProjectDeploymentArgs accepts custom branch."""
        args = GithubPagesEvidenceProjectDeploymentArgs(
            github_repo="user/repo",
            branch="main",
            github_token="token123",
        )
        assert args.branch == "main"
        assert args.github_token == "token123"

    def test_netlify_deployment_args(self):
        """Verify EvidenceProjectNetlifyDeploymentArgs parsing."""
        args = EvidenceProjectNetlifyDeploymentArgs(
            netlify_project_url="https://my-project.netlify.app"
        )
        assert args.type == "netlify"
        assert args.netlify_project_url == "https://my-project.netlify.app"


class TestDeploymentClasses:
    """Tests for deployment class instances."""

    def test_custom_deployment_creation(self):
        """Verify CustomEvidenceProjectDeployment can be created."""
        deployment = CustomEvidenceProjectDeployment(deploy_command="echo deploy")
        assert deployment.deploy_command == "echo deploy"
        assert isinstance(deployment, BaseEvidenceProjectDeployment)

    def test_github_pages_deployment_creation(self, github_token, github_repo):
        """Verify GithubPagesEvidenceProjectDeployment can be created."""
        deployment = GithubPagesEvidenceProjectDeployment(
            github_repo=github_repo,
            branch="gh-pages",
            github_token=github_token,
        )
        assert deployment.github_repo == github_repo
        assert deployment.branch == "gh-pages"
        assert deployment.github_token == github_token
        assert isinstance(deployment, BaseEvidenceProjectDeployment)

    def test_github_pages_default_branch(self, github_token, github_repo):
        """Verify GithubPagesEvidenceProjectDeployment uses gh-pages as default."""
        deployment = GithubPagesEvidenceProjectDeployment(
            github_repo=github_repo,
            github_token=github_token,
        )
        assert deployment.branch == "gh-pages"

    def test_netlify_deployment_creation(self):
        """Verify EvidenceProjectNetlifyDeployment can be created."""
        deployment = EvidenceProjectNetlifyDeployment(
            netlify_project_url="https://my-project.netlify.app"
        )
        assert deployment.netlify_project_url == "https://my-project.netlify.app"
        assert isinstance(deployment, BaseEvidenceProjectDeployment)


class TestResolveDeployment:
    """Tests for resolve_evidence_project_deployment function.

    Note: The resolver function uses resolve_fields which is designed for
    Dagster's component resolution context. Testing type detection only.
    """

    def test_resolve_unknown_deployment_raises(self):
        """Verify resolve_evidence_project_deployment raises for unknown types."""
        mock_context = MagicMock()
        # Create a mock model with unknown type
        mock_model = MagicMock()
        mock_model.type = "unknown_provider"

        with pytest.raises(NotImplementedError, match="Unknown deployment type"):
            resolve_evidence_project_deployment(mock_context, mock_model)

    def test_github_pages_args_default_branch(self):
        """Verify GithubPagesEvidenceProjectDeploymentArgs has correct default branch."""
        args = GithubPagesEvidenceProjectDeploymentArgs(github_repo="user/repo")
        assert args.branch == "gh-pages"

    def test_github_token_env_fallback(self, env_with_github_token, github_token):
        """Verify GITHUB_TOKEN env var can be used as fallback."""
        assert os.environ.get("GITHUB_TOKEN") == github_token


class TestGitHubPagesDeployment:
    """Tests for GitHub Pages deployment execution."""

    def test_deploy_creates_nojekyll(
        self, mock_git_repo, mock_asset_context, mock_pipes_subprocess_client, tmp_path
    ):
        """Verify deployment creates .nojekyll file."""
        build_path = tmp_path / "build"
        build_path.mkdir()

        deployment = GithubPagesEvidenceProjectDeployment(
            github_repo="user/repo",
            branch="gh-pages",
            github_token="token123",
        )

        # Execute the method (no longer a generator)
        deployment.deploy_evidence_project(
            evidence_project_build_path=str(build_path),
            context=mock_asset_context,
            pipes_subprocess_client=mock_pipes_subprocess_client,
        )

        # Verify .nojekyll was created
        assert (build_path / ".nojekyll").exists()

    def test_deploy_initializes_git_repo(
        self, mock_git_repo, mock_asset_context, mock_pipes_subprocess_client, tmp_path
    ):
        """Verify deployment initializes git repository."""
        build_path = tmp_path / "build"
        build_path.mkdir()

        deployment = GithubPagesEvidenceProjectDeployment(
            github_repo="user/repo",
            branch="gh-pages",
            github_token="token123",
        )

        deployment.deploy_evidence_project(
            evidence_project_build_path=str(build_path),
            context=mock_asset_context,
            pipes_subprocess_client=mock_pipes_subprocess_client,
        )

        # Verify git init was called
        mock_git_repo.init.assert_called_once_with(
            str(build_path), initial_branch="main"
        )

    def test_deploy_creates_remote_with_token(
        self, mock_git_repo, mock_asset_context, mock_pipes_subprocess_client, tmp_path
    ):
        """Verify deployment creates remote with token in URL."""
        build_path = tmp_path / "build"
        build_path.mkdir()

        deployment = GithubPagesEvidenceProjectDeployment(
            github_repo="user/repo",
            branch="gh-pages",
            github_token="secret_token",
        )

        mock_repo_instance = mock_git_repo.init.return_value

        deployment.deploy_evidence_project(
            evidence_project_build_path=str(build_path),
            context=mock_asset_context,
            pipes_subprocess_client=mock_pipes_subprocess_client,
        )

        # Verify remote was created with correct URL containing token
        mock_repo_instance.create_remote.assert_called_once()
        call_args = mock_repo_instance.create_remote.call_args
        assert call_args[0][0] == "origin"
        assert "secret_token" in call_args[0][1]
        assert "user/repo" in call_args[0][1]

    def test_deploy_force_pushes(
        self, mock_git_repo, mock_asset_context, mock_pipes_subprocess_client, tmp_path
    ):
        """Verify deployment force pushes to branch."""
        build_path = tmp_path / "build"
        build_path.mkdir()

        deployment = GithubPagesEvidenceProjectDeployment(
            github_repo="user/repo",
            branch="gh-pages",
            github_token="token123",
        )

        mock_repo_instance = mock_git_repo.init.return_value
        mock_remote = mock_repo_instance.create_remote.return_value

        deployment.deploy_evidence_project(
            evidence_project_build_path=str(build_path),
            context=mock_asset_context,
            pipes_subprocess_client=mock_pipes_subprocess_client,
        )

        # Verify force push was called
        mock_remote.push.assert_called_once_with(
            refspec="gh-pages:gh-pages", force=True
        )

    def test_deploy_raises_if_build_path_missing(
        self, mock_asset_context, mock_pipes_subprocess_client
    ):
        """Verify deployment raises if build directory doesn't exist."""
        deployment = GithubPagesEvidenceProjectDeployment(
            github_repo="user/repo",
            branch="gh-pages",
            github_token="token123",
        )

        with pytest.raises(FileNotFoundError, match="Build directory not found"):
            deployment.deploy_evidence_project(
                evidence_project_build_path="/nonexistent/path",
                context=mock_asset_context,
                pipes_subprocess_client=mock_pipes_subprocess_client,
            )


class TestCustomDeployment:
    """Tests for custom deployment execution."""

    def test_custom_deploy_runs_command(
        self, mock_asset_context, mock_pipes_subprocess_client, tmp_path
    ):
        """Verify custom deployment runs the deploy command."""
        build_path = tmp_path / "build"
        build_path.mkdir()

        deployment = CustomEvidenceProjectDeployment(deploy_command="./deploy.sh")

        deployment.deploy_evidence_project(
            evidence_project_build_path=str(build_path),
            context=mock_asset_context,
            pipes_subprocess_client=mock_pipes_subprocess_client,
        )

        # Verify pipes client was called with command
        mock_pipes_subprocess_client.run.assert_called_once()
        call_kwargs = mock_pipes_subprocess_client.run.call_args[1]
        assert call_kwargs["command"] == "./deploy.sh"
        assert call_kwargs["cwd"] == str(build_path)
