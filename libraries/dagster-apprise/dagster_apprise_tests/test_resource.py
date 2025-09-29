"""Tests for AppriseResource."""

from unittest.mock import Mock, patch

import dagster as dg

from dagster_apprise.resource import AppriseConfig, AppriseResource


class TestAppriseResource:
    """Test cases for AppriseResource."""

    def test_init_with_defaults(self):
        """Test resource initialization with default values."""
        resource = AppriseResource()
        assert resource.config.urls == []
        assert resource.config.config_file is None
        assert resource.config.base_url is None
        assert resource.config.title_prefix == "Dagster"

    def test_init_with_config(self):
        """Test resource initialization with custom config."""
        config = AppriseConfig(
            urls=["pover://user@token"],
            base_url="http://localhost:3000",
            title_prefix="Test",
        )
        resource = AppriseResource(config=config)
        assert resource.config.urls == ["pover://user@token"]
        assert resource.config.base_url == "http://localhost:3000"
        assert resource.config.title_prefix == "Test"

    @patch("dagster_apprise.resource.apprise.Apprise")
    def test_notify_success(self, mock_apprise_class):
        """Test successful notification."""
        mock_apprise = Mock()
        mock_apprise.notify.return_value = True
        mock_apprise_class.return_value = mock_apprise

        resource = AppriseResource()
        result = resource.notify("Test message", "Test title")

        assert result is True
        mock_apprise.notify.assert_called_once()

    @patch("dagster_apprise.resource.apprise.Apprise")
    def test_notify_failure(self, mock_apprise_class):
        """Test failed notification."""
        mock_apprise = Mock()
        mock_apprise.notify.return_value = False
        mock_apprise_class.return_value = mock_apprise

        resource = AppriseResource()
        result = resource.notify("Test message", "Test title")

        assert result is False
        mock_apprise.notify.assert_called_once()

    @patch("dagster_apprise.resource.apprise.Apprise")
    def test_notify_run_status_success(self, mock_apprise_class):
        """Test run status notification for success."""
        mock_apprise = Mock()
        mock_apprise.notify.return_value = True
        mock_apprise_class.return_value = mock_apprise

        config = AppriseConfig(base_url="http://localhost:3000")
        resource = AppriseResource(config=config)

        run = dg.DagsterRun(job_name="test_job", run_id="test_run_id")
        result = resource.notify_run_status(run, "SUCCESS", "Test message")

        assert result is True
        mock_apprise.notify.assert_called_once()

        # check the notification content
        call_args = mock_apprise.notify.call_args
        assert "test_job" in call_args[1]["body"]
        assert "test_run_id" in call_args[1]["body"]
        assert "SUCCESS" in call_args[1]["body"]
        assert "http://localhost:3000/runs/test_run_id" in call_args[1]["body"]

    @patch("dagster_apprise.resource.apprise.Apprise")
    def test_notify_run_status_failure(self, mock_apprise_class):
        """Test run status notification for failure."""
        mock_apprise = Mock()
        mock_apprise.notify.return_value = True
        mock_apprise_class.return_value = mock_apprise

        resource = AppriseResource()

        run = dg.DagsterRun(job_name="test_job", run_id="test_run_id")
        result = resource.notify_run_status(run, "FAILURE")

        assert result is True
        mock_apprise.notify.assert_called_once()

        # check the notification content
        call_args = mock_apprise.notify.call_args
        assert "test_job" in call_args[1]["body"]
        assert "test_run_id" in call_args[1]["body"]
        assert "FAILURE" in call_args[1]["body"]

    def test_apprise_property_initialization(self):
        """Test that apprise property initializes correctly."""
        with patch("dagster_apprise.resource.apprise.Apprise") as mock_apprise_class:
            mock_apprise = Mock()
            mock_apprise_class.return_value = mock_apprise

            resource = AppriseResource()
            apprise_instance = resource.apprise

            assert apprise_instance is mock_apprise
            mock_apprise_class.assert_called_once()

    def test_apprise_property_caching(self):
        """Test that apprise property is cached."""
        with patch("dagster_apprise.resource.apprise.Apprise") as mock_apprise_class:
            mock_apprise = Mock()
            mock_apprise_class.return_value = mock_apprise

            resource = AppriseResource()
            apprise1 = resource.apprise
            apprise2 = resource.apprise

            assert apprise1 is apprise2
            mock_apprise_class.assert_called_once()
