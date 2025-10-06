"""Tests for Apprise hooks."""

from unittest.mock import Mock, patch

from dagster_apprise.hooks import (
    apprise_failure_hook,
    apprise_success_hook,
)


class TestAppriseFailureHook:
    """Test failure notification hook."""

    def test_hook_with_apprise_resource(self):
        """Test hook execution with apprise resource available."""
        mock_resource = Mock()
        mock_resource.notify_run_status.return_value = True

        mock_context = Mock()
        mock_context.resources = {"apprise": mock_resource}
        mock_context.dagster_run = Mock()
        mock_context.dagster_run.run_id = "test_run_123"
        mock_context.asset_key = "test_asset"

        apprise_failure_hook(mock_context)

        mock_resource.notify_run_status.assert_called_once_with(
            run=mock_context.dagster_run,
            status="FAILURE",
            message="Asset test_asset failed",
        )

    def test_hook_without_apprise_resource(self):
        """Test hook execution when apprise resource is not available."""
        mock_context = Mock()
        mock_context.resources = {}

        with patch("dagster_apprise.hooks.logger") as mock_logger:
            apprise_failure_hook(mock_context)
            mock_logger.warning.assert_called_once_with(
                "Apprise resource not found, skipping notification"
            )

    def test_hook_with_none_apprise_resource(self):
        """Test hook execution when apprise resource is None."""
        mock_context = Mock()
        mock_context.resources = {"apprise": None}

        with patch("dagster_apprise.hooks.logger") as mock_logger:
            apprise_failure_hook(mock_context)
            mock_logger.warning.assert_called_once_with(
                "Apprise resource not found, skipping notification"
            )

    def test_hook_notification_success(self):
        """Test hook when notification is sent successfully."""
        mock_resource = Mock()
        mock_resource.notify_run_status.return_value = True

        mock_context = Mock()
        mock_context.resources = {"apprise": mock_resource}
        mock_context.dagster_run = Mock()
        mock_context.asset_key = "test_asset"

        with patch("dagster_apprise.hooks.logger") as mock_logger:
            apprise_failure_hook(mock_context)
            mock_logger.info.assert_called_once_with(
                "Sent failure notification for asset test_asset"
            )

    def test_hook_notification_failure(self):
        """Test hook when notification fails."""
        mock_resource = Mock()
        mock_resource.notify_run_status.return_value = False

        mock_context = Mock()
        mock_context.resources = {"apprise": mock_resource}
        mock_context.dagster_run = Mock()
        mock_context.asset_key = "test_asset"

        with patch("dagster_apprise.hooks.logger") as mock_logger:
            apprise_failure_hook(mock_context)
            mock_logger.error.assert_called_once_with(
                "Failed to send failure notification for asset test_asset"
            )

    def test_hook_exception_handling(self):
        """Test hook exception handling."""
        mock_context = Mock()
        mock_context.resources = {"apprise": Mock()}
        mock_context.resources["apprise"].notify_run_status.side_effect = Exception(
            "Test error"
        )

        with patch("dagster_apprise.hooks.logger") as mock_logger:
            apprise_failure_hook(mock_context)
            mock_logger.error.assert_called_once_with(
                "Error in apprise failure hook: Test error"
            )


class TestAppriseSuccessHook:
    """Test success notification hook."""

    def test_hook_with_apprise_resource(self):
        """Test hook execution with apprise resource available."""
        mock_resource = Mock()
        mock_resource.notify_run_status.return_value = True

        mock_context = Mock()
        mock_context.resources = {"apprise": mock_resource}
        mock_context.dagster_run = Mock()
        mock_context.dagster_run.run_id = "test_run_123"
        mock_context.asset_key = "test_asset"

        apprise_success_hook(mock_context)

        mock_resource.notify_run_status.assert_called_once_with(
            run=mock_context.dagster_run,
            status="SUCCESS",
            message="Asset test_asset completed successfully",
        )

    def test_hook_without_apprise_resource(self):
        """Test hook execution when apprise resource is not available."""
        mock_context = Mock()
        mock_context.resources = {}

        with patch("dagster_apprise.hooks.logger") as mock_logger:
            apprise_success_hook(mock_context)
            mock_logger.warning.assert_called_once_with(
                "Apprise resource not found, skipping notification"
            )

    def test_hook_notification_success(self):
        """Test hook when notification is sent successfully."""
        mock_resource = Mock()
        mock_resource.notify_run_status.return_value = True

        mock_context = Mock()
        mock_context.resources = {"apprise": mock_resource}
        mock_context.dagster_run = Mock()
        mock_context.asset_key = "test_asset"

        with patch("dagster_apprise.hooks.logger") as mock_logger:
            apprise_success_hook(mock_context)
            mock_logger.info.assert_called_once_with(
                "Sent success notification for asset test_asset"
            )

    def test_hook_notification_failure(self):
        """Test hook when notification fails."""
        mock_resource = Mock()
        mock_resource.notify_run_status.return_value = False

        mock_context = Mock()
        mock_context.resources = {"apprise": mock_resource}
        mock_context.dagster_run = Mock()
        mock_context.asset_key = "test_asset"

        with patch("dagster_apprise.hooks.logger") as mock_logger:
            apprise_success_hook(mock_context)
            mock_logger.error.assert_called_once_with(
                "Failed to send success notification for asset test_asset"
            )

    def test_hook_exception_handling(self):
        """Test hook exception handling."""
        mock_context = Mock()
        mock_context.resources = {"apprise": Mock()}
        mock_context.resources["apprise"].notify_run_status.side_effect = Exception(
            "Test error"
        )

        with patch("dagster_apprise.hooks.logger") as mock_logger:
            apprise_success_hook(mock_context)
            mock_logger.error.assert_called_once_with(
                "Error in apprise success hook: Test error"
            )


class TestCreateAppriseHooks:
    """Test hook creation functionality."""

    def test_create_hooks(self):
        """Test creating apprise hooks."""
        # test individual hook functions instead of the deprecated create_apprise_hooks
        assert callable(apprise_failure_hook)
        assert callable(apprise_success_hook)

    def test_hook_names(self):
        """Test that hooks have correct names."""
        # test that the hook functions exist and are callable
        assert apprise_failure_hook.__name__ == "apprise_failure_hook"
        assert apprise_success_hook.__name__ == "apprise_success_hook"
