"""Tests for edge cases and error conditions."""

from unittest.mock import Mock, patch

import dagster as dg

from dagster_apprise.components import AppriseNotificationsConfig, apprise_notifications
from dagster_apprise.resource import AppriseConfig, AppriseResource


class TestComponentEdgeCases:
    """Test edge cases in component functionality."""

    def test_unknown_event_type(self):
        """Test handling of unknown event types."""
        config = AppriseNotificationsConfig(
            urls=["pover://user@token"], events=["UNKNOWN_EVENT"]
        )

        defs = apprise_notifications(config)

        # unknown events do not create any sensors
        sensors = defs.sensors
        sensors_list = list(sensors) if sensors is not None else []
        assert len(sensors_list) == 0

    def test_empty_events_list(self):
        """Test handling of empty events list."""
        config = AppriseNotificationsConfig(urls=["pover://user@token"], events=[])

        defs = apprise_notifications(config)

        # empty events list does not create any sensors
        sensors = defs.sensors
        sensors_list = list(sensors) if sensors is not None else []
        assert len(sensors_list) == 0

    def test_sensor_notification_failure(self):
        """Test sensor behavior when notification fails."""
        config = AppriseNotificationsConfig(
            urls=["pover://user@token"], events=["FAILURE"], include_jobs=["test_*"]
        )

        defs = apprise_notifications(config)
        sensors = defs.sensors
        assert sensors is not None
        failure_sensor = next(s for s in sensors if "failure" in s.name.lower())

        # Create a test run and event
        run = dg.DagsterRun(job_name="test_job", run_id="test_run_123")
        from dagster._core.events import DagsterEvent, DagsterEventType

        event = DagsterEvent(
            event_type_value=DagsterEventType.PIPELINE_FAILURE.value,
            job_name="test_job",
            event_specific_data=None,
        )

        # recorder that simulates notification failure
        class FailingRecorder:
            def __init__(self):
                self.calls = []

            def notify_run_status(self, run, status, message=None):
                self.calls.append({"run": run, "status": status, "message": message})
                return False  # Simulate failure

        rec = FailingRecorder()

        ctx = dg.build_run_status_sensor_context(
            sensor_name="apprise_failure_sensor",
            dagster_event=event,
            dagster_run=run,
            dagster_instance=dg.DagsterInstance.ephemeral(),
            resources={"apprise": rec},
        )

        # call the sensor - should not raise an exception even if notification fails
        failure_sensor(ctx)

        # attempt to send notification
        assert len(rec.calls) == 1
        assert rec.calls[0]["status"] == "FAILURE"

    def test_sensor_with_invalid_job_name(self):
        """Test sensor behavior with invalid job names."""
        config = AppriseNotificationsConfig(
            urls=["pover://user@token"], events=["FAILURE"], include_jobs=["valid_*"]
        )

        defs = apprise_notifications(config)
        sensors = defs.sensors
        assert sensors is not None
        failure_sensor = next(s for s in sensors if "failure" in s.name.lower())

        # test run with job name that doesn't match include pattern
        run = dg.DagsterRun(job_name="invalid_job", run_id="test_run_123")
        from dagster._core.events import DagsterEvent, DagsterEventType

        event = DagsterEvent(
            event_type_value=DagsterEventType.PIPELINE_FAILURE.value,
            job_name="invalid_job",
            event_specific_data=None,
        )

        class Recorder:
            def __init__(self):
                self.calls = []

            def notify_run_status(self, run, status, message=None):
                self.calls.append({"run": run, "status": status, "message": message})
                return True

        rec = Recorder()

        ctx = dg.build_run_status_sensor_context(
            sensor_name="apprise_failure_sensor",
            dagster_event=event,
            dagster_run=run,
            dagster_instance=dg.DagsterInstance.ephemeral(),
            resources={"apprise": rec},
        )

        # call the sensor - should not send notification due to job filtering
        failure_sensor(ctx)

        # should not have sent notification
        assert len(rec.calls) == 0


class TestResourceEdgeCases:
    """Test edge cases in resource functionality."""

    def test_invalid_url_handling(self):
        """Test handling of invalid URLs."""
        config = AppriseConfig(
            urls=["invalid://url", "pover://user@token"],
            base_url="http://localhost:3000",
        )

        resource = AppriseResource(config=config)

        # should not raise an exception during initialization
        assert resource is not None
        assert resource.config.urls == ["invalid://url", "pover://user@token"]

    def test_config_file_handling(self):
        """Test handling of config file."""
        config = AppriseConfig(
            urls=["pover://user@token"], config_file="/nonexistent/file.yaml"
        )

        resource = AppriseResource(config=config)

        # should not raise an exception during initialization
        assert resource is not None
        assert resource.config.config_file == "/nonexistent/file.yaml"

    def test_notify_with_network_failure(self):
        """Test notify method with simulated network failure."""
        config = AppriseConfig(urls=["pover://user@token"])
        resource = AppriseResource(config=config)

        with patch("dagster_apprise.resource.apprise.Apprise") as mock_apprise_class:
            mock_apprise = Mock()
            mock_apprise.notify.side_effect = Exception("Network error")
            mock_apprise_class.return_value = mock_apprise

            from apprise import NotifyType

            result = resource.notify(
                body="Test message", title="Test Title", notify_type=NotifyType.INFO
            )

            assert result is False

    def test_notify_run_status_with_invalid_run(self):
        """Test notify_run_status with invalid run object."""
        config = AppriseConfig(urls=["pover://user@token"])
        resource = AppriseResource(config=config)

        # test with run missing required attributes
        invalid_run = Mock()
        invalid_run.job_name = None
        invalid_run.run_id = None

        with patch("dagster_apprise.resource.apprise.Apprise") as mock_apprise_class:
            mock_apprise = Mock()
            mock_apprise.notify.return_value = True
            mock_apprise_class.return_value = mock_apprise

            result = resource.notify_run_status(run=invalid_run, status="SUCCESS")
            # should still attempt to send notification
            assert isinstance(result, bool)

    def test_notify_run_status_with_invalid_status(self):
        """Test notify_run_status with invalid status."""
        config = AppriseConfig(urls=["pover://user@token"])
        resource = AppriseResource(config=config)

        run = dg.DagsterRun(job_name="test_job", run_id="test_run_123")

        with patch("dagster_apprise.resource.apprise.Apprise") as mock_apprise_class:
            mock_apprise = Mock()
            mock_apprise.notify.return_value = True
            mock_apprise_class.return_value = mock_apprise

            # test with invalid status
            result = resource.notify_run_status(run=run, status="INVALID_STATUS")

            # should still attempt to send notification
            assert result is True
            mock_apprise.notify.assert_called_once()

    def test_apprise_initialization_failure(self):
        """Test behavior when Apprise initialization fails."""
        config = AppriseConfig(urls=["pover://user@token"])
        resource = AppriseResource(config=config)

        with patch("dagster_apprise.resource.apprise.Apprise") as mock_apprise_class:
            mock_apprise_class.side_effect = Exception("Apprise initialization failed")

            # should not raise an exception during resource creation
            assert resource is not None
            assert resource.config == config

    def test_empty_urls_list(self):
        """Test behavior with empty URLs list."""
        config = AppriseConfig(urls=[])
        resource = AppriseResource(config=config)

        run = dg.DagsterRun(job_name="test_job", run_id="test_run_123")

        result = resource.notify_run_status(run=run, status="SUCCESS")

        # should return False when no URLs are configured
        assert result is False

    def test_malformed_base_url(self):
        """Test behavior with malformed base URL."""
        config = AppriseConfig(urls=["pover://user@token"], base_url="not-a-valid-url")

        resource = AppriseResource(config=config)

        run = dg.DagsterRun(job_name="test_job", run_id="test_run_123")

        with patch("dagster_apprise.resource.apprise.Apprise") as mock_apprise_class:
            mock_apprise = Mock()
            mock_apprise.notify.return_value = True
            mock_apprise_class.return_value = mock_apprise

            result = resource.notify_run_status(run=run, status="SUCCESS")

            # should still attempt to send notification
            assert result is True


class TestConfigurationEdgeCases:
    """Test edge cases in configuration."""

    def test_config_with_none_values(self):
        """Test configuration with None values."""
        # test with valid None values that are allowed
        config = AppriseNotificationsConfig(config_file=None, base_url=None)

        # should handle None values gracefully
        assert config.urls == []
        assert config.config_file is None
        assert config.base_url is None
        assert config.title_prefix == "Dagster"  # should use default

    def test_config_with_empty_strings(self):
        """Test configuration with empty strings."""
        config = AppriseNotificationsConfig(
            urls=[""], config_file="", base_url="", title_prefix=""
        )

        # should handle empty strings
        assert config.urls == [""]
        assert config.config_file == ""
        assert config.base_url == ""
        assert config.title_prefix == ""

    def test_config_with_very_long_values(self):
        """Test configuration with very long values."""
        long_string = "x" * 10000

        config = AppriseNotificationsConfig(
            urls=[f"pover://user@{long_string}"],
            base_url=f"http://localhost:3000/{long_string}",
            title_prefix=long_string,
        )

        # should handle long strings
        assert len(config.urls[0]) == 10000 + len("pover://user@")
        assert config.base_url is not None
        assert len(config.base_url) == 10000 + len("http://localhost:3000/")
        assert len(config.title_prefix) == 10000

    def test_config_with_special_characters(self):
        """Test configuration with special characters."""
        special_chars = "!@#$%^&*()_+-=[]{}|;':\",./<>?"

        config = AppriseNotificationsConfig(
            urls=[f"pover://user@{special_chars}"],
            base_url=f"http://localhost:3000/{special_chars}",
            title_prefix=special_chars,
        )

        # should handle special characters
        assert config.urls[0] == f"pover://user@{special_chars}"
        assert config.base_url == f"http://localhost:3000/{special_chars}"
        assert config.title_prefix == special_chars
