"""Tests for AppriseNotifications component and sensors."""

import dagster as dg

from dagster_apprise.components import AppriseNotificationsConfig, apprise_notifications


class TestAppriseNotifications:
    """Test cases for AppriseNotifications component."""

    def test_component_init_defaults(self):
        """Test component initialization with default values."""
        config = AppriseNotificationsConfig()
        assert config.urls == []
        assert config.config_file is None
        assert config.base_url is None
        assert config.title_prefix == "Dagster"
        assert config.events == ["FAILURE"]
        assert config.include_jobs == ["*"]
        assert config.exclude_jobs == []

    def test_component_init_custom(self):
        """Test component initialization with custom values."""
        config = AppriseNotificationsConfig(
            urls=["pover://user@token"],
            base_url="http://localhost:3000",
            events=["SUCCESS", "FAILURE"],
            include_jobs=["test_*"],
            exclude_jobs=["*_dev"],
        )
        assert config.urls == ["pover://user@token"]
        assert config.base_url == "http://localhost:3000"
        assert config.events == ["SUCCESS", "FAILURE"]
        assert config.include_jobs == ["test_*"]
        assert config.exclude_jobs == ["*_dev"]

    def test_build_defs(self):
        """Test building Dagster definitions."""
        config = AppriseNotificationsConfig(
            urls=["pover://user@token"], events=["FAILURE"]
        )
        defs = apprise_notifications(config)

        assert isinstance(defs, dg.Definitions)
        resources = defs.resources
        sensors = defs.sensors
        assert resources is not None
        assert "apprise" in resources
        assert sensors is not None
        sensors_list = list(sensors)
        assert len(sensors_list) == 1  # One sensor for FAILURE event

    def test_build_defs_multiple_events(self):
        """Test building definitions with multiple events."""
        config = AppriseNotificationsConfig(
            urls=["pover://user@token"], events=["SUCCESS", "FAILURE", "CANCELED"]
        )
        defs = apprise_notifications(config)

        sensors = defs.sensors
        sensors_list = list(sensors) if sensors is not None else []
        assert len(sensors_list) == 3
        sensor_names = [s.name for s in sensors_list]
        assert "apprise_success_sensor" in sensor_names
        assert "apprise_failure_sensor" in sensor_names
        assert "apprise_canceled_sensor" in sensor_names

    def test_should_include_job_wildcard(self):
        """Test job inclusion with wildcard pattern."""
        from dagster_apprise.components import _should_include_job

        assert _should_include_job("any_job", ["*"]) is True
        assert _should_include_job("test_job", ["*"]) is True

    def test_should_include_job_pattern(self):
        """Test job inclusion with specific patterns."""
        from dagster_apprise.components import _should_include_job

        assert _should_include_job("test_job", ["test_*", "prod_*"]) is True
        assert _should_include_job("prod_job", ["test_*", "prod_*"]) is True
        assert _should_include_job("dev_job", ["test_*", "prod_*"]) is False

    def test_should_exclude_job(self):
        """Test job exclusion patterns."""
        from dagster_apprise.components import _should_exclude_job

        assert _should_exclude_job("my_job_dev", ["*_dev", "test_*"]) is True
        assert _should_exclude_job("test_job", ["*_dev", "test_*"]) is True
        assert _should_exclude_job("prod_job", ["*_dev", "test_*"]) is False

    def test_matches_pattern(self):
        """Test pattern matching functionality."""
        from dagster_apprise.components import _matches_pattern

        assert _matches_pattern("test_job", "test_*") is True
        assert _matches_pattern("test_job", "prod_*") is False
        assert _matches_pattern("my_job", "*_job") is True


class TestAppriseSensor:
    """Test cases for Apprise run status sensors."""

    def test_failure_sensor_filters_and_sends(self):
        """Test that failure sensor filters jobs and sends notifications."""
        config = AppriseNotificationsConfig(
            urls=[], events=["FAILURE"], include_jobs=["good*"]
        )
        defs = apprise_notifications(config)

        # Find the failure sensor
        sensors = defs.sensors
        assert sensors is not None
        failure_sensor = next(s for s in sensors if s.name.endswith("_failure_sensor"))
        assert failure_sensor is not None

        # Create a test run and event
        run = dg.DagsterRun(job_name="good_job", run_id="rid")
        from dagster._core.events import DagsterEvent, DagsterEventType

        event = DagsterEvent(
            event_type_value=DagsterEventType.PIPELINE_FAILURE.value,
            job_name="good_job",
            event_specific_data=None,
        )

        # Inject a recorder double instead of mutating the (frozen) resource
        class Recorder:
            def __init__(self):
                self.calls = []

            def notify_run_status(self, run, status, message=None):
                self.calls.append({"run": run, "status": status, "message": message})
                return True

        rec = Recorder()

        # Build context with injected resources mapping
        ctx = dg.build_run_status_sensor_context(
            sensor_name="apprise_failure_sensor",
            dagster_event=event,
            dagster_run=run,
            dagster_instance=dg.DagsterInstance.ephemeral(),
            resources={"apprise": rec},
        )

        # Call the sensor with context carrying our recorder resource
        failure_sensor(ctx)

        assert len(rec.calls) == 1
        assert rec.calls[0]["status"] == "FAILURE"
        assert rec.calls[0]["run"].job_name == "good_job"

    def test_sensor_skips_excluded_jobs(self):
        """Test that sensor skips jobs in the exclude list."""
        config = AppriseNotificationsConfig(
            urls=[], events=["FAILURE"], include_jobs=["*"], exclude_jobs=["bad*"]
        )
        defs = apprise_notifications(config)

        sensors = defs.sensors
        assert sensors is not None
        failure_sensor = next(s for s in sensors if s.name.endswith("_failure_sensor"))

        # Create a test run with excluded job name
        run = dg.DagsterRun(job_name="bad_job", run_id="rid")
        from dagster._core.events import DagsterEvent, DagsterEventType

        event = DagsterEvent(
            event_type_value=DagsterEventType.PIPELINE_FAILURE.value,
            job_name="bad_job",
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

        # Call the sensor with context carrying our recorder resource
        failure_sensor(ctx)

        # Should not have been called due to exclusion
        assert len(rec.calls) == 0

    def test_sensor_skips_not_included_jobs(self):
        """Test that sensor skips jobs not in the include list."""
        config = AppriseNotificationsConfig(
            urls=[], events=["FAILURE"], include_jobs=["good*"]
        )
        defs = apprise_notifications(config)

        sensors = defs.sensors
        assert sensors is not None
        failure_sensor = next(s for s in sensors if s.name.endswith("_failure_sensor"))

        # Create a test run with job name not matching include pattern
        run = dg.DagsterRun(job_name="other_job", run_id="rid")
        from dagster._core.events import DagsterEvent, DagsterEventType

        event = DagsterEvent(
            event_type_value=DagsterEventType.PIPELINE_FAILURE.value,
            job_name="other_job",
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

        # Call the sensor with context carrying our recorder resource
        failure_sensor(ctx)

        # Should not have been called due to not matching include pattern
        assert len(rec.calls) == 0
