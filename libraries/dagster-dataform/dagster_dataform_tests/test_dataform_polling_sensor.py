from dagster_dataform.dataform_polling_sensor import (
    create_dataform_workflow_invocation_sensor,
)
from dagster_dataform.resources import DataformRepositoryResource
from dagster_dataform_tests.conftest import mock_dataform_client
import dagster as dg
from dagster import build_sensor_context
import pytest
import json


def test_dataform_polling_sensor_creates_sensor_and_job():
    resource = DataformRepositoryResource(
        project_id="test-project",
        repository_id="test-repo",
        location="us-central1",
        environment="dev",
        client=mock_dataform_client,
    )

    sensor = create_dataform_workflow_invocation_sensor(
        resource=resource,
        minutes_ago=10,
    )

    assert sensor is not None
    assert isinstance(sensor, dg.SensorDefinition)
    assert sensor.name == "dataform_workflow_invocation_sensor"
    assert sensor.job is not None
    assert sensor.job.name == "dataform_workflow_invocation_failure_notification_job"


def test_dataform_polling_sensor_creates_sensor_and_job_when_passed_job():
    resource = DataformRepositoryResource(
        project_id="test-project",
        repository_id="test-repo",
        location="us-central1",
        environment="dev",
        client=mock_dataform_client,
    )

    @dg.job
    def test_job():
        pass

    sensor = create_dataform_workflow_invocation_sensor(
        resource=resource,
        minutes_ago=10,
        job=test_job,
    )

    assert sensor is not None
    assert isinstance(sensor, dg.SensorDefinition)
    assert sensor.name == "dataform_workflow_invocation_sensor"
    assert sensor.job is not None
    assert sensor.job.name == "test_job"


@pytest.mark.parametrize(
    "mock_dataform_client",
    [
        {
            "git_commitish": "test-commitish",
            "default_database": "test-database",
            "default_schema": "test-schema",
            "default_location": "us-central1",
            "assertion_schema": "test-assertion-schema",
        }
    ],
    indirect=True,
)
def test_dataform_polling_sensor_returns_sensor_result_cursor_updated(
    mock_dataform_client,
):
    resource = DataformRepositoryResource(
        project_id="test-project",
        repository_id="test-repo",
        location="us-central1",
        environment="dev",
        client=mock_dataform_client,
    )

    sensor = create_dataform_workflow_invocation_sensor(
        resource=resource,
        minutes_ago=10,
    )

    cursor = json.dumps({"test-asset": 1723957400})

    expected_new_cursor = json.dumps({"test-asset": 1723958400})

    context = build_sensor_context(cursor=cursor)
    result = sensor.evaluate_tick(context)

    assert result is not None
    assert result.cursor == expected_new_cursor
    assert result.asset_events is not None
    assert len(result.asset_events) == 1
    assert isinstance(result.asset_events[0], dg.AssetMaterialization)
    assert result.asset_events[0].metadata is not None
    assert result.asset_events[0].metadata["Invocation SQL Query"] is not None
    assert result.asset_events[0].metadata["BigQuery JobID"] is not None


@pytest.mark.parametrize(
    "mock_dataform_client",
    [
        {
            "git_commitish": "test-commitish",
            "default_database": "test-database",
            "default_schema": "test-schema",
            "default_location": "us-central1",
            "assertion_schema": "test-assertion-schema",
        }
    ],
    indirect=True,
)
def test_dataform_polling_sensor_returns_sensor_result_cursor_not_updated(
    mock_dataform_client,
):
    resource = DataformRepositoryResource(
        project_id="test-project",
        repository_id="test-repo",
        location="us-central1",
        environment="dev",
        client=mock_dataform_client,
    )

    sensor = create_dataform_workflow_invocation_sensor(
        resource=resource,
        minutes_ago=10,
    )

    cursor = json.dumps({"test-asset": 1723958400})

    expected_new_cursor = json.dumps({"test-asset": 1723958400})

    context = build_sensor_context(cursor=cursor)
    result = sensor.evaluate_tick(context)

    assert result is not None
    assert result.cursor == expected_new_cursor
    assert result.asset_events is not None
    assert len(result.asset_events) == 0


@pytest.mark.parametrize(
    "mock_dataform_client",
    [
        {
            "git_commitish": "test-commitish",
            "default_database": "test-database",
            "default_schema": "test-schema",
            "default_location": "us-central1",
            "assertion_schema": "test-assertion-schema",
            "workflow_invocation_passed": False,
        }
    ],
    indirect=True,
)
def test_dataform_polling_sensor_returns_sensor_result_invocation_failed(
    mock_dataform_client,
):
    resource = DataformRepositoryResource(
        project_id="test-project",
        repository_id="test-repo",
        location="us-central1",
        environment="dev",
        client=mock_dataform_client,
    )

    sensor = create_dataform_workflow_invocation_sensor(
        resource=resource,
        minutes_ago=10,
    )

    cursor = json.dumps({"test-asset": 1723957400})

    expected_new_cursor = json.dumps({"test-asset": 1723958400})

    context = build_sensor_context(cursor=cursor)
    result = sensor.evaluate_tick(context)

    assert result is not None
    assert result.cursor == expected_new_cursor
    assert result.asset_events is not None
    assert len(result.asset_events) == 1
    assert isinstance(result.asset_events[0], dg.AssetObservation)
    assert result.asset_events[0].metadata is not None
    assert result.asset_events[0].metadata["Invocation SQL Query"] is not None
    assert result.asset_events[0].metadata["BigQuery JobID"] is not None
    assert result.run_requests is not None
    assert len(result.run_requests) == 1
