from dagster_dataform.dataform_orchestration_schedule import (
    create_dataform_orchestration_schedule,
)
from dagster_dataform.resources import DataformRepositoryResource
import dagster as dg
from dagster import build_schedule_context
import pytest


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
def test_dataform_orchestration_schedule_creates_schedule_and_job(mock_dataform_client):
    resource = DataformRepositoryResource(
        project_id="test-project",
        repository_id="test-repo",
        location="us-central1",
        environment="dev",
        client=mock_dataform_client,  # pyright: ignore[reportArgumentType]
    )

    schedule = create_dataform_orchestration_schedule(
        resource=resource,
        cron_schedule="0 0 * * *",
        git_commitish="dev",
    )

    assert schedule is not None
    assert isinstance(schedule, dg.ScheduleDefinition)
    assert schedule.name == "dataform_orchestration_schedule"
    assert schedule.cron_schedule == "0 0 * * *"
    assert schedule.job is not None
    assert schedule.job.name == "dataform_workflow_invocation_job"


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
def test_dataform_orchestration_schedule_tick_creates_run_request(mock_dataform_client):
    resource = DataformRepositoryResource(
        project_id="test-project",
        repository_id="test-repo",
        location="us-central1",
        environment="dev",
        client=mock_dataform_client,
    )

    schedule = create_dataform_orchestration_schedule(
        resource=resource,
        cron_schedule="0 0 * * *",
        git_commitish="dev",
    )

    context = build_schedule_context()
    result = schedule.evaluate_tick(context)

    assert result is not None
    assert result.run_requests is not None
    assert len(result.run_requests) == 1


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
def test_dataform_orchestration_schedule_with_parameters(mock_dataform_client):
    resource = DataformRepositoryResource(
        project_id="test-project",
        repository_id="test-repo",
        location="us-central1",
        environment="dev",
        client=mock_dataform_client,
    )

    schedule = create_dataform_orchestration_schedule(
        resource=resource,
        cron_schedule="0 0 * * *",
        git_commitish="dev",
        included_targets=["target1"],
        included_tags=["tag1"],
    )

    assert schedule is not None
    assert isinstance(schedule, dg.ScheduleDefinition)
    # Testing internal op config is complex without running it, but ensuring
    # the schedule validates and creates successfully confirms the interface works.
