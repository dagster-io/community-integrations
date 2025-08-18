from google.cloud import dataform_v1
from google.protobuf import timestamp_pb2
from google.type import interval_pb2
import pytest
from unittest.mock import Mock


def create_compilation_result(
    git_commitish, default_database, default_schema, default_location, assertion_schema
):
    return dataform_v1.CompilationResult(
        name="test-compilation-result",
        git_commitish=git_commitish,
        code_compilation_config=dataform_v1.CodeCompilationConfig(
            default_database=default_database,
            default_schema=default_schema,
            default_location=default_location,
            assertion_schema=assertion_schema,
        ),
    )


MOCK_COMPILATION_RESULT_ACTION = dataform_v1.CompilationResultAction(
    target=dataform_v1.Target(
        name="test-asset",
        schema="test_schema",
        database="test-database",
    ),
    relation=dataform_v1.CompilationResultAction.Relation(
        dependency_targets=[
            dataform_v1.Target(
                name="test-asset-1",
                schema="test_schema_1",
                database="test-database-1",
            ),
            dataform_v1.Target(
                name="test-asset-2",
                schema="test_schema_2",
                database="test-database-2",
            ),
        ]
    ),
)

MOCK_WORKFLOW_INVOCATION = dataform_v1.WorkflowInvocation(
    name="test-workflow-invocation",
    state=dataform_v1.WorkflowInvocation.State.SUCCEEDED,
)

MOCK_WORKFLOW_INVOCATION_ACTION_PASSED = dataform_v1.WorkflowInvocationAction(
    bigquery_action=dataform_v1.WorkflowInvocationAction.BigQueryAction(
        sql_script="SELECT 1",
        job_id="test-job-id",
    ),
    state=dataform_v1.WorkflowInvocationAction.State.SUCCEEDED,
    target=dataform_v1.Target(
        name="test-asset",
        schema="test_schema",
        database="test-database",
    ),
    invocation_timing=interval_pb2.Interval(
        start_time=timestamp_pb2.Timestamp(seconds=1723958400),
        end_time=timestamp_pb2.Timestamp(seconds=1723958400),
    ),
)

MOCK_WORKFLOW_INVOCATION_ACTION_FAILED = dataform_v1.WorkflowInvocationAction(
    bigquery_action=dataform_v1.WorkflowInvocationAction.BigQueryAction(
        sql_script="SELECT 1",
        job_id="",
    ),
    state=dataform_v1.WorkflowInvocationAction.State.FAILED,
    target=dataform_v1.Target(
        name="test-asset",
        schema="test_schema",
        database="test-database",
    ),
    invocation_timing=interval_pb2.Interval(
        start_time=timestamp_pb2.Timestamp(seconds=1723958400),
        end_time=timestamp_pb2.Timestamp(seconds=1723958400),
    ),
)

@pytest.fixture
def mock_dataform_client(request):
    # This allows us to parametrize the test with different values for the git commitish, default database, default schema, default location, and assertion schema if necesessary (see test_dataform_repository_resource_get_latest_compilation_result_name_wrong_environment)
    git_commitish = request.param.get("git_commitish", "test-commitish")
    default_database = request.param.get("default_database", "test-database")
    default_schema = request.param.get("default_schema", "test-schema")
    default_location = request.param.get("default_location", "us-central1")
    assertion_schema = request.param.get("assertion_schema", "test-assertion-schema")
    workflow_invocation_passed = request.param.get("workflow_invocation_passed", True)

    mock_client = Mock()

    mock_create_compilation_result_response = create_compilation_result(
        git_commitish,
        default_database,
        default_schema,
        default_location,
        assertion_schema,
    )
    mock_compilation_result_action = MOCK_COMPILATION_RESULT_ACTION
    mock_workflow_invocation = MOCK_WORKFLOW_INVOCATION
    mock_workflow_invocation_action_passed = MOCK_WORKFLOW_INVOCATION_ACTION_PASSED
    mock_workflow_invocation_action_failed = MOCK_WORKFLOW_INVOCATION_ACTION_FAILED

    mock_query_compilation_result_actions_response = (
        dataform_v1.QueryCompilationResultActionsResponse(
            compilation_result_actions=[mock_compilation_result_action]
        )
    )

    mock_list_compilation_results_response = dataform_v1.ListCompilationResultsResponse(
        compilation_results=[mock_create_compilation_result_response]
    )

    mock_list_workflow_invocations_response = (
        dataform_v1.ListWorkflowInvocationsResponse(
            workflow_invocations=[mock_workflow_invocation]
        )
    )

    if workflow_invocation_passed:
        mock_query_workflow_invocation_actions_response = (
            dataform_v1.QueryWorkflowInvocationActionsResponse(
                workflow_invocation_actions=[mock_workflow_invocation_action_passed]
            )
        )
    else:
        mock_query_workflow_invocation_actions_response = (
            dataform_v1.QueryWorkflowInvocationActionsResponse(
                workflow_invocation_actions=[mock_workflow_invocation_action_failed]
            )
        )

    mock_get_workflow_invocation_response = mock_workflow_invocation
    mock_client.create_compilation_result.return_value = (
        mock_create_compilation_result_response
    )
    mock_client.list_compilation_results.return_value = (
        mock_list_compilation_results_response
    )
    mock_client.query_compilation_result_actions.return_value = (
        mock_query_compilation_result_actions_response
    )
    mock_client.list_workflow_invocations.return_value = (
        mock_list_workflow_invocations_response
    )
    mock_client.query_workflow_invocation_actions.return_value = (
        mock_query_workflow_invocation_actions_response
    )
    mock_client.create_workflow_invocation.return_value = mock_workflow_invocation
    mock_client.get_workflow_invocation.return_value = (
        mock_get_workflow_invocation_response
    )

    return mock_client
