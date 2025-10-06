import dagster as dg
from dagster_dataform.resources import DataformRepositoryResource
import time
from typing import Optional, Dict, Any


def create_dataform_orchestration_schedule(
    resource: DataformRepositoryResource,
    cron_schedule: str,
    git_commitish: str,
    default_database: Optional[str] = None,
    default_schema: Optional[str] = None,
    default_location: Optional[str] = None,
    assertion_schema: Optional[str] = None,
    database_suffix: Optional[str] = None,
    schema_suffix: Optional[str] = None,
    table_prefix: Optional[str] = None,
    builtin_assertion_name_prefix: Optional[str] = None,
    vars: Optional[Dict[str, Any]] = None,
):
    """
    This function creates a schedule that, on the passed cron schedule, will create a dataform workflow invocation.
    It will then poll the status of the workflow invocation until it is complete.
    """

    @dg.op
    def dataform_workflow_invocation_op(context: dg.OpExecutionContext) -> str:
        compilation_result = resource.create_compilation_result(
            git_commitish=git_commitish,
            default_database=default_database,
            default_schema=default_schema,
            default_location=default_location,
            assertion_schema=assertion_schema,
            database_suffix=database_suffix,
            schema_suffix=schema_suffix,
            table_prefix=table_prefix,
            builtin_assertion_name_prefix=builtin_assertion_name_prefix,
            vars=vars,
        )

        context.log.info(f"Created compilation result: {compilation_result.name}")

        workflow_invocation = resource.create_workflow_invocation(
            compilation_result_name=compilation_result.name,
        )

        context.log.info(f"Created workflow invocation: {workflow_invocation.name}")

        # Poll the status of the workflow invocation until it is not in the RUNNING state or it has been running for more than 30 minutes
        context.log.info("Polling workflow invocation status")
        start_time = time.time()
        while True:
            context.log.info(
                f"Polling workflow invocation status at {time.time() - start_time} seconds"
            )
            workflow_invocation_details = resource.get_workflow_invocation_details(
                workflow_invocation.name
            )
            context.log.info(
                f"Workflow invocation status: {workflow_invocation_details.state.name}"
            )
            if workflow_invocation_details.state.name != "RUNNING":
                context.log.info("Workflow invocation is not running, breaking")
                break
            if time.time() - start_time > 1800:
                context.log.info("Workflow invocation timed out, breaking")
                raise Exception("Workflow invocation timed out")
            time.sleep(10)

        if workflow_invocation_details.state.name == "SUCCEEDED":
            context.log.info("Orchestration op completed")
            return "Orchestration op completed"
        else:
            context.log.info(
                f"Orchestration op failed with status {workflow_invocation_details.state.name}"
            )
            raise Exception(
                f"Orchestration op failed with status {workflow_invocation_details.state.name}"
            )

    @dg.job
    def dataform_workflow_invocation_job():
        dataform_workflow_invocation_op()

    @dg.schedule(
        cron_schedule=cron_schedule,
        job=dataform_workflow_invocation_job,
    )
    def dataform_orchestration_schedule(
        context: dg.ScheduleEvaluationContext,
    ):
        # Run the workflow invocation job on the passed cron schedule

        return dg.RunRequest(
            run_config={},
            run_key=None,
            job=dataform_workflow_invocation_job,
        )

    return dataform_orchestration_schedule
