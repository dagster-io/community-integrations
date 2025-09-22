import os

from dagster import job, op
from dagster_teradata import TeradataResource

td_resource = TeradataResource(
    host=os.getenv("TERADATA_HOST"),
    user=os.getenv("TERADATA_USER"),
    password=os.getenv("TERADATA_PASSWORD"),
    database=os.getenv("TERADATA_DATABASE"),
)


def test_local_ddl_execution():
    @op(required_resource_keys={"teradata"})
    def example_test_local_script(context):
        result = context.resources.teradata.ddl_operator(
            ddl=[
                "drop table ddl_test_abcd;",
                "create table ddl_test_abcd (a int, b int);",
            ],
            error_list=[3807],
        )
        context.log.info(result)
        assert result == 0

    @job(resource_defs={"teradata": td_resource})
    def example_job():
        example_test_local_script()

    example_job.execute_in_process(resources={"teradata": td_resource})


def test_remove_ddl_execution():
    @op(required_resource_keys={"teradata"})
    def example_test_local_script(context):
        result = context.resources.teradata.ddl_operator(
            ddl=[
                "drop table ddl_test_abcd;",
                "create table ddl_test_abcd (a int, b int);",
            ],
            error_list=[3807],
            remote_host="host",
            remote_user="user",
            remote_password="pass",
        )
        context.log.info(result)
        assert result == 0

    @job(resource_defs={"teradata": td_resource})
    def example_job():
        example_test_local_script()

    example_job.execute_in_process(resources={"teradata": td_resource})
