import os

from dagster import job, op
from dagster_teradata import TeradataResource

td_resource = TeradataResource(
    host=os.getenv("TERADATA_HOST"),
    user=os.getenv("TERADATA_USER"),
    password=os.getenv("TERADATA_PASSWORD"),
    database=os.getenv("TERADATA_DATABASE"),
)


def test_drop_database(tmp_path):
    @op(required_resource_keys={"teradata"})
    def example_test_drop_database(context):
        result = context.resources.teradata.drop_database(["abcd1", "abcd2"])
        context.log.info(result)

    @job(resource_defs={"teradata": td_resource})
    def example_job():
        example_test_drop_database()

    example_job.execute_in_process(resources={"teradata": td_resource})
