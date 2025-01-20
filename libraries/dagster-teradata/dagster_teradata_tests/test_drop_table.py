import os

import pytest
from dagster import job, op
from dagster_teradata import TeradataResource, teradata_resource

teradata_resource = TeradataResource(
    host=os.getenv("TERADATA_HOST"),
    user=os.getenv("TERADATA_USER"),
    password=os.getenv("TERADATA_PASSWORD"),
    database=os.getenv("TERADATA_DATABASE"),
)


@pytest.mark.integration
def test_drop_table(tmp_path):
    @op(required_resource_keys={"teradata"})
    def example_test_drop_table(context):
        result = context.resources.teradata.drop_table(["abcd1", "abcd2"])
        context.log.info(result)

    @job(resource_defs={"teradata": teradata_resource})
    def example_job():
        example_test_drop_table()

    example_job.execute_in_process(resources={"teradata": teradata_resource})