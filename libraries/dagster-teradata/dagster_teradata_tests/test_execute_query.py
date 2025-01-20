import os

import pytest
from dagster import job, op
from dagster_teradata import TeradataResource, teradata_resource

teradata_resource = TeradataResource(
    host=os.getenv("TERADATA_HOST"),
    user=os.getenv("TERADATA_USER"),
    password=os.getenv("TERADATA_PASSWORD"),
    # database=os.getenv("TERADATA_DATABASE"),
)


@pytest.mark.integration
def test_execute_query(tmp_path):
    @op(required_resource_keys={"teradata"})
    def example_test_execute_query(context):
        result = context.resources.teradata.execute_query("select * from dbc.dbcinfo", True)
        context.log.info(result)

    @job(resource_defs={"teradata": teradata_resource})
    def example_job():
        example_test_execute_query()

    example_job.execute_in_process(resources={"teradata": teradata_resource})