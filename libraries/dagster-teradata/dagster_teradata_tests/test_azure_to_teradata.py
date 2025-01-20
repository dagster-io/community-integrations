import os

from dagster import job, op
from dagster_azure.adls2 import ADLS2Resource, ADLS2SASToken
from dagster_teradata import TeradataResource

azure_resource = ADLS2Resource(
    storage_account=os.getenv("AZURE_ACCOUNT", ""),
    credential=ADLS2SASToken(token=os.getenv("AZURE_TOKEN", "")),
)

td_resource = TeradataResource(
    host=os.getenv("TERADATA_HOST"),
    user=os.getenv("TERADATA_USER"),
    password=os.getenv("TERADATA_PASSWORD"),
    database=os.getenv("TERADATA_DATABASE"),
)


def test_azure_to_teradata(tmp_path):
    @op(required_resource_keys={"teradata"})
    def drop_existing_table(context):
        context.resources.teradata.drop_table("people")

    @op(required_resource_keys={"teradata", "azure"})
    def example_test_azure_to_teradata(context):
        context.resources.teradata.azure_blob_to_teradata(
            azure_resource, os.getenv("AZURE_LOCATION"), "people"
        )

    @job(resource_defs={"teradata": td_resource, "azure": azure_resource})
    def example_job():
        drop_existing_table()
        example_test_azure_to_teradata()

    example_job.execute_in_process(
        resources={"azure": azure_resource, "teradata": td_resource}
    )
