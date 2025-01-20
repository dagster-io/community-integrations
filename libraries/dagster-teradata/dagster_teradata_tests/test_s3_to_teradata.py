import os

from dagster import job, op
from dagster_aws.s3 import S3Resource
from dagster_teradata import TeradataResource

s3_resource = S3Resource(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
)

td_resource = TeradataResource(
    host=os.getenv("TERADATA_HOST"),
    user=os.getenv("TERADATA_USER"),
    password=os.getenv("TERADATA_PASSWORD"),
    database=os.getenv("TERADATA_DATABASE"),
)


def test_s3_to_teradata(tmp_path):
    @op(required_resource_keys={"teradata"})
    def drop_existing_table(context):
        context.resources.teradata.drop_table("people")

    @op(required_resource_keys={"teradata", "s3"})
    def example_test_s3_to_teradata(context):
        context.resources.teradata.s3_to_teradata(
            s3_resource, os.getenv("AWS_S3_LOCATION"), "people"
        )

    @job(resource_defs={"teradata": td_resource, "s3": s3_resource})
    def example_job():
        drop_existing_table()
        example_test_s3_to_teradata()

    example_job.execute_in_process(
        resources={"s3": s3_resource, "teradata": td_resource}
    )
