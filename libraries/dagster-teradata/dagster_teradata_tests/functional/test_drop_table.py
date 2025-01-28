from dagster import job, op, EnvVar
from dagster_teradata import TeradataResource

td_resource = TeradataResource(
    host=EnvVar("TERADATA_HOST"),
    user=EnvVar("TERADATA_USER"),
    password=EnvVar("TERADATA_PASSWORD"),
    database=EnvVar("TERADATA_DATABASE"),
)


def test_drop_table(tmp_path):
    @op(required_resource_keys={"teradata"})
    def example_test_drop_table(context):
        result = context.resources.teradata.drop_table(["abcd1", "abcd2"])
        context.log.info(result)

    @job(resource_defs={"teradata": td_resource})
    def example_job():
        example_test_drop_table()

    example_job.execute_in_process(resources={"teradata": td_resource})
