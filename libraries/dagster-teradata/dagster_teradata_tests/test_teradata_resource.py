from unittest import mock

from dagster import (
    job,
    op,
)
from dagster_teradata import teradata_resource
from dagster_teradata_tests.utils import create_mock_connector


@mock.patch("teradatasql.connect", new_callable=create_mock_connector)
def test_teradata_resource(teradata_connect):
    @op(required_resource_keys={"teradata"})
    def teradata_op(context):
        assert context.resources.teradata
        with context.resources.teradata.get_connection() as _:
            pass

    resource = teradata_resource.configured(
        {
            "host": "foo",
            "user": "bar",
            "password": "baz",
            "database": "TESTDB",
        }
    )

    @job(resource_defs={"teradata": resource})
    def teradata_job():
        teradata_op()

    result = teradata_job.execute_in_process()
    assert result.success
    teradata_connect.assert_called_once_with(
        host="foo",
        user="bar",
        password="baz",
        database="TESTDB",
    )
