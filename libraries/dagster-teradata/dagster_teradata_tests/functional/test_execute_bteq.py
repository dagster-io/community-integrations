import os

from dagster import job, op
from dagster_teradata import TeradataResource

td_resource = TeradataResource(
    host=os.getenv("TERADATA_HOST"),
    user=os.getenv("TERADATA_USER"),
    password=os.getenv("TERADATA_PASSWORD"),
    database=os.getenv("TERADATA_DATABASE"),
)


def test_execute_bteq(tmp_path):
    @op(required_resource_keys={"teradata"})
    def example_test_execute_bteq(context):
        result = context.resources.teradata.bteq_operator(
            "select * from dbc.dbcinfo;"
        )
        context.log.info(result)

    @job(resource_defs={"teradata": td_resource})
    def example_job():
        example_test_execute_bteq()

    example_job.execute_in_process(resources={"teradata": td_resource})

def test_execute_bteq_sql_error(tmp_path):
    @op(required_resource_keys={"teradata"})
    def failing_op(context):
        context.resources.teradata.bteq_operator("SELECT * FROM;")  # Invalid SQL

    @job(resource_defs={"teradata": td_resource})
    def failing_job():
        failing_op()

    try:
        failing_job.execute_in_process(resources={"teradata": td_resource})
        assert False, "Expected job to fail due to syntax error"
    except Exception as e:
        assert "Syntax error" in str(e) or "3706" in str(e)


def test_execute_bteq_timeout(tmp_path):
    @op(required_resource_keys={"teradata"})
    def timeout_op(context):
        context.resources.teradata.bteq_operator("SELECT * FROM dbc.dbcinfo;", 1)

    @job(resource_defs={"teradata": td_resource})
    def timeout_job():
        timeout_op()

    try:
        timeout_job.execute_in_process(resources={"teradata": td_resource})
        assert False, "Expected timeout"
    except Exception as e:
        assert "timed out" in str(e)


import pytest

@pytest.mark.parametrize("invalid_sql", ["", None])
def test_invalid_bteq_sql(invalid_sql):
    @op(required_resource_keys={"teradata"})
    def invalid_sql_op(context):
        context.resources.teradata.bteq_operator(invalid_sql)

    @job(resource_defs={"teradata": td_resource})
    def invalid_sql_job():
        invalid_sql_op()

    with pytest.raises(ValueError, match="BTEQ script cannot be empty"):
        invalid_sql_job.execute_in_process(resources={"teradata": td_resource})


def test_bteq_custom_config(tmp_path):
    custom_td_resource = TeradataResource(
        host="localhost",
        user="user",
        password="password",
        database="dbc",
        bteq_output_width=200,
        bteq_session_encoding="UTF8",
        bteq_quit_zero=True,
    )

    @op(required_resource_keys={"teradata"})
    def config_check_op(context):
        result = context.resources.teradata.bteq_operator("SELECT * FROM dbc.dbcinfo;")
        assert result is not None

    @job(resource_defs={"teradata": custom_td_resource})
    def config_job():
        config_check_op()

    config_job.execute_in_process(resources={"teradata": custom_td_resource})


def test_logging_output(tmp_path, caplog):
    @op(required_resource_keys={"teradata"})
    def log_op(context):
        result = context.resources.teradata.bteq_operator("SELECT * FROM dbc.dbcinfo;")
        context.log.info(result)

    @job(resource_defs={"teradata": td_resource})
    def log_job():
        log_op()

    log_job.execute_in_process(resources={"teradata": td_resource})
    assert any("dbcinfo" in record.message for record in caplog.records)



