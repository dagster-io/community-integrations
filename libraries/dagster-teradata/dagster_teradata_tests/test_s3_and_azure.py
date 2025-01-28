from textwrap import dedent

import pytest
from unittest.mock import MagicMock
from dagster_teradata import TeradataResource, TeradataDagsterConnection
from dagster_aws.s3 import S3Resource
from dagster_azure.adls2 import ADLS2Resource, ADLS2SASToken


# Test for s3_to_teradata method
def test_s3_to_teradata():
    # Mock S3Resource
    s3_resource = MagicMock(spec=S3Resource)
    s3_resource.aws_access_key_id = "fake-access-key-id"
    s3_resource.aws_secret_access_key = "fake-access-key"
    s3_resource.aws_session_token = "fake-session-token"
    s3_source_key = "/s3/fake-bucket.s3.amazonaws.com/data.csv"
    teradata_table = "fake_teradata_table"

    # Create instance of TeradataResource and TeradataDagsterConnection
    teradata_resource = TeradataResource(
        host="fake_host", user="fake_user", password="fake_pass"
    )
    teradata_connection_resource = TeradataDagsterConnection(
        config={}, log=MagicMock(), teradata_connection_resource=teradata_resource
    )

    # Mock the execute_query to capture the SQL
    teradata_connection_resource.execute_query = MagicMock()

    # Call the s3_to_teradata method
    teradata_connection_resource.s3_to_teradata(
        s3=s3_resource, s3_source_key=s3_source_key, teradata_table=teradata_table
    )

    # Verify the generated SQL
    expected_sql = dedent("""
    CREATE MULTISET TABLE fake_teradata_table AS
    (
        SELECT * FROM (
            LOCATION = '/s3/fake-bucket.s3.amazonaws.com/data.csv'
            ACCESS_ID= 'fake-access-key-id' ACCESS_KEY= 'fake-access-key' SESSION_TOKEN = 'fake-session-token'
        ) AS d
    ) WITH DATA
    """).strip()
    # Assert that the execute_query method was called with the expected SQL
    actual_sql = teradata_connection_resource.execute_query.call_args[0][0].strip()
    assert actual_sql == expected_sql


# Test for azure_blob_to_teradata method
def test_azure_blob_to_teradata():
    # Mock ADLS2Resource
    azure_blob = MagicMock(spec=ADLS2Resource)
    azure_blob.storage_account = "fake-storage-account"
    azure_blob.credential = ADLS2SASToken(token="fake-token")
    blob_source_key = "/az/fake-container/fake-blob"
    teradata_table = "fake_teradata_table"

    # Create instance of TeradataResource and TeradataDagsterConnection
    teradata_resource = TeradataResource(
        host="fake_host", user="fake_user", password="fake_pass"
    )
    teradata_connection_resource = TeradataDagsterConnection(
        config={}, log=MagicMock(), teradata_connection_resource=teradata_resource
    )

    # Mock the execute_query to capture the SQL
    teradata_connection_resource.execute_query = MagicMock()

    # Call the azure_blob_to_teradata method
    teradata_connection_resource.azure_blob_to_teradata(
        azure=azure_blob, blob_source_key=blob_source_key, teradata_table=teradata_table
    )

    # Verify the generated SQL
    expected_sql = dedent("""
    CREATE MULTISET TABLE fake_teradata_table AS
    (
        SELECT * FROM (
            LOCATION = '/az/fake-container/fake-blob'
            ACCESS_ID= 'fake-storage-account' ACCESS_KEY= 'fake-token'
        ) AS d
    ) WITH DATA
    """).strip()
    # Assert that the execute_query method was called with the expected SQL
    actual_sql = teradata_connection_resource.execute_query.call_args[0][0].strip()
    assert actual_sql == expected_sql


# Run the tests using pytest
if __name__ == "__main__":
    pytest.main()
