# pyright: reportAttributeAccessIssue=false, reportFunctionMemberAccess=false
"""
Test suite for SalesforceResource using pytest-mock.

Note: Uses pytest-mock for cleaner mocking syntax and better readability.
All test methods include proper type hints.
This version is refactored to work with the simplified SalesforceResource API.
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture
from simple_salesforce.bulk2 import (
    _Bulk2Client,
    SFBulk2Type,
    ColumnDelimiter,
    LineEnding,
)

from dagster_salesforce import (
    SalesforceResource,
    SalesforceObject,
    SalesforceField,
)
from dagster_salesforce_tests.helpers import TestSalesforceResource


class TestSalesforceResourceAuthentication(TestSalesforceResource):
    """Test authentication functionality."""

    def test_authentication_success(
        self, salesforce_resource: SalesforceResource
    ) -> None:
        """Test successful authentication."""
        # Force authentication
        salesforce_resource._connect()

        # Verify authentication
        assert salesforce_resource._access_token == "test_session_id"
        assert salesforce_resource._instance_url == "https://test.salesforce.com"

    def test_authentication_caching(
        self, salesforce_resource: SalesforceResource, mocker: MockerFixture
    ) -> None:
        """Test that authentication is cached and reused."""
        # Set valid token expiry
        salesforce_resource._access_token = "existing_token"
        salesforce_resource._token_expiry = datetime.now(timezone.utc) + timedelta(
            hours=1
        )

        # Mock Salesforce constructor
        mock_salesforce_class = mocker.patch("dagster_salesforce.resource.Salesforce")

        # Authenticate (should not create new client)
        salesforce_resource._connect()

        # Verify Salesforce constructor was not called
        mock_salesforce_class.assert_not_called()


class TestSalesforceResourceObjectOperations(TestSalesforceResource):
    """Test operations through SalesforceObject."""

    def test_get_object_client(self, salesforce_resource: SalesforceResource) -> None:
        """Test getting SalesforceObject client."""
        # Set up mock
        mock_account = MagicMock()
        mock_describe_result = {
            "name": "Account",
            "label": "Account",
            "labelPlural": "Accounts",
            "custom": False,
            "createable": True,
            "updateable": True,
            "deletable": True,
            "queryable": True,
            "searchable": True,
            "retrieveable": True,
            "triggerable": True,
            "undeletable": False,
            "mergeable": True,
            "replicateable": True,
            "activateable": False,
            "fields": [
                {
                    "name": "Id",
                    "label": "Record ID",
                    "type": "id",
                    "length": 18,
                    "precision": 0,
                    "scale": 0,
                    "digits": 0,
                    "custom": False,
                    "nillable": False,
                    "createable": False,
                    "updateable": False,
                    "unique": True,
                    "autoNumber": False,
                    "externalId": False,
                    "calculated": False,
                    "filterable": True,
                    "sortable": True,
                    "groupable": True,
                    "compoundFieldName": None,
                },
                {
                    "name": "Name",
                    "label": "Account Name",
                    "type": "string",
                    "length": 255,
                    "precision": 0,
                    "scale": 0,
                    "digits": 0,
                    "custom": False,
                    "nillable": False,
                    "createable": True,
                    "updateable": True,
                    "unique": False,
                    "autoNumber": False,
                    "externalId": False,
                    "calculated": False,
                    "filterable": True,
                    "sortable": True,
                    "groupable": True,
                    "compoundFieldName": None,
                },
            ],
        }
        mock_account.describe.return_value = mock_describe_result
        salesforce_resource.client.Account = mock_account

        # Get object client
        account_obj = salesforce_resource.get_object_client("Account")

        # Verify object metadata
        assert isinstance(account_obj, SalesforceObject)
        assert account_obj.name == "Account"
        assert account_obj.label == "Account"
        assert account_obj.label_plural == "Accounts"
        assert account_obj.custom is False
        assert account_obj.createable is True
        assert account_obj.updateable is True
        assert account_obj.deletable is True
        assert account_obj.queryable is True

        # Verify fields
        assert len(account_obj.fields) == 2
        assert "Id" in account_obj.fields
        assert "Name" in account_obj.fields

        # Verify Id field
        id_field = account_obj.fields["Id"]
        assert isinstance(id_field, SalesforceField)
        assert id_field.name == "Id"
        assert id_field.label == "Record ID"
        assert id_field.type == "id"
        assert id_field.length == 18
        assert id_field.unique is True
        assert id_field.createable is False
        assert id_field.updateable is False

    def test_create_record_through_object(
        self, salesforce_resource: SalesforceResource, mocker: MockerFixture
    ) -> None:
        """Test creating a record through SalesforceObject."""
        # Mock the _salesforce client's Account object
        mock_account = MagicMock()
        mock_account.create.return_value = {"success": True, "id": "001ABC"}
        mock_account.describe.return_value = {
            "name": "Account",
            "label": "Account",
            "labelPlural": "Accounts",
            "custom": False,
            "createable": True,
            "updateable": True,
            "deletable": True,
            "queryable": True,
            "searchable": True,
            "retrieveable": True,
            "triggerable": True,
            "undeletable": False,
            "mergeable": True,
            "replicateable": True,
            "activateable": False,
            "fields": [],
        }
        salesforce_resource.client.Account = mock_account

        # Create record through SalesforceObject
        account_obj = salesforce_resource.get_object_client("Account")
        record_id = account_obj.create_record({"Name": "Test Account"})

        # Verify
        assert record_id == "001ABC"
        mock_account.create.assert_called_once_with({"Name": "Test Account"})

    def test_update_record_through_object(
        self, salesforce_resource: SalesforceResource, mocker: MockerFixture
    ) -> None:
        """Test updating a record through SalesforceObject."""
        # Mock the _salesforce client's Account object
        mock_account = MagicMock()
        mock_account.update.return_value = 204
        mock_account.describe.return_value = {
            "name": "Account",
            "label": "Account",
            "labelPlural": "Accounts",
            "custom": False,
            "createable": True,
            "updateable": True,
            "deletable": True,
            "queryable": True,
            "searchable": True,
            "retrieveable": True,
            "triggerable": True,
            "undeletable": False,
            "mergeable": True,
            "replicateable": True,
            "activateable": False,
            "fields": [],
        }
        salesforce_resource.client.Account = mock_account

        # Update record through SalesforceObject
        account_obj = salesforce_resource.get_object_client("Account")
        account_obj.update_record("001ABC", {"Name": "Updated Account"})

        # Verify
        mock_account.update.assert_called_once_with(
            "001ABC", {"Name": "Updated Account"}
        )

    def test_delete_record_through_object(
        self, salesforce_resource: SalesforceResource, mocker: MockerFixture
    ) -> None:
        """Test deleting a record through SalesforceObject."""
        # Mock the _salesforce client's Account object
        mock_account = MagicMock()
        mock_account.delete.return_value = 204
        mock_account.describe.return_value = {
            "name": "Account",
            "label": "Account",
            "labelPlural": "Accounts",
            "custom": False,
            "createable": True,
            "updateable": True,
            "deletable": True,
            "queryable": True,
            "searchable": True,
            "retrieveable": True,
            "triggerable": True,
            "undeletable": False,
            "mergeable": True,
            "replicateable": True,
            "activateable": False,
            "fields": [],
        }
        salesforce_resource.client.Account = mock_account

        # Delete record through SalesforceObject
        account_obj = salesforce_resource.get_object_client("Account")
        account_obj.delete_record("001ABC")

        # Verify
        mock_account.delete.assert_called_once_with("001ABC")


class TestSalesforceResourceBulkOperations(TestSalesforceResource):
    """Test bulk operations through SalesforceObject."""

    def test_bulk_create_through_object(
        self, salesforce_resource: SalesforceResource, mocker: MockerFixture
    ) -> None:
        """Test bulk create operation through SalesforceObject."""
        # Mock the Account describe
        mock_account = MagicMock()
        mock_account.describe.return_value = {
            "name": "Account",
            "label": "Account",
            "labelPlural": "Accounts",
            "custom": False,
            "createable": True,
            "updateable": True,
            "deletable": True,
            "queryable": True,
            "searchable": True,
            "retrieveable": True,
            "triggerable": True,
            "undeletable": False,
            "mergeable": True,
            "replicateable": True,
            "activateable": False,
            "fields": [],
        }
        salesforce_resource.client.Account = mock_account

        # Mock bulk2 handler for Account
        mock_account_bulk = MagicMock()
        mock_account_bulk.insert.return_value = [
            {
                "numberRecordsFailed": 0,
                "numberRecordsProcessed": 2,
                "numberRecordsTotal": 2,
                "job_id": "job123",
            }
        ]
        salesforce_resource.client.bulk2.Account = mock_account_bulk

        # Execute bulk create through SalesforceObject
        account_obj = salesforce_resource.get_object_client("Account")
        records = [{"Name": "Account 1"}, {"Name": "Account 2"}]
        result = account_obj.bulk2.insert(records=records, wait=300)

        # Verify
        assert result[0]["job_id"] == "job123"
        assert result[0]["numberRecordsProcessed"] == 2
        assert result[0]["numberRecordsFailed"] == 0
        mock_account_bulk.insert.assert_called_once_with(records=records, wait=300)

    def test_bulk_update_through_object(
        self, salesforce_resource: SalesforceResource, mocker: MockerFixture
    ) -> None:
        """Test bulk update operation through SalesforceObject."""
        # Mock the Account describe
        mock_account = MagicMock()
        mock_account.describe.return_value = {
            "name": "Account",
            "label": "Account",
            "labelPlural": "Accounts",
            "custom": False,
            "createable": True,
            "updateable": True,
            "deletable": True,
            "queryable": True,
            "searchable": True,
            "retrieveable": True,
            "triggerable": True,
            "undeletable": False,
            "mergeable": True,
            "replicateable": True,
            "activateable": False,
            "fields": [],
        }
        salesforce_resource.client.Account = mock_account

        # Mock bulk2 handler for Account
        mock_account_bulk = MagicMock()
        mock_account_bulk.update.return_value = [
            {
                "numberRecordsFailed": 0,
                "numberRecordsProcessed": 2,
                "numberRecordsTotal": 2,
                "job_id": "job456",
            }
        ]
        salesforce_resource.client.bulk2.Account = mock_account_bulk

        # Execute bulk update through SalesforceObject
        account_obj = salesforce_resource.get_object_client("Account")
        records = [
            {"Id": "001ABC", "Name": "Updated Account 1"},
            {"Id": "002ABC", "Name": "Updated Account 2"},
        ]
        result = account_obj.bulk2.update(records=records, wait=300)

        # Verify
        assert result[0]["job_id"] == "job456"
        assert result[0]["numberRecordsProcessed"] == 2
        mock_account_bulk.update.assert_called_once_with(records=records, wait=300)

    def test_bulk_upsert_through_object(
        self, salesforce_resource: SalesforceResource, mocker: MockerFixture
    ) -> None:
        """Test bulk upsert operation through SalesforceObject."""
        # Mock the Account describe
        mock_account = MagicMock()
        mock_account.describe.return_value = {
            "name": "Account",
            "label": "Account",
            "labelPlural": "Accounts",
            "custom": False,
            "createable": True,
            "updateable": True,
            "deletable": True,
            "queryable": True,
            "searchable": True,
            "retrieveable": True,
            "triggerable": True,
            "undeletable": False,
            "mergeable": True,
            "replicateable": True,
            "activateable": False,
            "fields": [],
        }
        salesforce_resource.client.Account = mock_account

        # Mock bulk2 handler for Account
        mock_account_bulk = MagicMock()
        mock_account_bulk.upsert.return_value = [
            {
                "numberRecordsFailed": 0,
                "numberRecordsProcessed": 2,
                "numberRecordsTotal": 2,
                "job_id": "job789",
            }
        ]
        salesforce_resource.client.bulk2.Account = mock_account_bulk

        # Execute bulk upsert through SalesforceObject
        account_obj = salesforce_resource.get_object_client("Account")
        records = [
            {"External_Id__c": "EXT001", "Name": "Account 1"},
            {"External_Id__c": "EXT002", "Name": "Account 2"},
        ]
        result = account_obj.bulk2.upsert(
            records=records, external_id_field="External_Id__c", wait=300
        )

        # Verify
        assert result[0]["job_id"] == "job789"
        assert result[0]["numberRecordsProcessed"] == 2
        mock_account_bulk.upsert.assert_called_once_with(
            records=records, external_id_field="External_Id__c", wait=300
        )

    def test_bulk_delete_through_object(
        self, salesforce_resource: SalesforceResource, mocker: MockerFixture
    ) -> None:
        """Test bulk delete operation through SalesforceObject."""
        # Mock the Account describe
        mock_account = MagicMock()
        mock_account.describe.return_value = {
            "name": "Account",
            "label": "Account",
            "labelPlural": "Accounts",
            "custom": False,
            "createable": True,
            "updateable": True,
            "deletable": True,
            "queryable": True,
            "searchable": True,
            "retrieveable": True,
            "triggerable": True,
            "undeletable": False,
            "mergeable": True,
            "replicateable": True,
            "activateable": False,
            "fields": [],
        }
        salesforce_resource.client.Account = mock_account

        # Mock bulk2 handler for Account
        mock_account_bulk = MagicMock()
        mock_account_bulk.delete.return_value = [
            {
                "numberRecordsFailed": 0,
                "numberRecordsProcessed": 2,
                "numberRecordsTotal": 2,
                "job_id": "job999",
            }
        ]
        salesforce_resource.client.bulk2.Account = mock_account_bulk

        # Execute bulk delete through SalesforceObject
        account_obj = salesforce_resource.get_object_client("Account")
        records = [{"Id": "001ABC"}, {"Id": "002ABC"}]
        result = account_obj.bulk2.delete(records=records, wait=300)

        # Verify
        assert result[0]["job_id"] == "job999"
        assert result[0]["numberRecordsProcessed"] == 2
        mock_account_bulk.delete.assert_called_once_with(records=records, wait=300)


class TestSalesforceResourceConnectionInfo(TestSalesforceResource):
    """Test connection and instance information methods."""

    def test_check_connection_success(
        self, salesforce_resource: SalesforceResource, mock_salesforce_auth: MagicMock
    ) -> None:
        """Test successful connection check."""
        # Set up mocks
        # Update the mock base_url to match our test expectation
        mock_salesforce_auth.base_url = "https://na123.salesforce.com"
        salesforce_resource._instance_url = None  # Force re-authentication
        salesforce_resource.client.query.side_effect = [
            # User query response
            {
                "records": [
                    {
                        "Id": "005XX000001bcDE",
                        "Username": "test@example.com",
                        "Name": "Test User",
                        "Email": "test@example.com",
                    }
                ]
            },
            # Organization query response
            {
                "records": [
                    {
                        "Id": "00DXX0000001abc",
                        "Name": "Test Company",
                        "OrganizationType": "Enterprise Edition",
                        "InstanceName": "NA123",
                        "IsSandbox": False,
                    }
                ]
            },
        ]

        # Check connection
        result = salesforce_resource.check_connection()

        # Verify
        assert result["authenticated"] is True
        assert result["instance_url"] == "https://na123.salesforce.com"
        assert result["username"] == "test@example.com"
        assert result["user_name"] == "Test User"
        assert result["organization_name"] == "Test Company"
        assert result["instance_type"] == "Production"
        assert result["is_sandbox"] is False

    def test_check_connection_failure(
        self, salesforce_resource: SalesforceResource
    ) -> None:
        """Test connection check with authentication failure."""
        # Set up mock to raise exception
        with pytest.raises(Exception):
            salesforce_resource.client.query.side_effect = Exception(
                "Authentication failed"
            )

            # Check connection
            salesforce_resource.check_connection()

    def test_get_limits(self, salesforce_resource: SalesforceResource) -> None:
        """Test getting API limits."""
        # Set up mock
        mock_limits = {
            "DailyApiRequests": {"Max": 15000, "Remaining": 14950},
            "DailyBulkApiRequests": {"Max": 10000, "Remaining": 9999},
        }
        salesforce_resource.client.limits.return_value = mock_limits

        # Get limits
        limits = salesforce_resource.get_limits()

        # Verify
        assert limits == mock_limits
        assert limits["DailyApiRequests"]["Max"] == 15000

    def test_get_available_objects(
        self, salesforce_resource: SalesforceResource
    ) -> None:
        """Test getting available objects."""
        # Set up mock
        salesforce_resource.client.describe.return_value = {
            "sobjects": [
                {"name": "Account", "queryable": True},
                {"name": "Contact", "queryable": True},
                {"name": "History", "queryable": False},  # Not queryable
                {"name": "Lead", "queryable": True},
            ]
        }

        # Get objects
        objects = salesforce_resource.get_available_objects()

        # Verify
        assert len(objects) == 3
        assert "Account" in objects
        assert "Contact" in objects
        assert "Lead" in objects
        assert "History" not in objects  # Not queryable

    def test_query_to_csv_through_object(
        self,
        salesforce_resource: SalesforceResource,
        mocker: MockerFixture,
        tmp_path: Path,
    ) -> None:
        """Test query_to_csv method through SalesforceObject."""
        # Mock the Account describe with fields
        mock_account = MagicMock()
        mock_account.describe.return_value = {
            "name": "Account",
            "label": "Account",
            "labelPlural": "Accounts",
            "custom": False,
            "createable": True,
            "updateable": True,
            "deletable": True,
            "queryable": True,
            "searchable": True,
            "retrieveable": True,
            "triggerable": True,
            "undeletable": False,
            "mergeable": True,
            "replicateable": True,
            "activateable": False,
            "fields": [
                {"name": "Id", "compoundFieldName": None},
                {"name": "Name", "compoundFieldName": None},
            ],
        }
        salesforce_resource.client.Account = mock_account

        # Mock the bulk2 client for Account
        mock_bulk2_client = MagicMock(spec=_Bulk2Client)
        mock_bulk2_client.create_job.return_value = {"id": "job123"}
        mock_bulk2_client.download_job_data.return_value = {
            "locator": None,
            "number_of_records": 1,
        }

        # Mock bulk2 handler for Account
        mock_account_bulk = MagicMock(spec=SFBulk2Type)
        mock_account_bulk._client = mock_bulk2_client
        salesforce_resource.client.bulk2.Account = mock_account_bulk

        # Mock wait_for_job function
        with mocker.patch(
            "dagster_salesforce.resource._wait_for_job", return_value=None
        ):
            # Execute query_to_csv through SalesforceObject
            account_obj = salesforce_resource.get_object_client("Account")
            output_dir = tmp_path / "output"
            account_obj.query_to_csv("SELECT Id, Name FROM Account", output_dir)

            # Verify
            mock_bulk2_client.create_job.assert_called_once()
            assert output_dir.exists()

    def test_upsert_records_through_object(
        self,
        salesforce_resource: SalesforceResource,
        mocker: MockerFixture,
        tmp_path: Path,
    ) -> None:
        """Test upsert_records operation through SalesforceObject."""
        # Mock the Account describe
        mock_account = MagicMock()
        mock_account.describe.return_value = {
            "name": "Account",
            "label": "Account",
            "labelPlural": "Accounts",
            "custom": False,
            "createable": True,
            "updateable": True,
            "deletable": True,
            "queryable": True,
            "searchable": True,
            "retrieveable": True,
            "triggerable": True,
            "undeletable": False,
            "mergeable": True,
            "replicateable": True,
            "activateable": False,
            "fields": [],
        }
        salesforce_resource.client.Account = mock_account

        # Mock bulk2 handler for Account
        mock_account_bulk = MagicMock()
        mock_account_bulk.upsert.return_value = [
            {
                "numberRecordsFailed": 1,
                "numberRecordsProcessed": 3,
                "numberRecordsTotal": 3,
                "job_id": "job_upsert_001",
            }
        ]

        # Mock get_all_ingest_records to return success and failure records
        mock_account_bulk.get_all_ingest_records.return_value = {
            "successfulRecords": [
                {"Id": "001ABC", "Name": "Account 1"},
                {"Id": "002ABC", "Name": "Account 2"},
            ],
            "failedRecords": [
                {"Id": "003ABC", "Name": "Account 3", "error": "Duplicate value"}
            ],
            "unprocessedRecords": [],
        }
        salesforce_resource.client.bulk2.Account = mock_account_bulk

        # Execute upsert_records through SalesforceObject
        account_obj = salesforce_resource.get_object_client("Account")
        records = [
            {"External_Id__c": "EXT001", "Name": "Account 1"},
            {"External_Id__c": "EXT002", "Name": "Account 2"},
            {"External_Id__c": "EXT003", "Name": "Account 3"},
        ]

        output_dir = tmp_path / "upsert_results"
        result = account_obj.upsert_records(
            result_output_directory=output_dir,
            records=records,
            external_id_field="External_Id__c",
            additional_result_metadata={"run_id": "test_run_001"},
            wait=5,
        )

        # Verify the upsert was called correctly
        mock_account_bulk.upsert.assert_called_once_with(
            csv_file=None,
            records=records,
            external_id_field="External_Id__c",
            batch_size=None,
            column_delimiter=ColumnDelimiter.COMMA,
            line_ending=LineEnding.LF,
            wait=5,
        )

        # Verify get_all_ingest_records was called with correct job_id
        mock_account_bulk.get_all_ingest_records.assert_called_once_with(
            "job_upsert_001"
        )

        # Verify the result
        assert result.number_of_jobs == 1
        assert result.successfulRecords == 2
        assert result.failedRecords == 1
        assert result.unprocessedRecords == 0
        assert len(result.files) == 2  # One for successful, one for failed

        # Verify output directory was created
        assert output_dir.exists()

        # Verify JSONL files were created with correct content
        successful_file = output_dir / "job_upsert_001_successfulRecords.jsonl"
        failed_file = output_dir / "job_upsert_001_failedRecords.jsonl"

        assert successful_file in result.files
        assert failed_file in result.files

        # Check successful records file content
        with open(successful_file) as f:
            lines = f.readlines()
            assert len(lines) == 2
            first_record = json.loads(lines[0])
            assert first_record["job_id"] == "job_upsert_001"
            assert first_record["result_type"] == "successfulRecords"
            assert first_record["run_id"] == "test_run_001"
            assert first_record["Id"] == "001ABC"
            assert first_record["Name"] == "Account 1"

        # Check failed records file content
        with open(failed_file) as f:
            lines = f.readlines()
            assert len(lines) == 1
            failed_record = json.loads(lines[0])
            assert failed_record["job_id"] == "job_upsert_001"
            assert failed_record["result_type"] == "failedRecords"
            assert failed_record["run_id"] == "test_run_001"
            assert failed_record["Id"] == "003ABC"
            assert failed_record["error"] == "Duplicate value"
