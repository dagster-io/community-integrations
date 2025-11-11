from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from dagster_salesforce import SalesforceResource
from dagster_salesforce.credentials import SalesforceJWTOAuthCredentials


class TestSalesforceResource:
    """Base test class for SalesforceResource unit tests."""

    @pytest.fixture(scope="class")
    def private_key_content(self) -> str:
        """Mock private key content for testing."""
        # This is a dummy private key for testing purposes only
        return """-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7VJTUt9Us8cKj
-----END PRIVATE KEY-----"""

    @pytest.fixture(autouse=True)
    def mock_salesforce_auth(self, mocker: MockerFixture) -> MagicMock:
        """Mock Salesforce authentication to prevent actual API calls."""
        mock_sf = MagicMock()
        mock_sf.session_id = "test_session_id"
        mock_sf.base_url = "https://test.salesforce.com"

        # Create mock bulk2 handler
        mock_bulk2_handler = MagicMock()
        mock_sf.bulk2 = mock_bulk2_handler

        # Mock the Salesforce constructor
        mock_salesforce_class = mocker.patch("dagster_salesforce.resource.Salesforce")
        mock_salesforce_class.return_value = mock_sf

        return mock_sf

    @pytest.fixture(scope="class")
    def salesforce_resource(self, private_key_content: str) -> SalesforceResource:
        """Class-scoped fixture to create a test SalesforceResource instance."""
        return SalesforceResource(
            credentials=SalesforceJWTOAuthCredentials(
                username="test@example.com",
                consumer_key="test_consumer_key",
                privatekey=private_key_content,
            )
        )
