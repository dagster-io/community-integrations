from pathlib import Path

import pytest

from dagster_salesforce import SalesforceResource
from dagster_salesforce.credentials import (
    SalesforceJWTOAuthCredentials,
    SalesforceUserPasswordCredentials,
    SalesforceSessionCredentials,
    SalesforceConnectedAppOAuthCredentials,
)
from dagster_salesforce_tests.helpers import TestSalesforceResource


class TestSalesforceResourceCredentials(TestSalesforceResource):
    """Test resource initialization and configuration."""

    def test_initialization_with_jwt_private_key_file(
        self, tmp_path: Path, private_key_content: str
    ) -> None:
        """Test initialization with JWT authentication using private key file path."""
        # Create temporary key file
        key_file = tmp_path / "private.key"
        key_file.write_text(private_key_content)

        resource = SalesforceResource(
            credentials=SalesforceJWTOAuthCredentials(
                username="test@example.com",
                consumer_key="test_consumer_key",
                privatekey_file=str(key_file),
            )
        )

        assert resource.credentials.username == "test@example.com"
        assert resource.credentials.consumer_key == "test_consumer_key"
        assert resource.credentials.privatekey_file == str(key_file)

    def test_initialization_with_jwt_private_key_content(
        self, private_key_content: str
    ) -> None:
        """Test initialization with JWT authentication using private key content."""
        resource = SalesforceResource(
            credentials=SalesforceJWTOAuthCredentials(
                username="test@example.com",
                consumer_key="test_consumer_key",
                privatekey=private_key_content,
            )
        )

        assert resource.credentials.username == "test@example.com"
        assert resource.credentials.consumer_key == "test_consumer_key"
        assert resource.credentials.privatekey == private_key_content

    def test_jwt_initialization_missing_private_key(self) -> None:
        """Test JWT initialization fails when no private key is provided."""
        with pytest.raises(
            ValueError,
            match="Either 'privatekey' or 'privatekey_file' must be provided",
        ):
            resource = SalesforceResource(
                credentials=SalesforceJWTOAuthCredentials(
                    username="test@example.com", consumer_key="test_consumer_key"
                )
            )
            # Force the connection to trigger the validation
            resource.credentials.connection_kwargs()

    def test_sandbox_configuration(self, private_key_content: str) -> None:
        """Test sandbox configuration."""
        resource = SalesforceResource(
            credentials=SalesforceJWTOAuthCredentials(
                username="test@example.com",
                consumer_key="test_consumer_key",
                privatekey=private_key_content,
                domain="test",
            )
        )

        assert resource.credentials.domain == "test"

    def test_initialization_with_password_credentials(self) -> None:
        """Test initialization with password authentication."""
        resource = SalesforceResource(
            credentials=SalesforceUserPasswordCredentials(
                username="test@example.com",
                password="test_password",
                security_token="test_token",
                domain="login",
            )
        )

        assert resource.credentials.username == "test@example.com"
        assert resource.credentials.password == "test_password"
        assert resource.credentials.security_token == "test_token"
        assert resource.credentials.domain == "login"

    def test_initialization_with_session_credentials(self) -> None:
        """Test initialization with session credentials."""
        resource = SalesforceResource(
            credentials=SalesforceSessionCredentials(
                session_id="test_session_id", instance_url="https://na1.salesforce.com"
            )
        )

        assert resource.credentials.session_id == "test_session_id"
        assert resource.credentials.instance_url == "https://na1.salesforce.com"

    def test_session_credentials_validation(self) -> None:
        """Test session credentials validation for mutually exclusive params."""
        # Test that both instance and instance_url cannot be provided
        with pytest.raises(
            ValueError,
            match="Only one of 'instance' or 'instance_url' should be provided, not both",
        ):
            resource = SalesforceResource(
                credentials=SalesforceSessionCredentials(
                    session_id="test_session_id",
                    instance="na1.salesforce.com",
                    instance_url="https://na1.salesforce.com",
                )
            )
            resource.credentials.connection_kwargs()

        # Test that at least one must be provided
        with pytest.raises(
            ValueError,
            match="Either 'instance' or 'instance_url' must be provided",
        ):
            resource = SalesforceResource(
                credentials=SalesforceSessionCredentials(session_id="test_session_id")
            )
            resource.credentials.connection_kwargs()

    def test_initialization_with_oauth_credentials(self) -> None:
        """Test initialization with OAuth Connected App credentials."""
        resource = SalesforceResource(
            credentials=SalesforceConnectedAppOAuthCredentials(
                consumer_key="test_consumer_key",
                consumer_secret="test_consumer_secret",
                domain="login",
            )
        )

        assert resource.credentials.consumer_key == "test_consumer_key"
        assert resource.credentials.consumer_secret == "test_consumer_secret"
        assert resource.credentials.domain == "login"
