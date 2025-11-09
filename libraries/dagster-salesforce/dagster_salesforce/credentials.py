import abc
from abc import abstractmethod
from typing import Any

import dagster as dg
from pydantic import Field
from simple_salesforce.api import DEFAULT_API_VERSION


class SalesforceCredentials(dg.ConfigurableResource, abc.ABC):
    version: str = Field(
        default=DEFAULT_API_VERSION, description="Salesforce API version"
    )

    @abstractmethod
    def connection_kwargs(self) -> dict[str, Any]:
        """Get a Salesforce client."""


class SalesforceUserPasswordCredentials(SalesforceCredentials):
    """Salesforce credentials for Password Authentication."""

    username: str = Field(description="Salesforce username (email)")
    password: str = Field(description="Salesforce password")
    security_token: str = Field(description="Salesforce security token")
    domain: str = Field(
        default="login",
        description="Custom domain (e.g., 'test' for test.salesforce.com)",
    )

    def connection_kwargs(self) -> dict[str, Any]:
        """Get connection kwargs for password authentication."""
        return {
            "username": self.username,
            "password": self.password,
            "security_token": self.security_token,
            "domain": self.domain,
            "version": self.version,
        }


class SalesforceSessionCredentials(SalesforceCredentials):
    """Salesforce credentials for Direct Session and Instance Access."""

    session_id: str = Field(description="Access token for this session")
    instance: str | None = Field(
        default=None,
        description="Domain of your Salesforce instance (e.g., 'na1.salesforce.com')",
    )
    instance_url: str | None = Field(
        default=None,
        description="Full URL of your instance (e.g., 'https://na1.salesforce.com')",
    )

    def connection_kwargs(self) -> dict[str, Any]:
        """Get connection kwargs for session-based authentication."""
        # Validate that both instance and instance_url aren't specified
        if self.instance and self.instance_url:
            raise ValueError(
                "Only one of 'instance' or 'instance_url' should be provided, not both"
            )

        if not self.instance and not self.instance_url:
            raise ValueError("Either 'instance' or 'instance_url' must be provided")

        kwargs = {
            "session_id": self.session_id,
            "version": self.version,
        }

        if self.instance_url:
            kwargs["instance_url"] = self.instance_url
        elif self.instance:
            kwargs["instance"] = self.instance

        return kwargs


class SalesforceConnectedAppOAuthCredentials(SalesforceCredentials):
    """Salesforce credentials for OAuth 2.0 Connected App Token Authentication."""

    consumer_key: str = Field(description="The consumer key generated for the user")
    consumer_secret: str = Field(
        description="The consumer secret generated for the user"
    )
    domain: str = Field(
        default="login",
        description="Custom domain (e.g., 'test' for test.salesforce.com)",
    )

    def connection_kwargs(self) -> dict[str, Any]:
        """Get connection kwargs for OAuth 2.0 Connected App authentication."""
        return {
            "consumer_key": self.consumer_key,
            "consumer_secret": self.consumer_secret,
            "domain": self.domain,
            "version": self.version,
        }


class SalesforceJWTOAuthCredentials(SalesforceCredentials):
    """Salesforce credentials for OAuth 2.0 JWT Bearer Token Authentication."""

    username: str = Field(description="Salesforce username (email)")
    consumer_key: str = Field(description="The consumer key generated for the user")
    privatekey_file: str | None = Field(
        default=None,
        description="Path to the private key file used for signing the JWT token",
    )
    privatekey: str | None = Field(
        default=None, description="The private key to use for signing the JWT token"
    )
    domain: str = Field(
        default="login",
        description="Custom domain (e.g., 'test' for test.salesforce.com)",
    )

    def connection_kwargs(self) -> dict[str, Any]:
        """Get connection kwargs for JWT Bearer Token authentication."""
        # Validate that both privatekey and privatekey_file aren't specified
        if self.privatekey and self.privatekey_file:
            raise ValueError(
                "Only one of 'privatekey' or 'privatekey_file' should be provided, not both"
            )

        if not self.privatekey and not self.privatekey_file:
            raise ValueError(
                "Either 'privatekey' or 'privatekey_file' must be provided"
            )

        kwargs = {
            "username": self.username,
            "consumer_key": self.consumer_key,
            "domain": self.domain,
            "version": self.version,
        }

        if self.privatekey:
            kwargs["privatekey"] = self.privatekey
        elif self.privatekey_file:
            kwargs["privatekey_file"] = self.privatekey_file

        return kwargs
