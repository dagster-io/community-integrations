import abc

from dagster import ConfigurableResource
from pydantic import Field


class BaseConnectionConfig(ConfigurableResource, abc.ABC):
    """Base class for Elasticsearch connection configurations."""


class HostsConfig(BaseConnectionConfig):
    """Connection parameters for self-hosted or generic Elasticsearch deployments."""

    hosts: list[str] = Field(
        description="One or more node URLs (e.g. ['https://localhost:9200']).",
    )
    api_key: str | None = Field(
        default=None,
        description="API key for authentication. Mutually exclusive with basic_auth.",
    )
    username: str | None = Field(
        default=None,
        description="Basic auth username. Use together with password.",
    )
    password: str | None = Field(
        default=None,
        description="Basic auth password. Use together with username.",
    )
    bearer_auth: str | None = Field(
        default=None,
        description="Bearer token for authentication.",
    )
    ca_certs: str | None = Field(
        default=None,
        description="Path to CA bundle for verifying TLS certificates.",
    )
    verify_certs: bool = Field(
        default=True,
        description="Whether to verify TLS certificates. Defaults to True.",
    )

    def to_client_kwargs(self) -> dict:
        kwargs: dict = {"hosts": self.hosts, "verify_certs": self.verify_certs}
        if self.api_key is not None:
            kwargs["api_key"] = self.api_key
        if self.username is not None and self.password is not None:
            kwargs["basic_auth"] = (self.username, self.password)
        if self.bearer_auth is not None:
            kwargs["bearer_auth"] = self.bearer_auth
        if self.ca_certs is not None:
            kwargs["ca_certs"] = self.ca_certs
        return kwargs


class CloudConfig(BaseConnectionConfig):
    """Connection parameters for Elastic Cloud deployments."""

    cloud_id: str = Field(
        description="Elastic Cloud deployment ID.",
    )
    api_key: str | None = Field(
        default=None,
        description="API key for authentication. Mutually exclusive with basic_auth.",
    )
    username: str | None = Field(
        default=None,
        description="Basic auth username. Use together with password.",
    )
    password: str | None = Field(
        default=None,
        description="Basic auth password. Use together with username.",
    )

    def to_client_kwargs(self) -> dict:
        kwargs: dict = {"cloud_id": self.cloud_id}
        if self.api_key is not None:
            kwargs["api_key"] = self.api_key
        if self.username is not None and self.password is not None:
            kwargs["basic_auth"] = (self.username, self.password)
        return kwargs
