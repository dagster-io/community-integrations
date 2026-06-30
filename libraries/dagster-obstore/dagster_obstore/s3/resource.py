from importlib.metadata import PackageNotFoundError, version
from typing import TYPE_CHECKING, Optional

from dagster import ConfigurableResource
from obstore.store import S3Store
from pydantic import Field

if TYPE_CHECKING:
    from obstore.store import ClientConfig, RetryConfig, S3Config


def _user_agent_suffix() -> str:
    try:
        pkg_version = version("dagster-obstore")
    except PackageNotFoundError:
        pkg_version = "dev"
    return f"dagster-obstore/{pkg_version}"


class S3ObjectStore(ConfigurableResource):
    """Resource for AWS S3 and Backblaze B2 object storage backed by ``obstore``.

    Works against AWS S3 by default. Point at Backblaze B2 by setting
    ``endpoint`` to the B2 S3-compatible endpoint
    (``https://s3.<region>.backblazeb2.com``) and ``region`` to the matching
    B2 region.
    """

    access_key_id: str = Field(
        description="S3 access key ID to use when creating the S3 Store."
    )
    secret_access_key: str = Field(
        description="S3 secret access key to use when creating the S3 Store."
    )
    region: str | None = Field(
        default=None, description="Specifies a custom region for the S3 Store."
    )
    endpoint: str | None = Field(
        default=None,
        description=(
            "Specifies a custom endpoint for the S3 Store. Set this to target "
            "Backblaze B2 (``https://s3.<region>.backblazeb2.com``). Leave unset "
            "for AWS S3."
        ),
    )
    allow_http: bool = Field(
        default=False,
        description="Whether to allow http connections. By default, https is used.",
    )
    allow_invalid_certificates: bool = Field(
        default=False,
        description="Whether to allow invalid certificates. By default, valid certs are required.",
    )

    def create_store(
        self,
        bucket: str,
        timeout: str = "60s",
        retry_config: Optional["RetryConfig"] = None,
        client_options: Optional["ClientConfig"] = None,
    ) -> S3Store:
        """Creates an S3 object store."""
        config: S3Config = {
            "access_key_id": self.access_key_id,
            "secret_access_key": self.secret_access_key,
        }
        if self.region is not None:
            config["region"] = self.region
        if self.endpoint is not None:
            config["endpoint"] = self.endpoint

        resolved_client_options: ClientConfig = {
            "timeout": timeout,
            "allow_http": self.allow_http,
            "allow_invalid_certificates": self.allow_invalid_certificates,
        }
        if client_options:
            resolved_client_options.update(client_options)
        resolved_client_options.setdefault("user_agent", _user_agent_suffix())

        return S3Store(
            bucket=bucket,
            config=config,
            client_options=resolved_client_options,
            retry_config=retry_config,
        )
