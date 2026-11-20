"""Backblaze B2 resources for Dagster.

`B2Resource` is a thin Pydantic wrapper around `dagster_aws.s3.S3Resource` that:

- defaults `endpoint_url` to the Backblaze B2 S3-compatible endpoint
- accepts B2-native field names (`application_key_id`, `application_key`) as
  aliases for the underlying boto3 `aws_access_key_id` / `aws_secret_access_key`

Backblaze B2 is fully S3-compatible, so under the hood this resource produces a
boto3 S3 client equivalent to the one produced by ``S3Resource``. The wrapper
exists so users get typed B2 config and B2-flavored field names without having
to remember the endpoint URL or the AWS-named credential fields.
"""

import os
from importlib.metadata import PackageNotFoundError, version
from typing import Any

import boto3
from botocore.config import Config
from botocore.handlers import disable_signing
from dagster_aws.s3.resources import S3Resource
from dagster_aws.utils import construct_boto_client_retry_config
from pydantic import Field, model_validator

DEFAULT_B2_ENDPOINT_URL = "https://s3.us-west-004.backblazeb2.com"

# Backblaze publishes application keys under these env vars; honored as a
# fallback so users following the documented B2 env-var convention don't
# have to set AWS-flavored env vars on a non-AWS deployment.
B2_APPLICATION_KEY_ID_ENV_VAR = "B2_APPLICATION_KEY_ID"
B2_APPLICATION_KEY_ENV_VAR = "B2_APPLICATION_KEY"
B2_ENDPOINT_URL_ENV_VAR = "B2_ENDPOINT_URL"


def _user_agent_suffix() -> str:
    try:
        pkg_version = version("dagster-backblaze")
    except PackageNotFoundError:
        pkg_version = "dev"
    return f"dagster-backblaze/{pkg_version}"


class B2Resource(S3Resource):
    """Resource that gives access to Backblaze B2 (S3-compatible) storage.

    Subclasses :py:class:`dagster_aws.s3.S3Resource` and pre-fills `endpoint_url`
    with the Backblaze B2 default. Accepts B2-native credential field names
    (`application_key_id`, `application_key`) which map to the underlying
    `aws_access_key_id` / `aws_secret_access_key` boto3 parameters.

    Example:
        .. code-block:: python

            from dagster import Definitions, asset
            from dagster_backblaze import B2Resource

            @asset
            def my_asset(b2: B2Resource):
                client = b2.get_client()
                client.put_object(Bucket="my-bucket", Key="hello", Body=b"world")

            Definitions(
                assets=[my_asset],
                resources={
                    "b2": B2Resource(
                        application_key_id="...",
                        application_key="...",
                    )
                },
            )

    The default endpoint is ``https://s3.us-west-004.backblazeb2.com``. Override
    `endpoint_url` to point at a different B2 region (for example
    ``https://s3.eu-central-003.backblazeb2.com``).
    """

    endpoint_url: str | None = Field(
        default=DEFAULT_B2_ENDPOINT_URL,
        description=(
            "Backblaze B2 S3-compatible endpoint URL. Defaults to "
            "https://s3.us-west-004.backblazeb2.com. Override to target a "
            "different B2 region."
        ),
    )
    application_key_id: str | None = Field(
        default=None,
        description=(
            "Backblaze B2 application key ID. Aliased onto `aws_access_key_id` "
            "for the underlying boto3 client."
        ),
    )
    application_key: str | None = Field(
        default=None,
        description=(
            "Backblaze B2 application key. Aliased onto `aws_secret_access_key` "
            "for the underlying boto3 client."
        ),
    )

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return False

    @model_validator(mode="after")
    def _map_b2_credentials_to_aws_fields(self) -> "B2Resource":
        # Pick up Backblaze-flavored env vars when the corresponding B2 field
        # was not provided. Explicit constructor values always win, env vars
        # are only a default for unconfigured fields.
        application_key_id = self.application_key_id or os.environ.get(
            B2_APPLICATION_KEY_ID_ENV_VAR
        )
        application_key = self.application_key or os.environ.get(
            B2_APPLICATION_KEY_ENV_VAR
        )

        # Map the B2-named fields onto the AWS-named fields that the parent
        # S3Resource forwards to boto3. We only set the AWS field if the user
        # provided a B2 field (or B2 env var) and did not also provide the AWS
        # field directly, so explicit `aws_access_key_id=...` still wins.
        if application_key_id is not None and self.aws_access_key_id is None:
            object.__setattr__(self, "aws_access_key_id", application_key_id)
        if application_key is not None and self.aws_secret_access_key is None:
            object.__setattr__(self, "aws_secret_access_key", application_key)

        # B2_ENDPOINT_URL env var overrides the per-region default but never
        # an explicit constructor value.
        if self.endpoint_url == DEFAULT_B2_ENDPOINT_URL:
            env_endpoint = os.environ.get(B2_ENDPOINT_URL_ENV_VAR)
            if env_endpoint:
                object.__setattr__(self, "endpoint_url", env_endpoint)

        return self

    def get_client(self) -> Any:
        """Construct a boto3 S3 client pre-configured for Backblaze B2.

        Mirrors ``dagster_aws.s3.utils.construct_s3_client`` and merges the
        retry config with the package identifier so it is not lost when both
        are present.
        """
        retry_config = construct_boto_client_retry_config(self.max_attempts)
        config = retry_config.merge(Config(user_agent_extra=_user_agent_suffix()))

        session = boto3.session.Session(profile_name=self.profile_name)
        s3_client = session.resource(
            "s3",
            region_name=self.region_name,
            use_ssl=self.use_ssl,
            verify=self.verify,
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            aws_session_token=self.aws_session_token,
            config=config,
        ).meta.client

        if self.use_unsigned_session:
            s3_client.meta.events.register("choose-signer.s3.*", disable_signing)

        return s3_client

    def get_object_to_set_on_execution_context(self) -> Any:
        return self.get_client()
