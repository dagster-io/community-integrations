"""Tests for `B2Resource`.

`B2Resource` is a thin wrapper around `dagster_aws.s3.S3Resource`. Backblaze B2
is fully S3-compatible, so we exercise the resource against `moto`'s mocked S3
service the same way `dagster-aws` tests do. The B2-specific behavior we
verify here is:

- the default `endpoint_url` is the Backblaze us-west-004 endpoint,
- a user-provided `endpoint_url` overrides the default,
- the ``B2_ENDPOINT_URL`` env var overrides the default but not an explicit value,
- `application_key_id` / `application_key` get mapped onto the boto3-named
  `aws_access_key_id` / `aws_secret_access_key` fields,
- ``B2_APPLICATION_KEY_ID`` / ``B2_APPLICATION_KEY`` env vars populate creds,
- explicit `aws_access_key_id` / `aws_secret_access_key` win when both are set,
- `get_client()` returns a working boto3 S3 client (round-trip via moto),
- every client carries the ``dagster-backblaze/{version}`` token in its
  botocore ``user_agent_extra``.
"""

from dagster_backblaze import B2Resource
from dagster_backblaze.b2.resources import DEFAULT_B2_ENDPOINT_URL, _user_agent_suffix
from moto import mock_aws

BUCKET_NAME = "test-bucket"

# moto enforces AWS-shaped access keys (20-char ID, 40-char secret) starting
# with AKIA/ASIA for the validators it ships. The resource itself does not
# care about the format because B2 application keys are forwarded to boto3
# as-is. These constants exist only to satisfy moto in the round-trip tests.
MOTO_FAKE_KEY_ID = "AKIAIOSFODNN7EXAMPLE"
MOTO_FAKE_SECRET = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"


def test_b2_resource_default_endpoint_url() -> None:
    b2 = B2Resource()
    assert b2.endpoint_url == DEFAULT_B2_ENDPOINT_URL
    assert b2.endpoint_url == "https://s3.us-west-004.backblazeb2.com"


def test_b2_env_vars_populate_credentials(monkeypatch) -> None:
    """``B2_APPLICATION_KEY_ID`` / ``B2_APPLICATION_KEY`` populate creds."""
    monkeypatch.setenv("B2_APPLICATION_KEY_ID", "00400_from_env")
    monkeypatch.setenv("B2_APPLICATION_KEY", "K004_from_env")

    b2 = B2Resource()

    assert b2.aws_access_key_id == "00400_from_env"
    assert b2.aws_secret_access_key == "K004_from_env"


def test_explicit_b2_fields_win_over_b2_env_vars(monkeypatch) -> None:
    """Constructor-set B2 fields take precedence over ``B2_*`` env vars."""
    monkeypatch.setenv("B2_APPLICATION_KEY_ID", "00400_from_env")
    monkeypatch.setenv("B2_APPLICATION_KEY", "K004_from_env")

    b2 = B2Resource(
        application_key_id="00400_explicit", application_key="K004_explicit"
    )

    assert b2.aws_access_key_id == "00400_explicit"
    assert b2.aws_secret_access_key == "K004_explicit"


def test_b2_endpoint_url_env_var_overrides_default(monkeypatch) -> None:
    """``B2_ENDPOINT_URL`` env var overrides the default endpoint."""
    eu_endpoint = "https://s3.eu-central-003.backblazeb2.com"
    monkeypatch.setenv("B2_ENDPOINT_URL", eu_endpoint)

    b2 = B2Resource()

    assert b2.endpoint_url == eu_endpoint


def test_explicit_endpoint_wins_over_env_var(monkeypatch) -> None:
    """Constructor-set ``endpoint_url`` wins over ``B2_ENDPOINT_URL``."""
    monkeypatch.setenv("B2_ENDPOINT_URL", "https://from.env.example.com")
    custom = "https://s3.eu-central-003.backblazeb2.com"

    b2 = B2Resource(endpoint_url=custom)

    assert b2.endpoint_url == custom


def test_b2_resource_endpoint_url_override() -> None:
    custom = "https://s3.eu-central-003.backblazeb2.com"
    b2 = B2Resource(endpoint_url=custom)
    assert b2.endpoint_url == custom


def test_application_key_fields_map_to_aws_fields() -> None:
    b2 = B2Resource(
        application_key_id="b2-key-id",
        application_key="b2-secret",
    )
    assert b2.aws_access_key_id == "b2-key-id"
    assert b2.aws_secret_access_key == "b2-secret"
    # B2-named fields should still be readable as set.
    assert b2.application_key_id == "b2-key-id"
    assert b2.application_key == "b2-secret"


def test_explicit_aws_credentials_win_over_b2_fields() -> None:
    b2 = B2Resource(
        application_key_id="b2-key-id",
        application_key="b2-secret",
        aws_access_key_id="aws-key-id",
        aws_secret_access_key="aws-secret",
    )
    assert b2.aws_access_key_id == "aws-key-id"
    assert b2.aws_secret_access_key == "aws-secret"


def test_only_application_key_id_provided() -> None:
    b2 = B2Resource(application_key_id="b2-key-id")
    assert b2.aws_access_key_id == "b2-key-id"
    assert b2.aws_secret_access_key is None


def test_only_application_key_provided() -> None:
    b2 = B2Resource(application_key="b2-secret")
    assert b2.aws_secret_access_key == "b2-secret"
    assert b2.aws_access_key_id is None


@mock_aws
def test_get_client_returns_working_boto3_client() -> None:
    # Pure constructor sanity check, no network calls.
    b2 = B2Resource(
        endpoint_url=None,
        region_name="us-east-1",
        application_key_id=MOTO_FAKE_KEY_ID,
        application_key=MOTO_FAKE_SECRET,
    )
    client = b2.get_client()
    assert hasattr(client, "put_object")
    assert hasattr(client, "get_object")
    assert hasattr(client, "create_bucket")


@mock_aws
def test_b2_resource_round_trip_put_get() -> None:
    # Override endpoint_url to None so moto can intercept at the AWS layer.
    # The endpoint_url semantics are covered separately by the non-mocked
    # tests above (test_b2_resource_default_endpoint_url and friends).
    b2 = B2Resource(
        endpoint_url=None,
        region_name="us-east-1",
        application_key_id=MOTO_FAKE_KEY_ID,
        application_key=MOTO_FAKE_SECRET,
    )
    client = b2.get_client()

    client.create_bucket(Bucket=BUCKET_NAME)
    client.put_object(Bucket=BUCKET_NAME, Key="hello", Body=b"world")

    response = client.get_object(Bucket=BUCKET_NAME, Key="hello")
    assert response["Body"].read() == b"world"


@mock_aws
def test_b2_user_agent_extra_attached_to_client() -> None:
    """Every client should carry the ``dagster-backblaze/{version}`` token.

    The suffix is resolved dynamically from the installed package version and
    appended to ``user_agent_extra`` (never replacing the botocore-built
    prefix). We override endpoint_url to None so moto can intercept; the suffix
    is independent of the endpoint URL.
    """
    suffix = _user_agent_suffix()
    assert suffix.startswith("dagster-backblaze/")

    b2 = B2Resource(
        endpoint_url=None,
        region_name="us-east-1",
        application_key_id=MOTO_FAKE_KEY_ID,
        application_key=MOTO_FAKE_SECRET,
    )
    client = b2.get_client()
    assert suffix in client.meta.config.user_agent_extra
    # The assembled UA string keeps the SDK-built prefix and appends our suffix.
    assert suffix in client.meta.config.user_agent
    assert "Botocore" in client.meta.config.user_agent


@mock_aws
def test_b2_resource_round_trip_with_custom_endpoint() -> None:
    # Same caveat as test_b2_resource_round_trip_put_get: we override
    # endpoint_url to None for the round-trip so moto can intercept normally,
    # then separately verify the custom endpoint value made it onto the
    # resource (which is what the test name promises).
    custom = "https://s3.eu-central-003.backblazeb2.com"
    b2_config_only = B2Resource(endpoint_url=custom)
    assert b2_config_only.endpoint_url == custom

    b2 = B2Resource(
        endpoint_url=None,
        region_name="us-east-1",
        application_key_id=MOTO_FAKE_KEY_ID,
        application_key=MOTO_FAKE_SECRET,
    )
    client = b2.get_client()
    client.create_bucket(Bucket=BUCKET_NAME)
    client.put_object(Bucket=BUCKET_NAME, Key="k", Body=b"v")
    assert client.get_object(Bucket=BUCKET_NAME, Key="k")["Body"].read() == b"v"
