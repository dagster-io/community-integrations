"""Unit tests for the fsspec → object_store storage_options remap.

Issue: https://github.com/dagster-io/community-integrations/issues/257
"""

from dagster_polars.io_managers.parquet import _remap_fsspec_storage_options


def test_none_in_none_out() -> None:
    assert _remap_fsspec_storage_options(None) is None


def test_empty_in_none_out() -> None:
    assert _remap_fsspec_storage_options({}) is None


def test_remaps_basic_fsspec_keys() -> None:
    out = _remap_fsspec_storage_options({"key": "k", "secret": "s", "token": "t"})
    assert out == {
        "aws_access_key_id": "k",
        "aws_secret_access_key": "s",
        "aws_session_token": "t",
    }


def test_unwraps_client_kwargs() -> None:
    out = _remap_fsspec_storage_options(
        {
            "client_kwargs": {
                "region_name": "us-east-1",
                "endpoint_url": "http://localhost:5555",
            }
        }
    )
    assert out == {
        "aws_region": "us-east-1",
        "aws_endpoint_url": "http://localhost:5555",
    }


def test_drops_fsspec_only_keys() -> None:
    out = _remap_fsspec_storage_options(
        {
            "key": "k",
            "secret": "s",
            "client_options": {"timeout": 5},
            "config_kwargs": {"signature_version": "s3v4"},
            "default_block_size": 1024,
            "version_aware": True,
            "anon": False,
        }
    )
    assert out == {"aws_access_key_id": "k", "aws_secret_access_key": "s"}


def test_passes_object_store_keys_through() -> None:
    out = _remap_fsspec_storage_options(
        {"aws_access_key_id": "k", "aws_endpoint_url": "http://localhost:5555"}
    )
    assert out == {
        "aws_access_key_id": "k",
        "aws_endpoint_url": "http://localhost:5555",
    }


def test_mixed_fsspec_and_object_store_keys() -> None:
    """A user mid-migration may have a mix; both should land in the result."""
    out = _remap_fsspec_storage_options(
        {
            "key": "k",  # fsspec
            "aws_secret_access_key": "s",  # object_store
            "client_kwargs": {"region_name": "eu-west-2"},
        }
    )
    assert out == {
        "aws_access_key_id": "k",
        "aws_secret_access_key": "s",
        "aws_region": "eu-west-2",
    }


def test_only_fsspec_drop_keys_returns_none() -> None:
    """If every supplied key is fsspec-only and gets dropped, treat as empty."""
    assert (
        _remap_fsspec_storage_options(
            {"client_options": {"x": 1}, "default_cache_type": "readahead"}
        )
        is None
    )


def test_unknown_keys_are_dropped() -> None:
    """A typo or unsupported key should not reach object_store. Drop silently
    rather than handing an unknown key to the Rust binding."""
    out = _remap_fsspec_storage_options(
        {
            "key": "k",
            "secret": "s",
            "aws_acccess_key_id": "typo",  # triple-c typo
            "totally_made_up": True,
        }
    )
    assert out == {"aws_access_key_id": "k", "aws_secret_access_key": "s"}


def test_extracts_creds_and_session_token_from_client_kwargs() -> None:
    """fsspec users sometimes nest credentials under ``client_kwargs`` using
    the boto3-style names. Extract those too."""
    out = _remap_fsspec_storage_options(
        {
            "client_kwargs": {
                "aws_access_key_id": "k",
                "aws_secret_access_key": "s",
                "aws_session_token": "t",
                "region_name": "ap-south-1",
                "endpoint_url": "http://example.invalid",
            }
        }
    )
    assert out == {
        "aws_access_key_id": "k",
        "aws_secret_access_key": "s",
        "aws_session_token": "t",
        "aws_region": "ap-south-1",
        "aws_endpoint_url": "http://example.invalid",
    }


def test_verify_in_client_kwargs_is_dropped() -> None:
    """``verify`` is deliberately not mapped: object_store distinguishes
    ``aws_allow_http`` (plain HTTP) from ``allow_invalid_certificates``
    (TLS without cert checks), and boto3 ``verify`` overloads the bool /
    path-to-CA-bundle distinction. Drop it; users who need TLS tweaks can
    set the object_store key directly."""
    out = _remap_fsspec_storage_options(
        {
            "key": "k",
            "client_kwargs": {"verify": False, "endpoint_url": "http://x.invalid"},
        }
    )
    assert out == {
        "aws_access_key_id": "k",
        "aws_endpoint_url": "http://x.invalid",
    }


def test_passthrough_only_known_object_store_keys() -> None:
    """AWS-prefixed S3 keys and the backend-agnostic HTTP toggles are passed
    through. Unprefixed names like ``endpoint`` (which would route to a
    different object_store backend) are not on the allowlist and get dropped."""
    out = _remap_fsspec_storage_options(
        {
            "aws_skip_signature": "true",
            "allow_invalid_certificates": "true",
            "endpoint": "http://other.invalid",  # not on allowlist
        }
    )
    assert out == {
        "aws_skip_signature": "true",
        "allow_invalid_certificates": "true",
    }
