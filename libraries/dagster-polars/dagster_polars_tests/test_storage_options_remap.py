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
