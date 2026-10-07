import polars as pl
import pytest
import pytest_mock
from dagster import InputContext
from upath import UPath

from dagster_polars.io_managers.parquet import scan_parquet


def _scan_kwargs(
    mocker: pytest_mock.MockerFixture,
    monkeypatch: pytest.MonkeyPatch,
    polars_version: str,
    metadata: dict,
    storage_options: dict | None = None,
) -> dict:
    monkeypatch.setattr(pl, "__version__", polars_version)
    scan = mocker.patch.object(pl, "scan_parquet")
    context = mocker.MagicMock(InputContext)
    type(context).definition_metadata = mocker.PropertyMock(return_value=metadata)
    scan_parquet(UPath("/tmp/x.parquet"), context, storage_options=storage_options)
    return scan.call_args.kwargs


def test_polars_1_passes_rechunk_and_retries(
    mocker: pytest_mock.MockerFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    kwargs = _scan_kwargs(mocker, monkeypatch, "1.40.1", {"retries": 3})
    assert kwargs["rechunk"] is True
    assert kwargs["retries"] == 3
    assert kwargs["storage_options"] is None


def test_polars_2_omits_removed_arguments(
    mocker: pytest_mock.MockerFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    kwargs = _scan_kwargs(mocker, monkeypatch, "2.0.0", {})
    assert "rechunk" not in kwargs
    assert "retries" not in kwargs
    assert kwargs["storage_options"] is None


def test_polars_2_forwards_retries_to_storage_options(
    mocker: pytest_mock.MockerFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    kwargs = _scan_kwargs(
        mocker,
        monkeypatch,
        "2.0.0",
        {"retries": 3},
        storage_options={"key": "k"},
    )
    assert kwargs["storage_options"] == {"max_retries": 3, "aws_access_key_id": "k"}


def test_polars_2_storage_options_max_retries_wins(
    mocker: pytest_mock.MockerFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    kwargs = _scan_kwargs(
        mocker,
        monkeypatch,
        "2.0.0",
        {"retries": 3},
        storage_options={"max_retries": 5},
    )
    assert kwargs["storage_options"] == {"max_retries": 5}
