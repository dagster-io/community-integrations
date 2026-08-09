from unittest.mock import MagicMock, patch

from dagster import EnvVar, asset, materialize_to_memory
from dagster._core.execution.context.init import build_init_resource_context

from dagster_adanos import AdanosResource


@patch("dagster_adanos.resource.AdanosClient")
def test_resource_configures_and_closes_client(mock_client: MagicMock) -> None:
    resource = AdanosResource(
        api_key="sk_live_test",
        base_url="https://sentiment.example.test",
        timeout=12.5,
    )
    context = build_init_resource_context()

    resource.setup_for_execution(context)

    assert resource.get_client() is mock_client.return_value
    mock_client.assert_called_once_with(
        api_key="sk_live_test",
        base_url="https://sentiment.example.test",
        timeout=12.5,
    )

    resource.teardown_after_execution(context)
    mock_client.return_value.close.assert_called_once_with()


@patch("dagster_adanos.resource.AdanosClient")
def test_resource_is_available_to_assets(mock_client: MagicMock) -> None:
    mock_client.return_value.reddit.trending.return_value = []

    @asset
    def reddit_sentiment(adanos: AdanosResource) -> list[object]:
        return adanos.get_client().reddit.trending(
            from_="2026-07-01",
            to="2026-07-07",
            limit=10,
        )

    result = materialize_to_memory(
        [reddit_sentiment],
        resources={"adanos": AdanosResource(api_key="sk_live_test")},
    )

    assert result.success
    mock_client.return_value.reddit.trending.assert_called_once_with(
        from_="2026-07-01",
        to="2026-07-07",
        limit=10,
    )
    mock_client.return_value.close.assert_called_once_with()


def test_resource_accepts_env_var() -> None:
    resource = AdanosResource(api_key=EnvVar("ADANOS_API_KEY"))

    assert resource.api_key == "ADANOS_API_KEY"
