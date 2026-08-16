from livetennisapi import Fixture

from dagster_livetennisapi import LiveTennisApiResource
from dagster_livetennisapi_tests.helpers import FIXTURES, MockLiveTennisApi


def test_get_client_calls_sdk_with_configured_key(monkeypatch):
    mock_api = MockLiveTennisApi()
    monkeypatch.setattr(
        "dagster_livetennisapi.resource.LiveTennisAPI", mock_api.client_factory
    )

    resource = LiveTennisApiResource(api_key="test-key")
    with resource.get_client() as client:
        page = client.list_fixtures(limit=10)

    assert len(page) == len(FIXTURES)
    assert isinstance(page[0], Fixture)
    assert page[0].player1_name == "Jannik Sinner"

    # The SDK sent the configured key as a bearer token.
    assert mock_api.requests[0].headers["Authorization"] == "Bearer test-key"


def test_get_client_closes_client_on_exit(monkeypatch):
    mock_api = MockLiveTennisApi()
    monkeypatch.setattr(
        "dagster_livetennisapi.resource.LiveTennisAPI", mock_api.client_factory
    )

    resource = LiveTennisApiResource(api_key="test-key")
    with resource.get_client() as client:
        assert not client._client.is_closed
    assert client._client.is_closed


def test_resource_forwards_connection_settings(monkeypatch):
    seen: dict = {}
    mock_api = MockLiveTennisApi()

    def recording_factory(**kwargs):
        seen.update(kwargs)
        return mock_api.client_factory(**kwargs)

    monkeypatch.setattr(
        "dagster_livetennisapi.resource.LiveTennisAPI", recording_factory
    )

    resource = LiveTennisApiResource(
        api_key="test-key",
        base_url="https://example.invalid/api",
        timeout=5.0,
        max_retries=0,
    )
    with resource.get_client() as client:
        assert client.base_url == "https://example.invalid/api"

    assert seen == {
        "api_key": "test-key",
        "base_url": "https://example.invalid/api",
        "timeout": 5.0,
        "max_retries": 0,
    }
