from dagster import materialize

from dagster_livetennisapi import (
    LiveTennisApiResource,
    build_fixtures_asset,
    build_live_matches_asset,
    build_players_asset,
)
from dagster_livetennisapi_tests.helpers import (
    FIXTURES,
    LIVE_MATCHES,
    PLAYERS,
    MockLiveTennisApi,
)

RESOURCES = {"livetennisapi": LiveTennisApiResource(api_key="test-key")}


def _patch(monkeypatch) -> MockLiveTennisApi:
    mock_api = MockLiveTennisApi()
    monkeypatch.setattr(
        "dagster_livetennisapi.resource.LiveTennisAPI", mock_api.client_factory
    )
    return mock_api


def test_fixtures_asset_paginates_until_short_page(monkeypatch):
    mock_api = _patch(monkeypatch)

    # page_size=2 over 3 fixtures: a full page, then a short page.
    fixtures_asset = build_fixtures_asset(page_size=2)
    result = materialize([fixtures_asset], resources=RESOURCES)

    assert result.success
    rows = result.output_for_node("livetennisapi_fixtures")
    assert rows == FIXTURES
    offsets = [request.url.params.get("offset") for request in mock_api.requests]
    assert offsets == ["0", "2"]


def test_fixtures_asset_respects_max_records(monkeypatch):
    _patch(monkeypatch)

    fixtures_asset = build_fixtures_asset(name="capped_fixtures", max_records=1)
    result = materialize([fixtures_asset], resources=RESOURCES)

    assert result.success
    assert result.output_for_node("capped_fixtures") == FIXTURES[:1]


def test_players_asset_uses_factory_default_search(monkeypatch):
    mock_api = _patch(monkeypatch)

    players_asset = build_players_asset(default_search="sinner")
    result = materialize([players_asset], resources=RESOURCES)

    assert result.success
    rows = result.output_for_node("livetennisapi_players")
    assert rows == PLAYERS
    assert rows[0]["ranking"] == 1  # current ranking is on the FREE surface
    assert mock_api.requests[0].url.params.get("search") == "sinner"


def test_players_asset_run_config_overrides_search(monkeypatch):
    mock_api = _patch(monkeypatch)

    players_asset = build_players_asset(default_search="sinner")
    result = materialize(
        [players_asset],
        resources=RESOURCES,
        run_config={
            "ops": {
                "livetennisapi_players": {"config": {"search": "alcaraz", "limit": 5}}
            }
        },
    )

    assert result.success
    params = mock_api.requests[0].url.params
    assert params.get("search") == "alcaraz"
    assert params.get("limit") == "5"


def test_live_matches_asset_snapshots_live_status(monkeypatch):
    mock_api = _patch(monkeypatch)

    live_asset = build_live_matches_asset()
    result = materialize([live_asset], resources=RESOURCES)

    assert result.success
    rows = result.output_for_node("livetennisapi_live_matches")
    assert rows == LIVE_MATCHES
    params = mock_api.requests[0].url.params
    assert params.get("status") == "live"
    assert params.get("tour") is None


def test_live_matches_asset_tour_filter_via_run_config(monkeypatch):
    mock_api = _patch(monkeypatch)

    live_asset = build_live_matches_asset()
    result = materialize(
        [live_asset],
        resources=RESOURCES,
        run_config={"ops": {"livetennisapi_live_matches": {"config": {"tour": "wta"}}}},
    )

    assert result.success
    assert mock_api.requests[0].url.params.get("tour") == "wta"
