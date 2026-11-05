"""Asset factories for the FREE-tier surface of the Live Tennis API.

Every asset built here stays inside the free tier (keyed; 30 requests/minute,
100 requests/day): upcoming fixtures, player search (players include their
current ranking), and a live-matches snapshot. A daily fixtures sync fits the
free-tier budget comfortably — a full fixtures materialization is typically
one to a few requests at the maximum page size of 200.

Completed-match **history** is a paid surface and is deliberately *not* built
as a default asset: ``client.list_completed_matches(...)`` and the per-match
point-by-point tape (``client.get_match_tape(...)``), the results archive and
head-to-head require a BASIC (or higher) key, market prices and the
rank-ordered rankings listing require PRO, and win probability requires ULTRA.
A call above your key's tier raises ``livetennisapi.UpgradeRequired`` — see
the README for a BASIC-tier history asset example you can opt into.
"""

from typing import Any

from dagster import (
    AssetExecutionContext,
    AssetsDefinition,
    Config,
    MetadataValue,
    asset,
)
from pydantic import Field

from dagster_livetennisapi.resource import LiveTennisApiResource


class PlayerSearchConfig(Config):
    """Run-time configuration for a players asset built by :func:`build_players_asset`."""

    search: str | None = Field(
        default=None,
        description=(
            "Name search passed to client.search_players(). Overrides the "
            "factory's default_search when set. Ranked players come first."
        ),
    )
    limit: int = Field(
        default=50,
        description="Maximum players to return (single request, max 200).",
    )


class LiveMatchesConfig(Config):
    """Run-time configuration for a live-matches asset built by :func:`build_live_matches_asset`."""

    tour: str | None = Field(
        default=None,
        description="Optional tour filter, e.g. 'atp' or 'wta'.",
    )
    limit: int = Field(
        default=200,
        description="Maximum matches to return (single request, max 200).",
    )


def build_fixtures_asset(
    *,
    name: str = "livetennisapi_fixtures",
    key_prefix: str | list[str] | None = None,
    group_name: str | None = None,
    page_size: int = 200,
    max_records: int | None = 1000,
) -> AssetsDefinition:
    """Build an asset that materializes upcoming scheduled fixtures (FREE tier).

    Pages through ``client.list_fixtures`` (earliest first) and returns the
    fixtures as a list of dicts. Suitable for a daily schedule: one full
    materialization is ``ceil(n_fixtures / page_size)`` requests against a
    100 request/day free-tier budget.

    Args:
        name: The asset name.
        key_prefix: Optional asset key prefix.
        group_name: Optional asset group.
        page_size: Page size for pagination (max 200).
        max_records: Stop after this many fixtures; ``None`` fetches all.

    Returns:
        AssetsDefinition: An asset requiring a ``livetennisapi`` resource.
    """

    @asset(
        name=name,
        key_prefix=key_prefix,
        group_name=group_name,
        description="Upcoming scheduled tennis fixtures from the Live Tennis API (FREE tier).",
    )
    def _fixtures_asset(
        context: AssetExecutionContext, livetennisapi: LiveTennisApiResource
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        with livetennisapi.get_client() as client:
            for fixture in client.paginate("list_fixtures", page_size=page_size):
                rows.append(fixture.to_dict())
                if max_records is not None and len(rows) >= max_records:
                    break
        context.add_output_metadata(
            {
                "dagster/row_count": len(rows),
                "preview": MetadataValue.json(rows[:5]),
            }
        )
        return rows

    return _fixtures_asset


def build_players_asset(
    *,
    name: str = "livetennisapi_players",
    key_prefix: str | list[str] | None = None,
    group_name: str | None = None,
    default_search: str | None = None,
) -> AssetsDefinition:
    """Build an asset that searches players by name (FREE tier).

    Calls ``client.search_players``; returned players include their current
    ranking. The search string can be set per-run via
    :class:`PlayerSearchConfig` (falling back to ``default_search``).

    Args:
        name: The asset name.
        key_prefix: Optional asset key prefix.
        group_name: Optional asset group.
        default_search: Search used when the run config does not provide one.

    Returns:
        AssetsDefinition: An asset requiring a ``livetennisapi`` resource.
    """

    @asset(
        name=name,
        key_prefix=key_prefix,
        group_name=group_name,
        description=(
            "Player search results (including current ranking) from the "
            "Live Tennis API (FREE tier)."
        ),
    )
    def _players_asset(
        context: AssetExecutionContext,
        config: PlayerSearchConfig,
        livetennisapi: LiveTennisApiResource,
    ) -> list[dict[str, Any]]:
        search = config.search if config.search is not None else default_search
        with livetennisapi.get_client() as client:
            page = client.search_players(search, limit=config.limit)
            rows = [player.to_dict() for player in page]
        context.add_output_metadata(
            {
                "dagster/row_count": len(rows),
                "search": MetadataValue.text(search or "(none)"),
            }
        )
        return rows

    return _players_asset


def build_live_matches_asset(
    *,
    name: str = "livetennisapi_live_matches",
    key_prefix: str | list[str] | None = None,
    group_name: str | None = None,
) -> AssetsDefinition:
    """Build an asset that snapshots the matches currently in play (FREE tier).

    Calls ``client.list_matches(status="live")`` once and returns the current
    live picture as a list of dicts. An optional tour filter can be set
    per-run via :class:`LiveMatchesConfig`.

    Note: only ``status="live"`` and ``status="upcoming"`` are FREE;
    ``status="completed"`` is the history surface and requires a BASIC+ key.

    Args:
        name: The asset name.
        key_prefix: Optional asset key prefix.
        group_name: Optional asset group.

    Returns:
        AssetsDefinition: An asset requiring a ``livetennisapi`` resource.
    """

    @asset(
        name=name,
        key_prefix=key_prefix,
        group_name=group_name,
        description="Snapshot of tennis matches currently in play (FREE tier).",
    )
    def _live_matches_asset(
        context: AssetExecutionContext,
        config: LiveMatchesConfig,
        livetennisapi: LiveTennisApiResource,
    ) -> list[dict[str, Any]]:
        with livetennisapi.get_client() as client:
            page = client.list_matches("live", tour=config.tour, limit=config.limit)
            rows = [match.to_dict() for match in page]
        context.add_output_metadata(
            {
                "dagster/row_count": len(rows),
                "tour": MetadataValue.text(config.tour or "(all)"),
            }
        )
        return rows

    return _live_matches_asset
