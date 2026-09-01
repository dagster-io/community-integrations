from dagster._core.libraries import DagsterLibraryRegistry

from dagster_livetennisapi.assets import (
    LiveMatchesConfig,
    PlayerSearchConfig,
    build_fixtures_asset,
    build_live_matches_asset,
    build_players_asset,
)
from dagster_livetennisapi.resource import LiveTennisApiResource

__all__ = [
    "LiveMatchesConfig",
    "LiveTennisApiResource",
    "PlayerSearchConfig",
    "build_fixtures_asset",
    "build_live_matches_asset",
    "build_players_asset",
]
__version__ = "0.0.1"

DagsterLibraryRegistry.register(
    "dagster-livetennisapi", __version__, is_dagster_package=False
)
