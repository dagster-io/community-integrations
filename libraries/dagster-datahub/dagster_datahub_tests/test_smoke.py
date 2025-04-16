from pathlib import Path

from dagster_datahub.lib.datahub_recipe import DatahubRecipe
from dagster.components import ComponentLoadContext


def test_smoke(tmp_path: Path):
    ctx = ComponentLoadContext.for_test().for_path(tmp_path)

    (tmp_path / "recipe.dhub.yml").write_text(
        """
source:
  type: demo-data
  config: {}
        """
    )

    defs = DatahubRecipe(
        recipe_path="recipe.dhub.yml",
    ).build_defs(ctx)

    asset_keys = set(asset.key.to_user_string() for asset in defs.assets)  # type: ignore
    assert asset_keys == {
        "SampleHiveDataset",
        "fct_users_created",
        "project_root_events_logging_events_bckp",
        "SampleKafkaDataset",
        "fct_users_deleted",
        "SampleHdfsDataset",
        "logging_events",
    }
