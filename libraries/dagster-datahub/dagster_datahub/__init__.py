from dagster._core.libraries import DagsterLibraryRegistry

from dagster_datahub.lib.datahub_recipe import DatahubRecipe

__version__ = "0.1.5"

__all__ = [DatahubRecipe]

DagsterLibraryRegistry.register("dagster-datahub", __version__)
