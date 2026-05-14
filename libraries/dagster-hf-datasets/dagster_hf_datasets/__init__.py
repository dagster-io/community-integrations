from dagster._core.libraries import DagsterLibraryRegistry
from .resources.huggingface_resource import HuggingFaceResource
from .assets.dataset_asset import hf_dataset_asset
from .assets.multi_asset import hf_multi_asset

__version__ = "0.0.1"

DagsterLibraryRegistry.register(
    "example-integration", __version__, is_dagster_package=False
)

__all__ = [
    "HuggingFaceResource",
    "hf_dataset_asset",
    "hf_multi_asset",
]