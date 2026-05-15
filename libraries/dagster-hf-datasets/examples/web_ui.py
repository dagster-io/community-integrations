from dagster import Definitions

from basic_asset import defs as basic_defs
from multi_asset import (
    defs as streaming_defs,
)
from dataset_pipeline import (
    defs as golden_defs,
)

defs = Definitions.merge(
    basic_defs,
    streaming_defs,
    golden_defs,
)
