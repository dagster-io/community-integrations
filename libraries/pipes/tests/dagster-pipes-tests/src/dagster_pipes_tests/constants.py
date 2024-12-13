import os
from pathlib import Path

from serde.toml import from_toml

from dagster_pipes_tests.pipes_config import PipesConfig

DEFAULT_PIPES_CONFIG_PATH = "pipes.toml"

PIPES_CONFIG_PATH = Path(os.getenv("PIPES_CONFIG_PATH", DEFAULT_PIPES_CONFIG_PATH))

PIPES_CONFIG = from_toml(PipesConfig, PIPES_CONFIG_PATH.read_text())


PACKAGE_DIR = Path(__file__).parent

DATA_DIR = PACKAGE_DIR / "data"
CASES_DIR = DATA_DIR / "cases"
STATIC_DIR = DATA_DIR / "static"

METADATA_CASES_PATH = CASES_DIR / "metadata.json"
CUSTOM_PAYLOAD_CASES_PATH = CASES_DIR / "custom_payload.json"

METADATA_PATH = STATIC_DIR / "metadata.json"
