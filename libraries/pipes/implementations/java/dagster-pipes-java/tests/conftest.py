from typing_extensions import TYPE_CHECKING
import pytest
from moto.server import ThreadedMotoServer
from typing import Iterator, Optional
import boto3
import subprocess
import pytest_cases
from dagster_aws.pipes import PipesS3ContextInjector
from dagster._core.pipes.utils import (
    PipesEnvContextInjector,
    PipesTempFileContextInjector,
)
from dagster_aws.pipes import PipesS3MessageReader
from dagster._core.pipes.client import PipesContextInjector, PipesMessageReader

import os
import moto
import moto.s3.responses

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client


@pytest.fixture(scope="session", autouse=True)
def built_jar():
    subprocess.run(["./gradlew", "build"], check=True)
