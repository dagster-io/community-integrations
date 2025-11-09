import os
from collections.abc import Iterator
from typing import TYPE_CHECKING

import boto3
import moto
import moto.s3.responses
import pytest
import pytest_cases
from dagster._core.pipes.client import PipesContextInjector, PipesMessageReader
from dagster._core.pipes.utils import (
    PipesEnvContextInjector,
    PipesTempFileContextInjector,
)
from dagster_aws.pipes import PipesS3ContextInjector, PipesS3MessageReader
from moto.server import ThreadedMotoServer

import dagster_pipes_tests.cases
import dagster_pipes_tests.cases.context_injector
import dagster_pipes_tests.cases.message_reader
from dagster_pipes_tests.constants import PIPES_CONFIG
from dagster_pipes_tests.suite import PipesTestSuite

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client


@pytest.fixture(scope="module")
def aws_endpoint_url() -> Iterator[str]:
    if not PIPES_CONFIG.message_channel.s3:
        pytest.skip("message_channel.s3 is not enabled in pipes.toml")

    """Fixture to run a mocked AWS server for testing."""
    # Note: pass `port=0` to get a random free port.
    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server.get_host_and_port()
    os.environ["AWS_DEFAULT_REGION"] = moto.s3.responses.DEFAULT_REGION_NAME = (
        "us-east-1"
    )
    os.environ["AWS_REGION"] = moto.s3.responses.AWS_REGION = "us-east-1"
    url = f"http://{host}:{port}"
    os.environ["AWS_ENDPOINT_URL"] = url
    os.environ["AWS_ACCESS_KEY_ID"] = "foo"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "bar"
    yield url
    server.stop()

    for key in [
        "AWS_ENDPOINT_URL",
        "AWS_DEFAULT_REGION",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
    ]:
        del os.environ[key]


@pytest.fixture
def s3_client(aws_endpoint_url: str) -> "S3Client":
    return boto3.client("s3", endpoint_url=aws_endpoint_url)


BUCKET_NAME = "pipes-testing"


@pytest.fixture
def s3_bucket(s3_client: "S3Client") -> str:
    s3_client.create_bucket(Bucket=BUCKET_NAME)
    return BUCKET_NAME


@pytest_cases.fixture
@pytest_cases.parametrize_with_cases(
    "params", cases=dagster_pipes_tests.cases.context_injector
)
def context_injector(params) -> PipesContextInjector:
    if params["type"] == "env":
        return PipesEnvContextInjector()
    elif params["type"] == "tempfile":
        return PipesTempFileContextInjector()
    elif params["type"] == "s3":
        return PipesS3ContextInjector(client=params["client"], bucket=params["bucket"])
    else:
        raise ValueError(f"Unknown type: {params['type']}")


@pytest_cases.fixture
@pytest_cases.parametrize_with_cases(
    "params", cases=dagster_pipes_tests.cases.message_reader
)
def message_reader(params) -> PipesMessageReader | None:
    if params["type"] == "default":
        return None
    elif params["type"] == "s3":
        return PipesS3MessageReader(client=params["client"], bucket=params["bucket"])
    else:
        raise ValueError(f"Unknown type: {params['type']}")


__all__ = [
    "PIPES_CONFIG",
    "aws_endpoint_url",
    "s3_client",
    "s3_bucket",
    "context_injector",
    "message_reader",
    "PipesTestSuite",
]
