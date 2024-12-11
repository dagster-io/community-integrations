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
import tests.cases.context_injector as context_injector_cases
import tests.cases.message_reader as message_reader_cases

import os
import moto
import moto.s3.responses

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client


@pytest.fixture(scope="session", autouse=True)
def built_jar():
    subprocess.run(["./gradlew", "build"], check=True)


@pytest.fixture(scope="module")
def aws_endpoint_url() -> Iterator[str]:
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


BUCKET_NAME = "pies-testing"


@pytest.fixture
def s3_bucket(s3_client: "S3Client") -> str:
    s3_client.create_bucket(Bucket=BUCKET_NAME)
    return BUCKET_NAME


@pytest_cases.fixture
@pytest_cases.parametrize_with_cases("params", cases=context_injector_cases)
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
@pytest_cases.parametrize_with_cases("params", cases=message_reader_cases)
def message_reader(params) -> Optional[PipesMessageReader]:
    if params["type"] == "default":
        return None
    elif params["type"] == "s3":
        return PipesS3MessageReader(client=params["client"], bucket=params["bucket"])
    else:
        raise ValueError(f"Unknown type: {params['type']}")
