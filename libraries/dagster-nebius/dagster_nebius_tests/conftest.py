from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import boto3
import pytest
from moto import mock_aws
from nebius.api.nebius.ai.v1 import JobSpec
from nebius.api.nebius.compute.v1 import DiskSpec


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client(
            "s3",
            region_name="us-east-1",
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
        )
        client.create_bucket(Bucket="pipes-test")
        yield client


@pytest.fixture
def spec():
    return JobSpec(
        image="example.invalid/gpu@sha256:abc",
        platform="gpu-l40s-a",
        preset="1gpu-8vcpu-32gb",
        subnet_id="subnet-test",
        disk=JobSpec.DiskSpec(
            type=DiskSpec.DiskType.NETWORK_SSD, size_bytes=100_000_000_000
        ),
        timeout=timedelta(minutes=10),
        restart_attempts=0,
    )


@pytest.fixture
def sdk():
    return SimpleNamespace(sync_close=Mock())
