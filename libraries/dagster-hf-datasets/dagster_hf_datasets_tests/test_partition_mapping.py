import pytest

from dagster_hf_datasets._partitions._partition_mapping import (
    DEFAULT_CONFIG_PARTITION_KEY,
    DEFAULT_DATE_PARTITION_KEY,
    DEFAULT_REVISION_PARTITION_KEY,
    HFPartitionMapping,
)


# ============================================================
# Revision Mapping
# ============================================================


def test_from_revision():
    mapping = HFPartitionMapping.from_revision("main")

    assert mapping.partition_type == DEFAULT_REVISION_PARTITION_KEY

    assert mapping.value == "main"

    assert mapping.is_revision is True
    assert mapping.is_config is False
    assert mapping.is_date is False


def test_revision_partition_key():
    mapping = HFPartitionMapping.from_revision("v1.0")

    assert mapping.to_partition_key() == "revision:v1.0"


# ============================================================
# Config Mapping
# ============================================================


def test_from_config():
    mapping = HFPartitionMapping.from_config("qqp")

    assert mapping.partition_type == DEFAULT_CONFIG_PARTITION_KEY

    assert mapping.value == "qqp"

    assert mapping.is_revision is False
    assert mapping.is_config is True
    assert mapping.is_date is False


def test_config_partition_key():
    mapping = HFPartitionMapping.from_config("mnli")

    assert mapping.to_partition_key() == "config:mnli"


# ============================================================
# Date Mapping
# ============================================================


def test_from_date():
    mapping = HFPartitionMapping.from_date("2025-01-01")

    assert mapping.partition_type == DEFAULT_DATE_PARTITION_KEY

    assert mapping.value == "2025-01-01"

    assert mapping.is_revision is False
    assert mapping.is_config is False
    assert mapping.is_date is True


def test_date_partition_key():
    mapping = HFPartitionMapping.from_date("2025-01-01")

    assert mapping.to_partition_key() == "date:2025-01-01"


# ============================================================
# Partition Key Parsing
# ============================================================


@pytest.mark.parametrize(
    ("partition_key", "expected_type", "expected_value"),
    [
        (
            "revision:main",
            "revision",
            "main",
        ),
        (
            "config:qqp",
            "config",
            "qqp",
        ),
        (
            "date:2025-01-01",
            "date",
            "2025-01-01",
        ),
    ],
)
def test_from_partition_key(
    partition_key,
    expected_type,
    expected_value,
):
    mapping = HFPartitionMapping.from_partition_key(partition_key)

    assert mapping.partition_type == expected_type

    assert mapping.value == expected_value


# ============================================================
# Roundtrip Serialization
# ============================================================


@pytest.mark.parametrize(
    "mapping",
    [
        HFPartitionMapping.from_revision("main"),
        HFPartitionMapping.from_config("qqp"),
        HFPartitionMapping.from_date("2025-01-01"),
    ],
)
def test_partition_key_roundtrip(
    mapping,
):
    partition_key = mapping.to_partition_key()

    restored = HFPartitionMapping.from_partition_key(partition_key)

    assert restored == mapping


# ============================================================
# Invalid Partition Keys
# ============================================================


@pytest.mark.parametrize(
    "invalid_key",
    [
        "",
        "invalid",
        "missing_separator",
        "revision",
        "config",
    ],
)
def test_invalid_partition_key_raises(
    invalid_key,
):
    with pytest.raises(
        ValueError,
        match="Invalid partition key format",
    ):
        HFPartitionMapping.from_partition_key(invalid_key)


# ============================================================
# Multi-Colon Parsing
# ============================================================


def test_partition_key_preserves_extra_colons():
    mapping = HFPartitionMapping.from_partition_key("revision:refs/pr/1")

    assert mapping.partition_type == "revision"

    assert mapping.value == "refs/pr/1"
