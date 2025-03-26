import pytest
from dagster import (
    DailyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
)


@pytest.fixture
def daily_partitions_definition() -> DailyPartitionsDefinition:
    return DailyPartitionsDefinition(start_date="2022-01-01", end_date="2022-01-10")


@pytest.fixture
def letter_partitions_definition() -> StaticPartitionsDefinition:
    return StaticPartitionsDefinition(["a", "b", "c"])


@pytest.fixture
def color_partitions_definition() -> StaticPartitionsDefinition:
    return StaticPartitionsDefinition(["red", "blue", "yellow"])


@pytest.fixture
def multi_partition_with_letter(
    daily_partitions_definition: DailyPartitionsDefinition,
    letter_partitions_definition: StaticPartitionsDefinition,
) -> MultiPartitionsDefinition:
    return MultiPartitionsDefinition(
        partitions_defs={
            "date": daily_partitions_definition,
            "letter": letter_partitions_definition,
        },
    )


@pytest.fixture
def multi_partition_with_color(
    daily_partitions_definition: DailyPartitionsDefinition,
    color_partitions_definition: StaticPartitionsDefinition,
) -> MultiPartitionsDefinition:
    return MultiPartitionsDefinition(
        partitions_defs={
            "date": daily_partitions_definition,
            "color": color_partitions_definition,
        },
    )
