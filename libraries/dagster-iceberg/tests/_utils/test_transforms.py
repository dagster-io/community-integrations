import datetime as dt

import pytest
from pyiceberg import transforms as iceberg_transforms

from dagster_iceberg._utils import transforms


@pytest.mark.parametrize(
    ("start", "end", "expected_transformation"),
    [
        (
            dt.datetime(2023, 1, 1, 0, 0, 0),
            dt.datetime(2023, 1, 1, 1, 0, 0),
            iceberg_transforms.HourTransform(),
        ),
        (
            dt.datetime(2023, 1, 1, 0, 0, 0),
            dt.datetime(2023, 1, 2, 0, 0, 0),
            iceberg_transforms.DayTransform(),
        ),
        (
            dt.datetime(2023, 1, 1, 0, 0, 0),
            dt.datetime(2023, 1, 8, 0, 0, 0),
            iceberg_transforms.DayTransform(),
        ),
        (
            dt.datetime(2023, 1, 1, 0, 0, 0),
            dt.datetime(2023, 2, 1, 0, 0, 0),
            iceberg_transforms.MonthTransform(),
        ),
    ],
)
def test_diff_to_transformation(
    start: dt.datetime,
    end: dt.datetime,
    expected_transformation: iceberg_transforms.Transform,
):
    transformation = transforms.diff_to_transformation(
        start=start,
        end=end,
    )
    assert transformation == expected_transformation


def test_diff_to_transformation_fails():
    with pytest.raises(NotImplementedError):
        transforms.diff_to_transformation(
            start=dt.datetime(2023, 1, 1, 0, 0, 0),
            end=dt.datetime(2023, 1, 1, 0, 0, 1),
        )
