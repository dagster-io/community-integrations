import datetime as dt

import pendulum
from pyiceberg import transforms


def date_diff(start: dt.datetime, end: dt.datetime) -> pendulum.Interval:
    """Compute an interval between two dates"""
    start_ = pendulum.instance(start)
    end_ = pendulum.instance(end)
    return end_ - start_


def diff_to_transformation(
    start: dt.datetime,
    end: dt.datetime,
) -> transforms.Transform:
    """Based on the interval between two dates, return a transformation"""
    delta = date_diff(start, end)
    match delta.in_hours():
        case 1:
            return transforms.HourTransform()
        case 24:
            return transforms.DayTransform()
        case 168:
            return transforms.DayTransform()  # No week transform available
        case _:
            if delta.in_months() == 1:
                return transforms.MonthTransform()
            raise NotImplementedError(
                f"Unsupported time window: {delta.in_words()}",
            )
