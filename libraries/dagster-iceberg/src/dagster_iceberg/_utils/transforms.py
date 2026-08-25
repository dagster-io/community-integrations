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
    end: dt.datetime
) -> transforms.Transform:
    """Based on the interval between two dates, return a transformation"""

    start_ = pendulum.instance(start)
    end_ = pendulum.instance(end)

    # calendar-aware checks (respect local time / DST)
    if start_.add(hours=1) == end_:
        return transforms.HourTransform()
    if start_.add(days=1) == end_:
        return transforms.DayTransform()
    if start_.add(weeks=1) == end_:
        return transforms.DayTransform()  # No week transform available
    if start_.add(months=1) == end_:
        return transforms.MonthTransform()

    # fallback: still check month-ish differences via in_months()
    if (end_ - start_).in_months() == 1:
        return transforms.MonthTransform()

    raise NotImplementedError(f"Unsupported time window: {(end_ - start_).in_words()}")
