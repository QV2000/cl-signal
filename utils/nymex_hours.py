"""
NYMEX/CME CL trading hours helper.

CME WTI Crude Oil futures trade:
- Sunday 5:00 PM CT to Friday 4:00 PM CT
- Daily break 4:00-5:00 PM CT

In UTC:
- Sunday 23:00 UTC to Friday 22:00 UTC
- Daily break 22:00-23:00 UTC
"""
from datetime import datetime, timezone


def is_nymex_open(ts: datetime) -> bool:
    """
    Check if CME CL is actively trading at the given timestamp.

    Args:
        ts: Datetime to check. If naive, assumed UTC.

    Returns:
        True if CME CL is trading, False otherwise.
    """
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    else:
        ts = ts.astimezone(timezone.utc)

    weekday = ts.weekday()  # Monday=0, Sunday=6
    hour = ts.hour

    # Full weekend closure: Friday 22:00 UTC to Sunday 23:00 UTC
    if weekday == 5:  # Saturday
        return False
    if weekday == 6 and hour < 23:  # Sunday before 23:00 UTC
        return False
    if weekday == 4 and hour >= 22:  # Friday 22:00+ UTC
        return False

    # Daily break: 22:00-23:00 UTC (except Friday which is already closed)
    if hour == 22:
        return False

    return True


def is_weekend(ts: datetime) -> bool:
    """
    Check if we're in the full CME closure period.

    Friday 22:00 UTC to Sunday 23:00 UTC.

    Args:
        ts: Datetime to check. If naive, assumed UTC.

    Returns:
        True if in weekend closure, False otherwise.
    """
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    else:
        ts = ts.astimezone(timezone.utc)

    weekday = ts.weekday()
    hour = ts.hour

    # Saturday all day
    if weekday == 5:
        return True
    # Sunday before 23:00 UTC
    if weekday == 6 and hour < 23:
        return True
    # Friday 22:00+ UTC
    if weekday == 4 and hour >= 22:
        return True

    return False


def next_nymex_open(ts: datetime) -> datetime:
    """
    Get the next time NYMEX opens after the given timestamp.

    Args:
        ts: Starting datetime. If naive, assumed UTC.

    Returns:
        Datetime when NYMEX next opens.
    """
    from datetime import timedelta

    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    else:
        ts = ts.astimezone(timezone.utc)

    # If already open, return current time
    if is_nymex_open(ts):
        return ts

    weekday = ts.weekday()
    hour = ts.hour

    # During daily break (22:00-23:00)
    if hour == 22 and weekday not in (4, 5, 6):
        return ts.replace(hour=23, minute=0, second=0, microsecond=0)

    # Weekend: find Sunday 23:00
    if weekday == 5:  # Saturday
        days_until_sunday = 1
    elif weekday == 6:  # Sunday
        days_until_sunday = 0
    elif weekday == 4 and hour >= 22:  # Friday after close
        days_until_sunday = 2
    else:
        days_until_sunday = 0

    next_open = ts + timedelta(days=days_until_sunday)
    next_open = next_open.replace(hour=23, minute=0, second=0, microsecond=0)

    # Handle Sunday case
    if next_open.weekday() != 6:
        while next_open.weekday() != 6:
            next_open += timedelta(days=1)
        next_open = next_open.replace(hour=23, minute=0, second=0, microsecond=0)

    return next_open


if __name__ == "__main__":
    # Test cases
    from datetime import datetime

    test_times = [
        datetime(2024, 3, 18, 14, 0),   # Monday 14:00 UTC - open
        datetime(2024, 3, 18, 22, 30),  # Monday 22:30 UTC - daily break
        datetime(2024, 3, 18, 23, 30),  # Monday 23:30 UTC - open
        datetime(2024, 3, 22, 21, 0),   # Friday 21:00 UTC - open
        datetime(2024, 3, 22, 22, 30),  # Friday 22:30 UTC - closed (weekend)
        datetime(2024, 3, 23, 12, 0),   # Saturday 12:00 UTC - closed
        datetime(2024, 3, 24, 12, 0),   # Sunday 12:00 UTC - closed
        datetime(2024, 3, 24, 23, 30),  # Sunday 23:30 UTC - open
    ]

    for t in test_times:
        print(f"{t.strftime('%A %H:%M')} UTC: open={is_nymex_open(t)}, weekend={is_weekend(t)}")
