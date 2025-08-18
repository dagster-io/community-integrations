from datetime import datetime, timezone, timedelta


def get_epoch_time_ago(minutes: int) -> int:
    current_utc_datetime = datetime.now(timezone.utc)

    # Subtract x seconds from the current time
    time_ago_utc = current_utc_datetime - timedelta(minutes=minutes)

    # Convert the datetime object to an integer Unix timestamp (seconds since epoch)
    return int(time_ago_utc.timestamp())
