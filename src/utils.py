
from datetime import datetime, timezone


def get_current_timestamp_millis() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def format_currency(value):
    if value >= 1_000_000_000_000:
        return f"${value/1_000_000_000_000:.2f}T"
    if value >= 1_000_000_000:
        return f"${value/1_000_000_000:.2f}B"
    if value >= 1_000_000:
        return f"${value/1_000_000:.2f}M"
    elif value >= 1_000:
        return f"${value/1_000:.2f}K"
    else:
        return f"${value:.2f}"
