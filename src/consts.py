# assumes unit started when spot BTC started trading
from datetime import datetime, timezone

unit_start_date = datetime(2025, 2, 14, 0, 0, 0, tzinfo=timezone.utc)
unitStartTime = int(unit_start_date.timestamp() * 1000)