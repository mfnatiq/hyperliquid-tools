# assumes unit started when spot BTC started trading
from datetime import datetime, timezone

# TODO check this isnt repeated once other PR is merged
unit_start_date = datetime(2025, 2, 14, tzinfo=timezone.utc)
unitStartTime = int(unit_start_date.timestamp() * 1000)