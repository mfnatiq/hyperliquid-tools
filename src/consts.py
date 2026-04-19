# assumes unit started when spot BTC started trading
from datetime import datetime, timezone
from typing import TypedDict, NotRequired


AccountStats = TypedDict('AccountStats', {
    'Name': str,
    'Num Trades': int,
    'Quote Fees': float,
    'Token Fees': float,
    'Remarks': NotRequired[str],
})

unit_start_date = datetime(2025, 2, 14, 0, 0, 0, tzinfo=timezone.utc)
unitStartTime = int(unit_start_date.timestamp() * 1000)

km_start_date = datetime(2026, 1, 12, 0, 0, 0, tzinfo=timezone.utc)
kinetiqStartTime = int(km_start_date.timestamp() * 1000)

oneDayInS = 60 * 60 * 24

acceptedPayments = {
    'USD₮0': {
        'address': '0xB8CE59FC3717ada4C02eaDF9682A9e934F625ebb',
        'minAmount': 20,
    },
    # handle specially (no SC calls needed so simply fetching value from txn)
    'HYPE': {
        'address': '0x0000000000000000000000000000000000000000',
        'minAmount': 0.3,
    },
}

NON_LOGGED_IN_TRADES_TOTAL = 10000