from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from src.consts import unitStartTime
from src.hl_client import HyperliquidClient
from src.utils.utils import get_today_timestamp_millis

def get_candlestick_data(info: HyperliquidClient, token_ids: list[str], token_names: list[str]) -> pd.DataFrame:
    def fetch(k: str) -> list:
        return info.candles_snapshot(
            name=k,
            startTime=unitStartTime,    # use proper start time at midnight
            interval='1d',
            endTime=get_today_timestamp_millis(),
        )

    # one call per token, independent of each other: run them together (map keeps token order)
    with ThreadPoolExecutor(max_workers=min(8, max(1, len(token_ids)))) as pool:
        fetched = list(pool.map(fetch, token_ids))

    rows = []
    for v, data in zip(token_names, fetched):
        for d in data:
            rows.append({
                'start_date': pd.Timestamp(d['t'], unit="ms"),
                'token_name': v,
                'close_price': float(d['c']),
                'volume_usd': float(d['c']) * float(d['v'])
            })

    candlestick_data = pd.DataFrame(rows)

    # sort by token_name and start_date for correct cumulative calculation
    candlestick_data = candlestick_data.sort_values(by=['token_name', 'start_date'])

    # calculate cumulative volume by token
    candlestick_data['cumulative_volume_usd'] = candlestick_data.groupby('token_name')[
        'volume_usd'].cumsum()

    return candlestick_data
