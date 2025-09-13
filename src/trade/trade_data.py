import json
from logging import Logger
import os
from dotenv import load_dotenv
from hyperliquid.info import Info
import pandas as pd
import requests
from src.consts import unitStartTime
from src.utils.utils import get_today_timestamp_millis

def get_candlestick_data(info: Info, token_ids: list[str], token_names: list[str]) -> pd.DataFrame:
    rows = []

    for k, v in zip(token_ids, token_names):
        data = info.candles_snapshot(
            name=k,
            startTime=unitStartTime,    # use proper start time at midnight
            interval='1d',
            endTime=get_today_timestamp_millis(),
        )

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

# error handling done externally
def get_user_fills(account: str, startTime: int, endTime: int, logger: Logger):
    load_dotenv()

    fills_result = requests.post(
        "https://api.hydromancer.xyz/info",
        data=json.dumps({
            "type": "userFillsByTime",  # up to 10k total, then need to query from s3
            "user": account,
            "aggregateByTime": True,
            "startTime": startTime,
            "endTime": endTime,
        }),
        headers={
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {os.getenv("HYDROMANCER_API_KEY")}'
        },
    )

    fills_result.raise_for_status()

    return fills_result.json()
