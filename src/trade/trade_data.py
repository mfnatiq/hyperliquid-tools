from hyperliquid.info import Info
import pandas as pd
from consts import unitStartTime
from utils import get_current_timestamp_millis

# TODO need to update once other PR is merged
def get_cumulative_trade_data(info: Info, unit_token_mappings: dict[str, str]) -> pd.DataFrame:
    currTime = get_current_timestamp_millis()

    rows = []

    for k, v in unit_token_mappings.items():
        # TODO can replace ccxt with this? or better to use separately
        # if keeping ccxt, use volume data from there instead
        data = info.candles_snapshot(
            name=k,
            startTime=unitStartTime,
            interval='1d',
            endTime=currTime,
        )

        for d in data:
            rows.append({
                'start_date': pd.Timestamp(d['t'], unit="ms"),
                'token_name': v,
                'volume_usd': float(d['c']) * float(d['v'])
            })

    volume_data = pd.DataFrame(rows)

    # sort by token_name and start_date for correct cumulative calculation
    volume_data = volume_data.sort_values(by=['token_name', 'start_date'])

    # calculate cumulative volume by token
    volume_data['cumulative_volume_usd'] = volume_data.groupby('token_name')['volume_usd'].cumsum()
    
    return volume_data
