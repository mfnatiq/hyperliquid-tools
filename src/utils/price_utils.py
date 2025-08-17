from datetime import datetime, timezone
from logging import Logger
import ccxt
import streamlit as st

exch = ccxt.hyperliquid()

# consts
one_day_in_s = 60 * 60 * 24
start_date = datetime(2025, 2, 14, 0, 0, 0, tzinfo=timezone.utc)
start_timestamp = exch.parse8601(start_date.isoformat())
timeframe = '1d'

@st.cache_data(ttl=one_day_in_s, show_spinner=False)    # cache daily just to get OHLCV prices
def get_prices_cached(token_list: list[str], _logger: Logger) -> dict[str, dict[float, float]]:
    """
    output dict:
    { token: { date : close price } }
    
    uses close price for simplicity
    """
    end_date = datetime.now(tz=timezone.utc)
    num_days = (end_date - start_date).days + 1
    
    output = {
        t: {} for t in token_list
    }

    # note: pagination limit on hyperliquid fetch_ohlcv is 5k
    for token in token_list:
        symbol = f'{token}/USDC'
        ohlcv = exch.fetch_ohlcv(symbol, timeframe=timeframe, since=start_timestamp, limit=num_days)
        if ohlcv:
            _logger.info(f'fetched OHLCV prices for {symbol}')
            # timestamp / open / high / low / close / volume
            for timestamp, _, _, _, close_price, _ in ohlcv:
                output[token][timestamp] = close_price
        else:
            _logger.warning(f'no historical prices found for {symbol}, skipping')

    return output