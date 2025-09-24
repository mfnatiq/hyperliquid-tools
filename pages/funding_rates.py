import pandas as pd
import requests
import streamlit as st
from hyperliquid.info import Info
from hyperliquid.utils import constants

st.set_page_config(
    'Funding Rates',
    "⚖️",
    layout="wide",
)

st.title("Funding Rate Comparison (8H)")

# setup and configure logging
import logging
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

fetch_funding_rates_cache_duration_s = 60

# TODO put postprocessing of fetched data in the respective cached functions

# hyperliquid
try:
    info = Info(constants.MAINNET_API_URL, skip_ws=True)
except Exception as e:
    logger.error(f'client error occured: {e}')
    st.error(
        "Hyperliquid API rate limit reached, please try again in a short while")
@st.cache_data(ttl=fetch_funding_rates_cache_duration_s, show_spinner=False)
def get_hyperliquid_funding_rates():
    hyperliquid_data_raw = info.meta_and_asset_ctxs()
    parsed_hyperliquid = []
    if len(hyperliquid_data_raw) == 2:
        universe_data = hyperliquid_data_raw[0].get('universe', [])
        asset_contexts = hyperliquid_data_raw[1]

        # ensure both lists are of same length to avoid index errors
        if len(universe_data) == len(asset_contexts):
            for i, asset_info in enumerate(universe_data):
                symbol = asset_info.get('name')
                funding_rate_info = asset_contexts[i]
                funding_rate = funding_rate_info.get('funding')
                if symbol and funding_rate is not None:
                    try:
                        parsed_hyperliquid.append({
                            'symbol': symbol,
                            'hyperliquid': float(funding_rate)
                        })
                    except (ValueError, TypeError):
                        continue
    hyperliquid_df = pd.DataFrame(parsed_hyperliquid)
    return hyperliquid_df

@st.cache_data(ttl=fetch_funding_rates_cache_duration_s, show_spinner=False)
def get_lighter_funding_rates():
    # contains binance, bybit, hyperliquid, lighter
    # ignore hyperliquid one as we get that directly
    response = requests.get(
        "https://mainnet.zklighter.elliot.ai/api/v1/funding-rates",
        headers={ "accept": "application/json" },
    )
    return response.json()

@st.cache_data(ttl=fetch_funding_rates_cache_duration_s, show_spinner=False)
def get_pacifica_funding_rates():
    response = requests.get(
        "https://api.pacifica.fi/api/v1/info",
        headers={ "Accept": "*/*" },
    )
    return response.json()

@st.cache_data(ttl=fetch_funding_rates_cache_duration_s, show_spinner=False)
def get_extended_funding_rates():
    response = requests.get(
        "https://api.starknet.extended.exchange/api/v1/info/markets",
        headers={ "Accept": "*/*" },
    )
    return response.json()

@st.cache_data(ttl=fetch_funding_rates_cache_duration_s, show_spinner=False)
def get_paradex_funding_rates():
    response = requests.get(
        "https://api.prod.paradex.trade/v1/markets/summary?market=ALL",
        headers={ "Accept": "*/*" },
    )
    return response.json()

# --- 2. Styling Function ---
def apply_styles(val):
    """
    this function handles all styling logic in one place
    """
    # check for NaN first
    if pd.isna(val):
        return 'color: #808080' # muted grey

    # check for numeric types
    if isinstance(val, (int, float)):
        if val > 0:
            return 'color: #2E8B57'  # green
        elif val < 0:
            return 'color: #C70039'  # red

    # no specific style for other cases (like zero or text)
    return ''

with st.spinner(show_time=True):
    st.info("Symbols with differing names across exchanges like 1000BONK vs. kBONK have been normalised to e.g. BONK")

    lighter_funding_rates = get_lighter_funding_rates()['funding_rates']
    hyperliquid_df = get_hyperliquid_funding_rates()
    pacifica_funding_rates = get_pacifica_funding_rates()['data']
    # variational_funding_rates = get_variational_funding_rates()['result']
    extended_funding_rates = get_extended_funding_rates()['data']
    paradex_funding_rates = get_paradex_funding_rates()['results']

    # start with this
    combined_df = pd.DataFrame(lighter_funding_rates)

    # pivot df to get exchanges as columns
    combined_df = combined_df.pivot(index='symbol', columns='exchange', values='rate').reset_index()

    # drop hyperliquid from lighter data as it will be fetched separately
    combined_df = combined_df.drop(columns=['hyperliquid'])

    if not hyperliquid_df.empty:
        combined_df = pd.merge(combined_df, hyperliquid_df, on='symbol', how='outer')

    # parse pacifica data
    pacifica_df = pd.DataFrame(pacifica_funding_rates)
    pacifica_df = pacifica_df[['symbol', 'funding_rate']].rename(columns={'funding_rate': 'pacifica'})
    pacifica_df['pacifica'] = pacifica_df['pacifica'].astype(float)
    if not pacifica_df.empty:
        combined_df = pd.merge(combined_df, pacifica_df, on='symbol', how='outer')

    # process extended data
    parsed_extended_data = []
    for market in extended_funding_rates:
        # check if market is active and has necessary data
        if market.get('status') == 'ACTIVE' and 'marketStats' in market:
            asset_name = market.get('assetName')
            funding_rate = market['marketStats'].get('fundingRate')
            if asset_name and funding_rate is not None:
                parsed_extended_data.append({'symbol': asset_name, 'extended': float(funding_rate)})
    extended_df = pd.DataFrame(parsed_extended_data)
    if not extended_df.empty:
        combined_df = pd.merge(combined_df, extended_df, on='symbol', how='outer')

    # process paradex data
    parsed_paradex = []
    for market in paradex_funding_rates:
        symbol = market.get('symbol', '')
        # filter for perp markets only and ensure it has a funding rate
        if symbol.endswith('-PERP') and market.get('funding_rate') is not None:
            # extract base asset name e.g., "OM-USD-PERP" -> "OM"
            base_symbol = symbol.split('-')[0]
            try:
                # safely convert funding rate to float
                rate = float(market['funding_rate'])
                parsed_paradex.append({'symbol': base_symbol, 'paradex': rate})
            except (ValueError, TypeError):
                continue # skip if funding rate is not a valid number
    paradex_df = pd.DataFrame(parsed_paradex)
    if not paradex_df.empty:
        combined_df = pd.merge(combined_df, paradex_df, on='symbol', how='outer')

    # define desired column order
    exchange_cols = [
        'binance',
        'bybit',
        'lighter',
        'hyperliquid',
        'pacifica',
        'extended',
        'paradex'
    ]
    final_columns = ['symbol'] + exchange_cols

    # reindex df to include all desired columns, filling missing ones with NaN
    combined_df = combined_df.reindex(columns=final_columns)

    # create standardized 'symbol' column: removes '1000' or 'k' prefix from symbol names
    combined_df['symbol'] = combined_df['symbol'].str.replace(r'^(1000|k)', '', regex=True)

    # group by 'symbol' and aggregate results for exchange columns
    # .first() method takes the first non-null value for each exchange in the group.
    merged_df = combined_df.groupby('symbol')[exchange_cols].first()

    # reset the index to turn 'symbol' into a column
    final_df = merged_df.reset_index()

    # drop rows where all exchange values are NaN
    final_df = final_df.dropna(subset=exchange_cols, how='all')

    # lighter api, paradex return 8h rates
    formatters = {
        'binance': lambda x: f"{x * 100:.4f}%",
        'bybit': lambda x: f"{x * 100:.4f}%",
        'lighter': lambda x: f"{x * 100:.4f}%",
        'hyperliquid': lambda x: f"{(x * 8) * 100:.4f}%",  # data is for every 1h
        'pacifica': lambda x: f"{(x * 8) * 100:.4f}%",  # data is for every 1h
        'extended': lambda x: f"{(x * 8) * 100:.4f}%",  # data is for every 1h
        'paradex': lambda x: f"{x * 100:.4f}%",
    }

    st.dataframe(
        final_df.style
            # apply color styling function first
            .map(apply_styles, subset=exchange_cols)
            # use .format() to format data + change the display text for NaNs
            .format(formatters, na_rep="---", subset=exchange_cols),
        hide_index=True,
        # column_config={
        #     'user_rank': st.column_config.TextColumn('Rank'),
        #     'user_address': st.column_config.TextColumn('Address'),
        #     'total_volume_usd': st.column_config.TextColumn('Total Volume (USD)'),
        # },
    )

    # TODO check any pagination needed for any apis (variational?)