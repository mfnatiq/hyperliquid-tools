from hyperliquid.info import Info
from hyperliquid.utils import constants
from hyperliquid.utils.error import ClientError, ServerError
import pandas as pd
import requests
import streamlit as st

st.set_page_config(
    'Funding Rates',
    "⚖️",
    layout="wide",
)

st.title("Funding Rate Comparison")

# setup and configure logging
import logging
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# hyperliquid
try:
    hyperliquid_info = Info(constants.MAINNET_API_URL, skip_ws=True)
except Exception as e:
    logger.error(f'client error occured: {e}')
    st.error(
        "Hyperliquid API rate limit reached, please try again in a short while")

# TODO handle rate limit issues
@st.cache_data(ttl=60, show_spinner=False)
def get_hyperliquid_funding_rates():
    return hyperliquid_info.meta_and_asset_ctxs()

@st.cache_data(ttl=60, show_spinner=False)
def get_lighter_funding_rates():
    response = requests.get(
        "https://mainnet.zklighter.elliot.ai/api/v1/funding-rates",
        headers={ "accept": "application/json" },
    )
    return response.json()

@st.cache_data(ttl=60, show_spinner=False)
def get_pacifica_funding_rates():
    response = requests.get(
        "https://api.pacifica.fi/api/v1/info",
        headers={ "Accept": "*/*" },
    )
    return response.json()

# --- 2. Styling Function ---
# This function will be applied to each cell in the specified columns.
# It returns a CSS style string.
def style_funding_rates(val):
    """
    Applies color to funding rates:
    - Green for positive values
    - Red for negative values
    - Default (black) for zero or non-numeric types
    """
    color = ''
    # Check if the value is a number before comparing
    breakpoint()
    if isinstance(val, (int, float)):
        if val > 0:
            color = '#2E8B57'  # Green
        elif val < 0:
            color = '#C70039'  # Red
    return f'color: {color}'

with st.spinner(show_time=True):
    lighter_funding_rates = get_lighter_funding_rates()['funding_rates']
    pacifica_funding_rates = get_pacifica_funding_rates()['data']

    # Create a DataFrame from the first list of data
    df1 = pd.DataFrame(lighter_funding_rates)

    # Pivot the DataFrame to get exchanges as columns
    funding_rates_df = df1.pivot(index='symbol', columns='exchange', values='rate').reset_index()

    # Create a DataFrame from the second list of data for 'pacifica'
    pacifica_df = pd.DataFrame(pacifica_funding_rates)
    pacifica_df = pacifica_df[['symbol', 'funding_rate']].rename(columns={'funding_rate': 'pacifica'})
    pacifica_df['pacifica'] = pacifica_df['pacifica'].astype(float)


    # Merge the two DataFrames on the 'symbol'
    combined_df = pd.merge(funding_rates_df, pacifica_df, on='symbol', how='outer')

    # Define the desired column order
    numeric_cols = ['binance', 'hyperliquid', 'lighter', 'pacifica']
    final_columns = ['symbol'] + numeric_cols

    # Reindex the DataFrame to include all desired columns, filling missing ones with NaN
    combined_df = combined_df.reindex(columns=final_columns)

    # Display the resulting DataFrame
    print(combined_df)

    # 2. Create a standardized 'base_symbol' column
    # This removes the '1000' or 'k' prefix from the symbol names
    combined_df['base_symbol'] = combined_df['symbol'].str.replace(r'^(1000|k)', '', regex=True)

    # 3. Group by 'base_symbol' and aggregate the results for exchange columns
    # The .first() method takes the first non-null value for each exchange in the group.
    exchange_cols = ['binance', 'hyperliquid', 'lighter', 'pacifica']
    merged_df = combined_df.groupby('base_symbol')[exchange_cols].first()

    # 4. Finalize the DataFrame
    # Reset the index to turn 'base_symbol' into a column
    final_df = merged_df.reset_index()

    # Rename the 'base_symbol' column to 'symbol'
    final_df = final_df.rename(columns={'base_symbol': 'symbol'})

    # 5. Drop rows where ALL exchange values are NaN
    final_df = final_df.dropna(subset=exchange_cols, how='all')

    st.dataframe(
        final_df.style
            # Apply the color styling function first
            .map(style_funding_rates, subset=numeric_cols)
            # NOW, format ONLY the NaN values without changing number precision
            .format(na_rep="---", subset=numeric_cols),
        hide_index=True,
        # column_config={
        #     'user_rank': st.column_config.TextColumn('Rank'),
        #     'user_address': st.column_config.TextColumn('Address'),
        #     'total_volume_usd': st.column_config.TextColumn('Total Volume (USD)'),
        # },
    )