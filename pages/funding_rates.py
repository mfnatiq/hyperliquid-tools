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
def apply_styles(val):
    """
    This function handles all styling logic in one place.
    - Returns grey color for NaN values.
    - Returns green color for positive numbers.
    - Returns red color for negative numbers.
    - Returns nothing for zeros or other data types.
    """
    # Check for NaN first, as it's not a number
    if pd.isna(val):
        # Use a muted grey for NaN values
        return 'color: #808080'
    
    # Now check for numeric types
    if isinstance(val, (int, float)):
        if val > 0:
            return 'color: #2E8B57'  # Green
        elif val < 0:
            return 'color: #C70039'  # Red
            
    # Return no specific style for other cases (like zero or text)
    return ''

with st.spinner(show_time=True):
    st.info("Symbols with differing names across exchanges like 1000BONK vs. kBONK have been normalised to e.g. BONK")

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
            .map(apply_styles, subset=exchange_cols)
            # Use .format() ONLY to change the display text for NaNs
            .format(na_rep="---", subset=exchange_cols),
        hide_index=True,
        # column_config={
        #     'user_rank': st.column_config.TextColumn('Rank'),
        #     'user_address': st.column_config.TextColumn('Address'),
        #     'total_volume_usd': st.column_config.TextColumn('Total Volume (USD)'),
        # },
    )

    # TODO convert all to percentages and mention is hourly
    # for all binance / hyperliquid / lighter, divide by 8 then x100 then add %
    # for pacifica, x100 then add % (alr in 1h)