from datetime import datetime, timezone
import pandas as pd
from hyperliquid.utils.types import SpotAssetInfo
from hyperliquid.info import Info
from hyperliquid.utils import constants
import streamlit as st
import plotly.express as px

st.set_page_config(
    'Hyperliquid Tools',
    "🔧",
)

st.title("Unit Volume Tracker")
st.markdown("Input 1 or more addresses (comma-separated) to see combined volume across Hyperliquid Unit tokens")

# with caching and show_spinner=false
# even if this is wrapped around a spinner,
# that spinner only runs whenever data is NOT fetched from cache
# i.e. if data is actually fetched
# hence reducing unnecessary quick spinner upon fetching from cache
@st.cache_data(ttl=3600, show_spinner=False)
def get_cached_unit_token_mappings() -> dict[str, str]:
    info = Info(constants.MAINNET_API_URL, skip_ws=True)
    spot_metadata = info.spot_meta()

    unit_tokens = (t for t in spot_metadata['tokens']
                    if t.get('fullName') is not None and \
                    t['fullName'].startswith('Unit '))

    universe_metadata: list[SpotAssetInfo] = spot_metadata['universe']
    universe_metadata_idx = 0
    mapping = {}

    for t in unit_tokens:
        token_name = t['name']
        token_idx = t['index']

        universe_entry = None
        while universe_metadata_idx < len(universe_metadata):
            if token_idx in universe_metadata[universe_metadata_idx]['tokens']:
                universe_entry = universe_metadata[universe_metadata_idx]
                break
            universe_metadata_idx += 1

        if universe_entry:
            mapping[universe_entry['name']] = token_name
        else:
            print(f'unable to find pair metadata for token {token_name}, skipping processing')

    return mapping

with st.spinner('Initializing token mappings...'):
    unit_token_mappings = get_cached_unit_token_mappings()

@st.cache_data(ttl=60, show_spinner=False)
def get_cached_unit_volumes(addresses: list[str], unit_token_mappings: dict[str, str]) -> tuple[dict, str]:
    """
    get unit volumes with caching
    """
    info = Info(constants.MAINNET_API_URL, skip_ws=True)
    fills = []
    try:
        for address in addresses:
            fills.extend(info.post("/info", {
                "type": "userFills",
                "user": address,
                "aggregateByTime": True
            }))
    except Exception:
        return dict(), 'Unable to fetch trade history - did you put a valid set of addresses? If you copied your Liminal institutional subaccount address, remember to remove the "HL:" prefix'

    volume_by_token = {
        t: {
            'Buy': {
                'Last Updated': None,
                'Volume': 0.0,
            },
            'Sell': {
                'Last Updated': None,
                'Volume': 0.0,
            }
        }
        for t in unit_token_mappings.values()
    }

    for f in fills:
        coin = f['coin']
        direction = f['dir']
        if coin in unit_token_mappings.keys():
            token_name = unit_token_mappings[coin]
            trade_volume = float(f['sz']) * float(f['px'])
            trade_time = datetime.fromtimestamp(f['time'] / 1000, tz=timezone.utc)
            prev_last_updated = volume_by_token[token_name][direction]['Last Updated']
            if prev_last_updated is None or trade_time > prev_last_updated:
                volume_by_token[token_name][direction]['Last Updated'] = trade_time
            volume_by_token[token_name][direction]['Volume'] += trade_volume

    return volume_by_token, None

# --- data processing and display functions ---
def get_latest_txn_datetime(latest_buy: datetime | None, latest_sell: datetime | None):
    if latest_buy is None:
        return latest_sell
    if latest_sell is None:
        return latest_buy
    return max(latest_buy, latest_sell)

def create_volume_dataframe(volume_by_token: dict) -> pd.DataFrame:
    """
    converts volume_by_token dict to properly formatted df
    """
    records = []
    for token, volumes in volume_by_token.items():
        buys = volumes['Buy']
        buy_volume = buys['Volume']
        sells = volumes['Sell']
        sell_volume = sells['Volume']
        total_volume = buy_volume + sell_volume
        
        if total_volume > 0:
            records.append({
                'Token': token,
                'Buy Volume': buy_volume,
                'Latest Buy': buys['Last Updated'],
                'Sell Volume': sell_volume,
                'Latest Sell': sells['Last Updated'],
                'Total Volume': total_volume,
                'Last Transaction': get_latest_txn_datetime(buys['Last Updated'], sells['Last Updated']),
                'Buy %': (buy_volume / total_volume * 100) if total_volume > 0 else 0,
                'Sell %': (sell_volume / total_volume * 100) if total_volume > 0 else 0,
            })
    
    df = pd.DataFrame(records)
    if not df.empty:
        df = df.sort_values('Total Volume', ascending=False).reset_index(drop=True)
    return df

def format_currency(value):
    if value >= 1_000_000_000_000_000:
        return f"${value/1_000_000_000_000_000:.2f}Q"
    if value >= 1_000_000_000_000:
        return f"${value/1_000_000_000_000:.2f}T"
    if value >= 1_000_000_000:
        return f"${value/1_000_000_000:.2f}B"
    if value >= 1_000_000:
        return f"${value/1_000_000:.2f}M"
    elif value >= 1_000:
        return f"${value/1_000:.2f}K"
    else:
        return f"${value:.2f}"

def display_volume_table(df: pd.DataFrame):
    have_volume = not df.empty

    last_updated = datetime.now(timezone.utc)
    st.caption(f"Last updated: {last_updated.strftime('%Y-%m-%d %H:%M:%S UTC')}")

    if not have_volume:
        st.warning("No trade volumes on unit tokens found - if you think this is an error, contact me (details below)")

    # display metrics at top
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Volume", format_currency(df['Total Volume'].sum() if have_volume else 0.0))
    with col2:
        st.metric("Tokens Traded", len(df))
    with col3:
        most_traded = df.iloc[0]['Token'] if have_volume else "N/A"
        st.metric("Most Traded Token", most_traded)

    if not have_volume:
        return

    st.markdown("---")

    # format df for display
    display_df = df[['Token', 'Buy Volume', 'Sell Volume', 'Total Volume', 'Last Transaction']].copy()
    for col in ['Buy Volume', 'Sell Volume', 'Total Volume']:
        display_df[col] = display_df[col].apply(lambda x: f"${x:,.2f}")
    display_df['Last Transaction'] = display_df['Last Transaction'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S UTC'))
    
    # display the table
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            'Token': st.column_config.TextColumn('Token', width='small'),
            'Buy Volume': st.column_config.TextColumn('Buy Volume', width='small'),
            'Sell Volume': st.column_config.TextColumn('Sell Volume', width='small'),
            'Total Volume': st.column_config.TextColumn('Total Volume', width='small'),
            'Last Transaction': st.column_config.TextColumn('Last Transaction', width='medium'),
        }
    )

    # bar chart for buy / sell volume distribution
    if len(df) > 1:
        st.markdown("### Volume Distribution")
        fig = px.bar(
            df,
            x='Token',
            y=['Buy Volume', 'Sell Volume'],
            labels={'value': 'Volume (USD)', 'variable': 'Volume Type'}
        )
        fig.update_layout(
            barmode='stack',
            xaxis_title='Token',
            yaxis_title='Volume (USD)'
        )
        st.plotly_chart(fig, use_container_width=True)


# main app logic reruns upon any interaction
def main():
    st.info(f'Unit tokens: {", ".join(unit_token_mappings.values())}')

    col1, col2 = st.columns([5, 1])
    with col1:
        addresses_input: str = st.text_input(
            "Enter hyperliquid addresses, separated by comma",
            placeholder="Enter hyperliquid addresses, separated by comma",
            label_visibility='collapsed',
            key='hyperliquid_address_input',
        )
    with col2:
        submitted = st.button("Fetch Data", type="primary")

    # create placeholder that can be cleared and rewritten
    output_placeholder = st.empty()

    if submitted and addresses_input:
        # upon address(es) update, clear the placeholder immediately
        # so that loading spinner only shows up after
        output_placeholder.empty()

        addresses = [a.strip() for a  in addresses_input.split(",") if a]

        with st.spinner(f'Loading trade history for {", ".join(addresses)}...'):
            volume_by_token, err = get_cached_unit_volumes(addresses, unit_token_mappings)

        # create container within placeholder for new content
        with output_placeholder.container():
            if err is not None:
                st.error(err)
            else:
                df = create_volume_dataframe(volume_by_token)

                display_volume_table(df)

                # show raw data in expander
                if not df.empty:
                    with st.expander("Raw Data"):
                        st.json(volume_by_token)

if __name__ == '__main__':
    main()