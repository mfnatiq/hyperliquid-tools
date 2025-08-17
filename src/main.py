from datetime import datetime, timedelta, timezone
import pandas as pd
from hyperliquid.utils.types import SpotAssetInfo
from hyperliquid.info import Info
from hyperliquid.utils import constants
import streamlit as st
import plotly.express as px
from bridge.unit_bridge_api import UnitBridgeInfo
from utils.price_utils import get_prices_cached
from utils.render_utils import footer_html

# setup and configure logging
import logging
from bridge.unit_bridge_utils import create_bridge_summary, process_bridge_operations
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

from utils.utils import DATE_FORMAT, format_currency, get_current_timestamp_millis

st.set_page_config(
    'Hyperliquid Tools',
    "🔧",
    layout="wide",
)

st.title("Unit Volume Tracker")

# region sticky footer
# put up here so container emptying doesn't make footer flash
# render footer
st.markdown(footer_html, unsafe_allow_html=True)
# endregion

info = Info(constants.MAINNET_API_URL, skip_ws=True)
unit_bridge_info = UnitBridgeInfo()

# with caching and show_spinner=false
# even if this is wrapped around a spinner,
# that spinner only runs whenever data is NOT fetched from cache
# i.e. if data is actually fetched
# hence reducing unnecessary quick spinner upon fetching from cache
@st.cache_data(ttl=3600, show_spinner=False)
def get_cached_unit_token_mappings() -> dict[str, tuple[str, int]]:
    spot_metadata = info.spot_meta()

    unit_tokens = (t for t in spot_metadata['tokens']
                    if t.get('fullName') is not None and
                    t['fullName'].startswith('Unit '))

    universe_metadata: list[SpotAssetInfo] = spot_metadata['universe']
    universe_metadata_idx = 0
    mapping = {}

    for t in unit_tokens:
        token_name = t['name']
        token_idx = t['index']

        try:
            # used for bridge
            token_decimals = int(t['weiDecimals'])
            if t['evmContract'] is not None:
                token_decimals += int(t['evmContract']['evm_extra_wei_decimals'])
        except Exception:
            # skip
            logger.warning(f'skipping as unable to find decimals info for {token_name}: {t}')
            continue

        universe_entry = None
        while universe_metadata_idx < len(universe_metadata):
            if token_idx in universe_metadata[universe_metadata_idx]['tokens']:
                universe_entry = universe_metadata[universe_metadata_idx]
                break
            universe_metadata_idx += 1

        if universe_entry:
            mapping[universe_entry['name']] = (token_name, token_decimals)
        else:
            logging.info(
                f'unable to find pair metadata for token {token_name}, skipping processing')

    return mapping

with st.spinner('Initialising...'):
    unit_token_mappings = get_cached_unit_token_mappings()
    token_list = [t for t, _ in unit_token_mappings.values()]
    prices = get_prices_cached(token_list, logger)

st.markdown(
    "Input 1 or more accounts (comma-separated)")


# region optimisations
@st.cache_data(ttl=3600, show_spinner=False)
def get_subaccounts_cached(account: str) -> list:
    subaccounts = info.query_sub_accounts(account)
    return subaccounts if subaccounts is not None else []
# endregion

# assumes unit started when spot BTC started trading
unit_start_date = datetime(2025, 2, 14, tzinfo=timezone.utc)

@st.cache_data(ttl=60, show_spinner=False)
def get_cached_unit_volumes(accounts: list[str], unit_token_mappings: dict[str, tuple[str, int]], exclude_subaccounts: bool = False):
    """
    get unit volumes with caching
    """
    # account: { remarks (subaccount of another), num fills }
    accounts_mapping: dict[str, dict[str, int]] = dict()

    for account in accounts:
        accounts_mapping[account] = {
            "Name": "",
            "Remarks": "",
            "Num Trades": 0,
            "Token Fees": 0.0,
            "USDC Fees": 0.0,
        }
        if not exclude_subaccounts:
            subaccounts = get_subaccounts_cached(account)
            for sub in subaccounts:
                subaccount = sub['subAccountUser']

                accounts_mapping[subaccount] = {
                    "Name": sub['name'],
                    "Remarks": f"Subaccount of {account[:6]}...",
                    "Num Trades": 0,
                    "Token Fees": 0.0,
                    "USDC Fees": 0.0,
                }

    accounts_to_query = accounts_mapping.keys()

    volume_by_token = {
        t: {
            'Direction': {
                'Buy': {
                    'First Txn': None,
                    'Last Txn': None,
                    'Volume': 0.0,
                },
                'Sell': {
                    'First Txn': None,
                    'Last Txn': None,
                    'Volume': 0.0,
                }
            },
            'Type': {
                'Maker': {
                    'First Txn': None,
                    'Last Txn': None,
                    'Volume': 0.0,
                },
                'Taker': {
                    'First Txn': None,
                    'Last Txn': None,
                    'Volume': 0.0,
                }
            },
            'Token Fees': 0.0,
            'USDC Fees': 0.0,
            'Num Trades': 0,
        }
        for t, _ in unit_token_mappings.values()
    }

    fills = dict()  # { account: list of fills }
    currTime = get_current_timestamp_millis()
    numDaysQuerySpan = 30
    try:
        for account in accounts_to_query:
            num_fills_total = 0
            account_fills = []

            # initial values
            startTime = int(unit_start_date.timestamp() * 1000)

            # seems like 2k limit for endpoint counts from the start
            # so start from overall start time then move up til currtime
            while startTime < currTime: # check back until this date
                endTime = startTime + int(timedelta(days=numDaysQuerySpan).total_seconds()) * 1000
                logging.info(f'querying for {account} startTime: {startTime}; endTime: {endTime}')

                fills_result = info.post("/info", {
                    "type": "userFillsByTime",  # up to 10k total, then need to query from s3
                    "user": account,
                    "aggregateByTime": True,
                    "startTime": startTime,
                    "endTime": endTime,
                })
                logging.info(f'num fills: {len(fills_result)}')

                # query again til no more
                num_fills = len(fills_result)
                num_fills_total += num_fills

                # logic:
                # 1) if hit limit (2k fills per api call), set start = latest fill timestamp + 1
                # 2) else, slide window fully i.e. start = end + 1
                # always set endTime as startTime + interval
                if num_fills == 2000:
                    latest_fill_timestamp = max(f['time'] for f in fills_result)
                    startTime = latest_fill_timestamp + 1
                else:
                    startTime = endTime + 1

                account_fills.extend(fills_result)
            fills[account] = account_fills
    except Exception as e:
        logging.error(e)
        return dict(), dict(), [], 'Unable to fetch trade history - did you put a valid list of accounts?'

    # 10k - need get from s3
    accounts_hitting_fills_limits = []

    for account, fills_list in fills.items():
        if len(fills_list) == 10000:
            accounts_hitting_fills_limits.append(account)
        
        for f in fills_list:
            coin = f['coin']
            direction = f['dir']
            if coin in unit_token_mappings.keys() and direction in ['Buy', 'Sell']:
                token_name, _ = unit_token_mappings[coin]
                
                # only count unit fills
                volume_by_token[token_name]['Num Trades'] += 1
                accounts_mapping[account]['Num Trades'] += 1

                price = float(f['px'])

                trade_volume = float(f['sz']) * price
                trade_time = datetime.fromtimestamp(
                    f['time'] / 1000, tz=timezone.utc)
                
                # direction
                prev_first_txn = volume_by_token[token_name]['Direction'][direction]['First Txn']
                if prev_first_txn is None or trade_time < prev_first_txn:
                    volume_by_token[token_name]['Direction'][direction]['First Txn'] = trade_time
                prev_last_txn = volume_by_token[token_name]['Direction'][direction]['Last Txn']
                if prev_last_txn is None or trade_time > prev_last_txn:
                    volume_by_token[token_name]['Direction'][direction]['Last Txn'] = trade_time
                volume_by_token[token_name]['Direction'][direction]['Volume'] += trade_volume
                
                # trade type (maker / taker)
                trade_type = 'Taker' if f['crossed'] is True else 'Maker'
                prev_first_txn = volume_by_token[token_name]['Type'][trade_type]['First Txn']
                if prev_first_txn is None or trade_time < prev_first_txn:
                    volume_by_token[token_name]['Type'][trade_type]['First Txn'] = trade_time
                prev_last_txn = volume_by_token[token_name]['Type'][trade_type]['Last Txn']
                if prev_last_txn is None or trade_time > prev_last_txn:
                    volume_by_token[token_name]['Type'][trade_type]['Last Txn'] = trade_time
                volume_by_token[token_name]['Type'][trade_type]['Volume'] += trade_volume

                fee_in_quote = f['feeToken'] == 'USDC'
                if fee_in_quote:
                    fee_amt = float(f['fee'])
                    accounts_mapping[account]['USDC Fees'] += fee_amt
                    volume_by_token[token_name]['USDC Fees'] += fee_amt
                else:
                    fee_amt = float(f['fee']) * price
                    accounts_mapping[account]['Token Fees'] += fee_amt
                    volume_by_token[token_name]['Token Fees'] += fee_amt

    return volume_by_token, accounts_mapping, accounts_hitting_fills_limits, None

# --- data processing and display functions ---
def get_earliest_txn_datetime(earliest_buy: datetime | None, earliest_sell: datetime | None):
    if earliest_buy is None:
        return earliest_sell
    if earliest_sell is None:
        return earliest_buy
    return min(earliest_buy, earliest_sell)

def get_latest_txn_datetime(latest_buy: datetime | None, latest_sell: datetime | None):
    if latest_buy is None:
        return latest_sell
    if latest_sell is None:
        return latest_buy
    return max(latest_buy, latest_sell)

def create_volume_df(volume_by_token: dict) -> pd.DataFrame:
    """
    converts volume_by_token dict to properly formatted df
    """
    records = []
    for token, volumes in volume_by_token.items():
        buys = volumes['Direction']['Buy']
        buy_volume = buys['Volume']
        sells = volumes['Direction']['Sell']
        sell_volume = sells['Volume']
        total_volume = buy_volume + sell_volume

        maker = volumes['Type']['Maker']
        maker_volume = maker['Volume']
        taker = volumes['Type']['Taker']
        taker_volume = taker['Volume']

        if total_volume > 0:
            records.append({
                'Token': token,

                'Buy Volume': buy_volume,
                'First Buy': buys['First Txn'],
                'Last Buy': buys['Last Txn'],
                'Sell Volume': sell_volume,
                'First Sell': sells['First Txn'],
                'Last Sell': sells['Last Txn'],

                'Total Volume': total_volume,
                
                'Maker Volume': maker_volume,
                'First Maker Txn': maker['First Txn'],
                'Last Maker Txn': maker['Last Txn'],
                'Taker Volume': taker_volume,
                'First Taker Txn': taker['First Txn'],
                'Last Taker Txn': taker['Last Txn'],
                
                'Token Fees': volumes['Token Fees'],
                'USDC Fees': volumes['USDC Fees'],
                'Num Trades': volumes['Num Trades'],
                'First Trade': get_earliest_txn_datetime(buys['First Txn'], sells['First Txn']),
                'Last Trade': get_latest_txn_datetime(buys['Last Txn'], sells['Last Txn']),
            })

    df = pd.DataFrame(records)
    if not df.empty:
        df = df.sort_values(
            'Total Volume', ascending=False).reset_index(drop=True)
    return df


def display_trade_volume_table(df: pd.DataFrame, num_accounts: int):
    have_volume = not df.empty

    if not have_volume:
        st.warning(
            "No trades on Unit tokens found - if you think this is an error, contact me")

    # display metrics at top
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Trade Volume", format_currency(
            df['Total Volume'].sum() if have_volume else 0.0))
    with col2:
        most_traded = df.iloc[0]['Token'] if have_volume else "N/A"
        st.metric("Top Traded Token", most_traded)
    with col3:
        st.metric("Tokens Traded", len(df))

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Accounts Traded On", num_accounts)
    with col2:
        total_fees = 0.0
        if have_volume:
            df['Total Fees'] = df['Token Fees'] + df['USDC Fees']
            total_fees = df['Total Fees'].sum()
        st.metric('Total Fees Paid', format_currency(total_fees))
    with col3:
        st.metric('Total Trades Made', df['Num Trades'].sum() if have_volume else 0)

    if not have_volume:
        return

    st.markdown("---")

    # format df for display
    display_df = df[['Token', 'Buy Volume', 'Sell Volume',
                    'Total Volume', 'Total Fees', 'First Trade', 'Last Trade', 'Maker Volume', 'Taker Volume']].copy()
    for col in ['Buy Volume', 'Sell Volume', 'Total Volume', 'Total Fees', 'Maker Volume', 'Taker Volume']:
        display_df[col] = display_df[col].apply(lambda x: format_currency(x))
    display_df['First Trade'] = display_df['First Trade'].apply(
        lambda x: x.strftime(DATE_FORMAT))
    display_df['Last Trade'] = display_df['Last Trade'].apply(
        lambda x: x.strftime(DATE_FORMAT))

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
            'First Trade': st.column_config.TextColumn('First Trade', width='medium'),
            'Last Trade': st.column_config.TextColumn('Last Trade', width='medium'),
            'Maker Volume': st.column_config.TextColumn('Maker Volume', width='small'),
            'Taker Volume': st.column_config.TextColumn('Taker Volume', width='small'),
        }
    )

    # bar chart for volume distribution
    if len(df) > 1:
        st.markdown("### Volume Distribution")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # txn side distribution
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
            
        with col2:
            # txn type distribution (maker / taker)
            fig = px.bar(
                df,
                x='Token',
                y=['Maker Volume', 'Taker Volume'],
                labels={'value': 'Volume (USD)', 'variable': 'Volume Type'}
            )
            fig.update_layout(
                barmode='stack',
                xaxis_title='Token',
                yaxis_title='Volume (USD)'
            )
            st.plotly_chart(fig, use_container_width=True)

def display_accounts_table(accounts_df: pd.DataFrame):
    st.dataframe(
        accounts_df,
        use_container_width=True,
        hide_index=True,
    )

# main app logic reruns upon any interaction
def main():
    st.info(f'Unit tokens: {", ".join(token_list)}')

    col1, col2, col3 = st.columns([10, 2, 1])
    with col1:
        addresses_input: str = st.text_input(
            "Enter hyperliquid accounts, separated by comma",
            placeholder="Enter hyperliquid accounts, separated by comma",
            label_visibility='collapsed',
            key='hyperliquid_address_input',
        )
    with col2:
        exclude_subaccounts = st.checkbox("Exclude Subaccounts")
    with col3:
        submitted = st.button("Run", type="primary")

    # create placeholder that can be cleared and rewritten
    output_placeholder = st.empty()

    if submitted and addresses_input:
        # upon account(es) update, clear the placeholder immediately
        # so that loading spinner only shows up after
        output_placeholder.empty()

        accounts = [a.strip() for a in addresses_input.split(",") if a]

        with st.spinner(f'Loading data for {", ".join(accounts)}...'):
            volume_by_token, accounts_mapping, accounts_hitting_fills_limits, err = get_cached_unit_volumes(
                accounts, unit_token_mappings, exclude_subaccounts)
            
            raw_bridge_data = unit_bridge_info.get_operations(accounts)

        with output_placeholder.container():
            if err is not None:
                st.error(err)
            else:
                # store results in session state so they persist across tab switches
                st.session_state['volume_data'] = {
                    'volume_by_token': volume_by_token,
                    'accounts_mapping': accounts_mapping,
                    'accounts_hitting_fills_limits': accounts_hitting_fills_limits,
                    'last_updated': datetime.now(timezone.utc),
                    'accounts': accounts,
                }
                st.session_state['raw_bridge_data'] = raw_bridge_data

    # show content in output placeholder only if have data
    if 'volume_data' in st.session_state and 'raw_bridge_data' in st.session_state:
        with output_placeholder.container():
            volume_data = st.session_state['volume_data']
            raw_bridge_data = st.session_state['raw_bridge_data']

            st.caption(f"Last updated: {volume_data['last_updated'].strftime(DATE_FORMAT)}")

            # create tabs
            tab1, tab2 = st.tabs(["📊 Trade Analysis", "🌉 Bridge Analysis"])
            
            with tab1:
                display_trade_data(
                    volume_data['volume_by_token'],
                    volume_data['accounts_mapping'],
                    volume_data['accounts_hitting_fills_limits']
                )
            
            with tab2:
                processed_bridge_data = format_bridge_data(raw_bridge_data, unit_token_mappings, prices)
                display_bridge_data(raw_bridge_data, processed_bridge_data)

def display_trade_data(volume_by_token, accounts_mapping, accounts_hitting_fills_limits):
    if len(accounts_hitting_fills_limits) > 0:
        st.warning(f'Unable to fetch all fills for accounts due to hitting API limits (contact me to check): {', '.join(accounts_hitting_fills_limits)}')

    df = create_volume_df(volume_by_token)
    display_trade_volume_table(df, len(accounts_mapping))

    # show raw data in expander
    if not df.empty:
        with st.expander("Raw Data"):
            st.json(accounts_mapping)
            st.json(volume_by_token)

def format_bridge_data(
    raw_bridge_data: dict,
    unit_token_mappings: dict[str, tuple[str, int]],
    prices: dict[str, dict[float, float]],
):
    # combine bridge operations from all addresses into a single DataFrame
    # TODO separate by address
    all_operations_df = pd.DataFrame()
    for _, data in raw_bridge_data.items():
        processed_df = process_bridge_operations(data, unit_token_mappings, prices, logger)
        if processed_df is not None and not processed_df.empty:
            all_operations_df = pd.concat([all_operations_df, processed_df], ignore_index=True)
    return all_operations_df

def display_bridge_data(raw_bridge_data: dict, all_operations_df: pd.DataFrame):
    if all_operations_df.empty:
        st.info("No bridge transactions found: if you think this is an error, contact me")
        return

    # create summary
    summary_df, top_asset = create_bridge_summary(all_operations_df)

    if summary_df is None or summary_df.empty:
        st.warning("No bridge transactions found: if you think this is an error, contact me")
        return

    # display metric
    col1, col2, col3 = st.columns(3)
    with col1:
        total_bridge_volume = summary_df['Total Volume'].sum()
        st.metric("Total Bridge Volume", format_currency(total_bridge_volume))
    with col2:
        st.metric("Top Bridged Asset", top_asset)
    with col3:
        total_transactions = summary_df['Total Transactions'].sum()
        st.metric("Total Bridge Transactions", int(total_transactions))

    col1, col2, col3 = st.columns(3)
    with col1:
        total_deposit_volume = summary_df['Deposit Volume'].sum()
        st.metric("Total Deposit Volume", format_currency(total_deposit_volume))
    with col2:
        total_withdraw_volume = summary_df['Withdraw Volume'].sum()
        st.metric("Total Withdraw Volume", format_currency(total_withdraw_volume))
    with col3:
        st.metric("Tokens Bridged", len(summary_df))

    st.markdown("---")

    # display bridge summary table
    st.markdown("### Bridge Activity Summary")

    # format display df
    display_df = summary_df.copy()
    display_df['Deposit Volume'] = display_df['Deposit Volume'].apply(lambda x: format_currency(x))
    display_df['Withdraw Volume'] = display_df['Withdraw Volume'].apply(lambda x: format_currency(x))
    display_df['Total Volume'] = display_df['Total Volume'].apply(lambda x: format_currency(x))
    
    # format dates
    display_df['First Transaction'] = display_df['First Transaction'].apply(
        lambda x: x.strftime(DATE_FORMAT) if pd.notna(x) else "N/A"
    )
    display_df['Last Transaction'] = display_df['Last Transaction'].apply(
        lambda x: x.strftime(DATE_FORMAT) if pd.notna(x) else "N/A"
    )

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            'Asset': st.column_config.TextColumn('Asset', width='small'),
            'Deposit Volume': st.column_config.TextColumn('Deposit Volume', width='small'),
            'Withdraw Volume': st.column_config.TextColumn('Withdraw Volume', width='small'),
            'Total Volume': st.column_config.TextColumn('Total Volume', width='small'),
            'Total Transactions': st.column_config.NumberColumn('Txns', width='small'),
            'First Transaction': st.column_config.TextColumn('First Transaction', width='medium'),
            'Last Transaction': st.column_config.TextColumn('Last Transaction', width='medium'),
        }
    )

    # bridge activity chart if we have multiple assets
    if len(summary_df) > 1:
        st.markdown("### Bridge Volume Distribution")
        
        chart_df = summary_df[['Asset', 'Deposit Volume', 'Withdraw Volume']].melt(
            id_vars=['Asset'],
            value_vars=['Deposit Volume', 'Withdraw Volume'],
            var_name='Volume Type',
            value_name='Volume'
        )
        
        fig = px.bar(
            chart_df,
            x='Asset',
            y='Volume',
            color='Volume Type',
            labels={'Volume': 'Volume (USD)'}
        )
        fig.update_layout(
            xaxis_title='Asset',
            yaxis_title='Volume',
            showlegend=True
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # raw data
    with st.expander("Raw Data"):
        st.dataframe(all_operations_df[['asset', 'direction', 'amount_formatted', 'opCreatedAt', 'state', 'sourceChain', 'destinationChain']])
        st.json(raw_bridge_data)


if __name__ == '__main__':
    main()