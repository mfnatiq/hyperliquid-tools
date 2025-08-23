from utils.utils import DATE_FORMAT, format_currency, get_cached_unit_token_mappings, get_current_timestamp_millis
from datetime import datetime, timedelta, timezone
import pandas as pd
from hyperliquid.info import Info
from hyperliquid.utils import constants
import streamlit as st
import streamlit.components.v1 as components
import plotly.express as px
from bridge.unit_bridge_api import UnitBridgeInfo
from utils.render_utils import footer_html, copy_script
from trade.trade_data import get_candlestick_data

from bridge.unit_bridge_utils import create_bridge_summary, process_bridge_operations
from consts import unitStartTime, oneDayInS

# setup and configure logging
import logging
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


st.set_page_config(
    'Hyperliquid Tools',
    "🔧",
    layout="wide",
)

# plausible analytics
components.html("""
<script defer data-domain="app.hyperliquid-tools.xyz"
    src="https://plausible-analytics-ce-production-441d.up.railway.app/js/script.js">
</script>
""", height=0)

st.title("Unit Volume Tracker")

@st.dialog("Welcome to hyperliquid-tools!", width="large", on_dismiss="ignore")
def announcement():
    st.write("""
        This site lets you view your HyperUnit trading / bridging volume, along with some other metrics.

        If you like what you see, please follow me on Twitter or perhaps make a little donation (address at bottom of page) :)

        If you find any bugs or have any feature requests, feel free to DM me on Twitter as well.

        Enjoy!
    """)
# opening modal only upon startup
if 'startup' not in st.session_state:
    announcement()
    st.session_state['startup'] = True

info = Info(constants.MAINNET_API_URL, skip_ws=True)
unit_bridge_info = UnitBridgeInfo()


@st.cache_data(ttl=3600, show_spinner=False)
def _get_cached_unit_token_mappings() -> dict[str, tuple[str, int]]:
    """
    with caching and show_spinner=false
    even if this is wrapped around a spinner,
    that spinner only runs whenever data is NOT fetched from cache
    i.e. if data is actually fetched
    hence reducing unnecessary quick spinner upon fetching from cache
    """
    return get_cached_unit_token_mappings(info, logger)


@st.cache_data(ttl=oneDayInS, show_spinner=False)
def _get_candlestick_data(_token_ids: list[str], _token_names: list[str]):
    """
    cache daily just to get OHLCV prices
    """
    return get_candlestick_data(info, _token_ids, _token_names)

@st.cache_data(ttl=60, show_spinner=False)
def get_curr_hype_price():
    prices = info.all_mids()
    return float(prices['@107'])


def load_data():
    unit_token_mappings = _get_cached_unit_token_mappings()
    token_list = [t for t, _ in unit_token_mappings.values()]
    cumulative_trade_data = _get_candlestick_data(
        [k for k in unit_token_mappings.keys()], token_list)

    return unit_token_mappings, token_list, cumulative_trade_data


# -------- initialisation and caching --------
if "init_done" not in st.session_state:
    # first-ever run: initialisation
    with st.spinner("Initialising..."):
        unit_token_mappings, token_list, cumulative_trade_data = load_data()

        # save into session_state so don't re-init
        st.session_state.unit_token_mappings = unit_token_mappings
        st.session_state.token_list = token_list
        st.session_state.cumulative_trade_data = cumulative_trade_data
        st.session_state.init_done = True
else:
    # subsequent runs: refresh cached data
    unit_token_mappings, token_list, cumulative_trade_data = load_data()

    # update session_state with latest values
    st.session_state.unit_token_mappings = unit_token_mappings
    st.session_state.token_list = token_list
    st.session_state.cumulative_trade_data = cumulative_trade_data
# use cached/session values
unit_token_mappings = st.session_state.unit_token_mappings
token_list = st.session_state.token_list
cumulative_trade_data = st.session_state.cumulative_trade_data


st.metric("Current HYPE Price", format_currency(get_curr_hype_price()))


col1, col2, col3 = st.columns([10, 2, 1])
with col1:
    addresses_input: str = st.text_input(
        "Enter 1 or more hyperliquid accounts (comma-separated)",
        placeholder="Enter 1 or more hyperliquid accounts (comma-separated)",
        label_visibility='collapsed',
        key='hyperliquid_address_input',
    )
with col2:
    exclude_subaccounts = st.checkbox("Exclude Subaccounts")
with col3:
    submitted = st.button("Run", type="primary")


# region optimisations
@st.cache_data(ttl=3600, show_spinner=False)
def get_subaccounts_cached(account: str) -> list:
    subaccounts = info.query_sub_accounts(account)
    return subaccounts if subaccounts is not None else []
# endregion


@st.cache_data(ttl=60, show_spinner=False)
def get_cached_unit_volumes(
    accounts: list[str],
    unit_token_mappings: dict[str, tuple[str, int]],
    exclude_subaccounts: bool = False,
):
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

            # initialisation
            startTime = unitStartTime

            # seems like 2k limit for endpoint counts from the start
            # so start from overall start time then move up til currtime
            while startTime < currTime:  # check back until this date
                endTime = startTime + \
                    int(timedelta(days=numDaysQuerySpan).total_seconds()) * 1000
                logging.info(
                    f'querying for {account} startTime: {startTime}; endTime: {endTime}')

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
                    latest_fill_timestamp = max(
                        f['time'] for f in fills_result)
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


# main app logic reruns upon any interaction
def main():
    # placeholder that can be cleared and rewritten
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
            df_trade = create_volume_df(volume_data['volume_by_token'])

            raw_bridge_data = st.session_state['raw_bridge_data']
            processed_bridge_data = format_bridge_data(
                raw_bridge_data, unit_token_mappings, cumulative_trade_data)
            df_bridging, top_bridged_asset = create_bridge_summary(
                processed_bridge_data)

            st.caption(
                f"Last updated: {volume_data['last_updated'].strftime(DATE_FORMAT)}")

            # create tabs
            tab1, tab2, tab3 = st.tabs(
                ["📋 Summary", "📊 Trade Analysis", "🌉 Bridge Analysis"])

            with tab1:
                display_summary(df_trade, df_bridging, top_bridged_asset)

            with tab2:
                display_trade_data(
                    df_trade,
                    volume_data['accounts_mapping'],
                    volume_data['accounts_hitting_fills_limits'],
                )

            with tab3:
                display_bridge_data(
                    raw_bridge_data, df_bridging, top_bridged_asset, processed_bridge_data)

# --------------- display ---------------
def display_summary(df_trade: pd.DataFrame, df_bridging: pd.DataFrame | None, top_bridged_asset: str):
    # trade data
    if df_trade.empty:
        st.warning(
            "No trades on Unit tokens found - if you think this is an error, contact me")
    else:
        st.subheader("Trade Data")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Trade Volume", format_currency(
                df_trade['Total Volume'].sum()))
        with col2:
            st.metric("Top Traded Token", df_trade.iloc[0]['Token'])
        with col3:
            st.metric('Total Trades Made', df_trade['Num Trades'].sum())

    # bridging data
    if df_bridging is None or df_bridging.empty:
        st.warning(
            "No bridge transactions found - if you think this is an error, contact me")
    else:
        st.subheader("Bridge Data")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Bridge Volume (USD)", format_currency(
                df_bridging['Total (USD)'].sum()))
        with col2:
            st.metric("Top Bridged Token", top_bridged_asset)
        with col3:
            st.metric("Total Bridge Transactions", int(
                df_bridging['Total Transactions'].sum()))


def display_trade_volume_table(df: pd.DataFrame, num_accounts: int):
    have_trade_volume = not df.empty

    if not have_trade_volume:
        st.warning(
            "No trades on Unit tokens found - if you think this is an error, contact me")
        return

    # display metrics at top
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Trade Volume", format_currency(
            df['Total Volume'].sum() if have_trade_volume else 0.0))
    with col2:
        most_traded = df.iloc[0]['Token'] if have_trade_volume else "N/A"
        st.metric("Top Traded Token", most_traded)
    with col3:
        st.metric("Tokens Traded", len(df))

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Accounts Traded On", num_accounts)
    with col2:
        total_fees = 0.0
        if have_trade_volume:
            df['Total Fees'] = df['Token Fees'] + df['USDC Fees']
            total_fees = df['Total Fees'].sum()
        st.metric('Total Fees Paid', format_currency(total_fees))
    with col3:
        st.metric('Total Trades Made', df['Num Trades'].sum() if have_trade_volume else 0)

    if not have_trade_volume:
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
        st.markdown("## Volume Distribution")

        col1, col2 = st.columns(2, gap="large")

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


def display_trade_volume_info(
    trade_df: pd.DataFrame,
    cumulative_trade_data: pd.DataFrame,
    accounts: list[str]
):
    final_cumulative_volume = cumulative_trade_data.groupby('token_name').agg(
        final_cumulative_volume=('cumulative_volume_usd', 'last')
    ).reset_index()

    rows = []
    total_user_volume = 0
    total_exchange_volume = 0

    for token in token_list:
        user_volume = 0
        try:
            user_volume = trade_df[trade_df['Token'] == token].iloc[0]['Total Volume']
        except:
            logger.info(
                f'no volume found for {", ".join(accounts)} for {token}; skipping')

        exchange_volume = 0
        try:
            exchange_volume = final_cumulative_volume[final_cumulative_volume['token_name']
                                                        == token].iloc[0]['final_cumulative_volume']
        except:
            # no cumulative volume found for this unit token, ignoring
            continue

        total_user_volume += user_volume
        total_exchange_volume += exchange_volume

        rows.append({
            'Asset': token,
            'Your Volume (USD)': user_volume,
            'Market Volume (USD)': exchange_volume,
            'Your Share (%)': (user_volume / exchange_volume / 2.0 * 100.0),
        })

    # some error here
    if total_exchange_volume == 0:
        logger.warning(
            "unable to get any cumulative trading data; ignoring cumulative volume metrics")
        return

    st.markdown("## Volume Share Info")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Your Volume (Total)", format_currency(total_user_volume))
    with col2:
        st.metric("Total Exchange Unit Trading Volume", format_currency(total_exchange_volume))
    with col3:
        st.metric("Share of Total Exchange Unit Trading Volume",
                f"{(total_user_volume / total_exchange_volume / 2.0 * 100):.10f}%",
                help="""
                Percentage is your volume / half exchange volume, as exchange volume is 1x for both maker + taker

                E.g. if you take a 1k maker order, both you and the maker get 1k volume, but exchange volume is only 1k

                So each of you gets 50\\% of that volume
                """)

    df_cumulative = pd.DataFrame(rows)
    token_order = (
        df_cumulative.groupby('Asset')['Market Volume (USD)']
        .max().sort_values(ascending=False).index.tolist()
    )
    df_cumulative = df_cumulative.sort_values(
        'Market Volume (USD)', ascending=False)

    # format for display only
    df_cumulative['Your Volume (USD)'] = df_cumulative['Your Volume (USD)'].apply(
        format_currency)
    df_cumulative['Market Volume (USD)'] = df_cumulative['Market Volume (USD)'].apply(
        format_currency)
    df_cumulative['Your Share (%)'] = df_cumulative['Your Share (%)'].map(
        lambda x: f"{x:.6f}%")

    col1, col2 = st.columns(2, vertical_alignment='top', gap="large")
    with col1:
        st.subheader("Trading Volume Breakdown", help="Same half proportion as above")
        st.dataframe(df_cumulative, hide_index=True)
    with col2:
        # plot cumulative volume over time
        st.subheader("Cumulative Volume Over Time by Token")
        fig = px.line(
            cumulative_trade_data,
            x='start_date',
            y='cumulative_volume_usd',
            color='token_name',
            labels={
                'cumulative_volume_usd': 'Cumulative Volume (USD)',
                'start_date': 'Date',
                'token_name': 'Token'
            },
            category_orders={'token_name': token_order}
        )
        # display legend in descending order of total volume
        st.plotly_chart(fig, use_container_width=True)


def display_trade_data(df_trade, accounts_mapping: dict, accounts_hitting_fills_limits):
    if len(accounts_hitting_fills_limits) > 0:
        st.warning(
            f'Unable to fetch all fills for accounts due to hitting API limits (contact me to check): {', '.join(accounts_hitting_fills_limits)}')

    display_trade_volume_table(df_trade, len(accounts_mapping))

    display_trade_volume_info(df_trade, cumulative_trade_data, list(accounts_mapping.keys()))

    # show raw data in expander
    if not df_trade.empty:
        with st.expander("Raw Data"):
            st.json(accounts_mapping)
            st.json(df_trade)


def format_bridge_data(
    raw_bridge_data: dict,
    unit_token_mappings: dict[str, tuple[str, int]],
    cumulative_trade_data: pd.DataFrame,
):
    # combine bridge operations from all addresses into a single DataFrame
    # TODO separate by address?
    all_operations_df = pd.DataFrame()
    for _, data in raw_bridge_data.items():
        processed_df = process_bridge_operations(
            data, unit_token_mappings, cumulative_trade_data, logger)
        if processed_df is not None and not processed_df.empty:
            all_operations_df = pd.concat(
                [all_operations_df, processed_df], ignore_index=True)
    return all_operations_df


def display_bridge_data(raw_bridge_data: dict, summary_df: pd.DataFrame | None, top_asset: str, all_operations_df: pd.DataFrame):
    if all_operations_df.empty or summary_df is None or summary_df.empty:
        st.warning(
            "No bridge transactions found - if you think this is an error, contact me")
        return

    # display metric
    col1, col2, col3 = st.columns(3)
    with col1:
        total_bridge_volume = summary_df['Total (USD)'].sum()
        st.metric("Total Bridge Volume (USD)",
                  format_currency(total_bridge_volume))
    with col2:
        st.metric("Top Bridged Token", top_asset)
    with col3:
        total_transactions = summary_df['Total Transactions'].sum()
        st.metric("Total Bridge Transactions", int(total_transactions))

    col1, col2, col3 = st.columns(3)
    with col1:
        total_deposit_volume = summary_df['Deposit (USD)'].sum()
        st.metric("Total Deposit Volume (USD)",
                  format_currency(total_deposit_volume))
    with col2:
        total_withdraw_volume = summary_df['Withdraw (USD)'].sum()
        st.metric("Total Withdraw Volume (USD)",
                  format_currency(total_withdraw_volume))
    with col3:
        st.metric("Tokens Bridged", len(summary_df))

    st.markdown("---")

    # display bridge summary table
    st.markdown("## Bridge Activity Summary")

    # format display df
    display_df = summary_df.copy()
    display_df['Deposit'] = display_df['Deposit'].apply(lambda x: f"{x:.4f}")
    display_df['Withdraw'] = display_df['Withdraw'].apply(lambda x: f"{x:.4f}")
    display_df['Total'] = display_df['Total'].apply(lambda x: f"{x:.4f}")
    display_df['Deposit (USD)'] = display_df['Deposit (USD)'].apply(
        lambda x: format_currency(x))
    display_df['Withdraw (USD)'] = display_df['Withdraw (USD)'].apply(
        lambda x: format_currency(x))
    display_df['Total (USD)'] = display_df['Total (USD)'].apply(
        lambda x: format_currency(x))

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
            'Deposit': st.column_config.TextColumn('Deposit', width='small'),
            'Withdraw': st.column_config.TextColumn('Withdraw', width='small'),
            'Total': st.column_config.TextColumn('Total', width='small'),
            'Deposit (USD)': st.column_config.TextColumn('Deposit (USD)', width='small'),
            'Withdraw (USD)': st.column_config.TextColumn('Withdraw (USD)', width='small'),
            'Total (USD)': st.column_config.TextColumn('Total (USD)', width='small'),
            'Total Transactions': st.column_config.NumberColumn('Txns', width='small'),
            'First Transaction': st.column_config.TextColumn('First Transaction', width='medium'),
            'Last Transaction': st.column_config.TextColumn('Last Transaction', width='medium'),
        }
    )

    # bridge activity chart if we have multiple assets
    if len(summary_df) > 1:
        st.markdown("## Bridge Volume Distribution")

        chart_df = summary_df[['Asset', 'Deposit (USD)', 'Withdraw (USD)']].melt(
            id_vars=['Asset'],
            value_vars=['Deposit (USD)', 'Withdraw (USD)'],
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
        st.dataframe(all_operations_df[['asset', 'direction', 'amount_formatted',
                     'amount_usd', 'opCreatedAt', 'state', 'sourceChain', 'destinationChain']])
        st.json(raw_bridge_data)


if __name__ == '__main__':
    main()

    # region sticky footer
    # put up here so container emptying doesn't make footer flash
    # render footer
    st.html(footer_html)
    # render copy script in a separate component to avoid CSP issues
    components.html(copy_script, height=0)
    # endregion