from utils import format_currency, get_current_timestamp_millis
from datetime import datetime, timedelta, timezone
import pandas as pd
from hyperliquid.utils.types import SpotAssetInfo
from hyperliquid.info import Info
from hyperliquid.utils import constants
import streamlit as st
import streamlit.components.v1 as components
import plotly.express as px
from consts import unitStartTime

# setup and configure logging
import logging
from trade.trade_data import get_cumulative_trade_data
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

st.title("Unit Volume Tracker")
st.markdown(
    "Input 1 or more accounts (comma-separated) to see combined volume across Hyperliquid Unit tokens")

# region sticky footer
# put up here so container emptying doesn't make footer flash
footer_html = """
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
<link rel="stylesheet" href="https://fonts.googleapis.com/css?family=Source+Sans+Pro:400,600&display=swap">
<style>
html, body { margin: 0; padding: 0; background: transparent; }
.footer {
    position: fixed;
    left: 0;
    bottom: 0;
    width: 100%;
    color: #D3D3D3;
    display: flex;
    justify-content: center;
    align-items: center;
    padding: 10px 20px;
    font-size: 14px;
    font-family: 'Source Sans Pro', sans-serif;
    background-color: #0e1117;  /* same as main streamlit background */
    z-index: 9999;
}
.footer a { color: #87CEEB; text-decoration: none; }
.separator { margin: 0 15px; }
.donation-address {
    background-color: #2C2C2C;
    padding: 4px 8px;
    border-radius: 5px;
    font-family: monospace;
    margin: 8px;
}
.icon-container {
    display: inline-block;
    width: 1.5em;
    text-align: center;
    cursor: pointer;
}
.copy-icon { color: #A9A9A9; transition: color 0.2s; }
.copy-icon:hover { color: #87CEEB; }
</style>

<div class="footer">
    <span>made by <a href="https://x.com/mfnatiq1" target="_blank">@mfnatiq1</a></span>
    <span class="separator">•</span>
    <span>donations:</span>
    <span id="donation-address" class="donation-address">0xB17648Ed98C9766B880b5A24eEcAebA19866d1d7</span> 
    <span class="icon-container" id="copy-btn" title="Copy to clipboard">
        <i id="icon-copy" class="fa-solid fa-copy copy-icon"></i>
        <i id="icon-check" class="fa-solid fa-check copy-icon" style="display:none; color:#7CFC00;"></i>
    </span>
</div>
"""

# JavaScript component for copy functionality
copy_script = """
<script>
function copy_to_clipboard() {
    var copyText = "0xB17648Ed98C9766B880b5A24eEcAebA19866d1d7";
    var iconCopy = parent.document.getElementById("icon-copy");
    var iconCheck = parent.document.getElementById("icon-check");

    function showTick() {
        if (iconCopy && iconCheck) {
            iconCopy.style.display = 'none';
            iconCheck.style.display = 'inline-block';
            setTimeout(function() {
                iconCheck.style.display = 'none';
                iconCopy.style.display = 'inline-block';
            }, 1500);
        }
    }

    if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(copyText).then(showTick).catch(function() {
            fallbackCopy();
        });
    } else {
        fallbackCopy();
    }

    function fallbackCopy() {
        var ta = document.createElement('textarea');
        ta.value = copyText;
        ta.style.position = 'fixed';
        ta.style.left = '-9999px';
        document.body.appendChild(ta);
        ta.select();
        try {
            document.execCommand('copy');
            showTick();
        } catch (e) {
            alert('Copy failed');
        }
        document.body.removeChild(ta);
    }
}

// Function to attach event listener
function attachCopyEvent() {
    var copyBtn = parent.document.getElementById('copy-btn');
    if (copyBtn && !copyBtn.hasAttribute('data-listener-attached')) {
        copyBtn.addEventListener('click', copy_to_clipboard);
        copyBtn.setAttribute('data-listener-attached', 'true');
        return true;
    }
    return false;
}

// Try to attach immediately
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', attachCopyEvent);
} else {
    attachCopyEvent();
}

// Fallback for Streamlit's dynamic content loading
setTimeout(function() {
    if (!attachCopyEvent()) {
        var observer = new MutationObserver(function(mutations) {
            mutations.forEach(function(mutation) {
                if (mutation.type === 'childList') {
                    if (attachCopyEvent()) {
                        observer.disconnect();
                    }
                }
            });
        });
        if (parent.document.body) {
            observer.observe(parent.document.body, { childList: true, subtree: true });
            setTimeout(function() {
                observer.disconnect();
            }, 5000);
        }
    }
}, 100);
</script>
"""

# render footer
st.markdown(footer_html, unsafe_allow_html=True)

# render copy script in a separate component to avoid CSP issues
components.html(copy_script, height=0)
# endregion

info = Info(constants.MAINNET_API_URL, skip_ws=True)

# with caching and show_spinner=false
# even if this is wrapped around a spinner,
# that spinner only runs whenever data is NOT fetched from cache
# i.e. if data is actually fetched
# hence reducing unnecessary quick spinner upon fetching from cache


@st.cache_data(ttl=3600, show_spinner=False)
def get_cached_unit_token_mappings() -> dict[str, str]:
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

        universe_entry = None
        while universe_metadata_idx < len(universe_metadata):
            if token_idx in universe_metadata[universe_metadata_idx]['tokens']:
                universe_entry = universe_metadata[universe_metadata_idx]
                break
            universe_metadata_idx += 1

        if universe_entry:
            mapping[universe_entry['name']] = token_name
        else:
            logging.info(
                f'unable to find pair metadata for token {token_name}, skipping processing')

    return mapping


with st.spinner('Initializing token mappings...'):
    unit_token_mappings = get_cached_unit_token_mappings()
    cumulative_trade_data = get_cumulative_trade_data(info, unit_token_mappings)


# region optimisations
@st.cache_data(ttl=3600, show_spinner=False)
def get_subaccounts_cached(account: str) -> list:
    subaccounts = info.query_sub_accounts(account)
    return subaccounts if subaccounts is not None else []
# endregion


@st.cache_data(ttl=60, show_spinner=False)
def get_cached_unit_volumes(accounts: list[str], unit_token_mappings: dict[str, str], exclude_subaccounts: bool = False):
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
        for t in unit_token_mappings.values()
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
                token_name = unit_token_mappings[coin]

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


def display_volume_table(df: pd.DataFrame, num_accounts: int):
    have_volume = not df.empty

    if not have_volume:
        st.warning(
            "No trades on Unit tokens found - if you think this is an error, contact me")

    # display metrics at top
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Volume", format_currency(
            df['Total Volume'].sum() if have_volume else 0.0))
    with col2:
        st.metric("Tokens Traded", len(df))
    with col3:
        most_traded = df.iloc[0]['Token'] if have_volume else "N/A"
        st.metric("Most Traded Token", most_traded)

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
        st.metric('Total Trades Made',
                  df['Num Trades'].sum() if have_volume else 0)

    if not have_volume:
        return

    st.markdown("---")

    # format df for display
    display_df = df[['Token', 'Buy Volume', 'Sell Volume',
                    'Total Volume', 'Total Fees', 'First Trade', 'Last Trade', 'Maker Volume', 'Taker Volume']].copy()
    for col in ['Buy Volume', 'Sell Volume', 'Total Volume', 'Total Fees', 'Maker Volume', 'Taker Volume']:
        display_df[col] = display_df[col].apply(lambda x: f"${x:,.2f}")
    display_df['First Trade'] = display_df['First Trade'].apply(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S UTC'))
    display_df['Last Trade'] = display_df['Last Trade'].apply(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S UTC'))

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

def display_trade_volume_info(
    trade_df: pd.DataFrame,
    cumulative_trade_data: pd.DataFrame,
    unit_token_mappings: dict[str, str],
    accounts: list[str]
):
    # TODO change to use token_list when merging other PR
    token_list = list(unit_token_mappings.values())

    final_cumulative_volume = cumulative_trade_data.groupby('token_name').agg(
        final_cumulative_volume=('cumulative_volume_usd', 'last')
    ).reset_index()

    rows = []
    total_user_volume = 0
    total_cumulative_volume = 0

    for token in token_list:
        user_volume = 0
        try:
            user_volume = trade_df[trade_df['Token'] == token].iloc[0]['Total Volume']
        except:
            logger.info(f'no volume found for {", ".join(accounts)} for {token}; skipping')

        cumulative_volume = 0
        try:
            cumulative_volume = final_cumulative_volume[final_cumulative_volume['token_name'] == token].iloc[0]['final_cumulative_volume']
        except:
            # no cumulative volume found for this unit token, ignoring
            continue

        total_user_volume += user_volume
        total_cumulative_volume += cumulative_volume

        rows.append({
            'Asset': token,
            'User Volume': format_currency(user_volume),
            'Total Volume': cumulative_volume,
            'User Percentage': f"{(user_volume / cumulative_volume * 100):.10f}%",
        })

    # some error here
    if total_cumulative_volume == 0:
        logger.warning("unable to get any cumulative trading data; ignoring cumulative volume metrics")
        return

    st.markdown("### Cumulative Volume Share")

    st.metric("Share of Total Unit Trading Volume", f"{(total_user_volume / total_cumulative_volume * 100):.10f}%")

    df_cumulative = pd.DataFrame(rows)
    token_order = df_cumulative.groupby('Asset')['Total Volume'].max().sort_values(ascending=False).index.tolist()
    df_cumulative = df_cumulative.sort_values('Total Volume', ascending=False)

    # get sorting order then format for display
    df_cumulative['Total Volume'] = df_cumulative['Total Volume'].apply(format_currency)

    col1, col2 = st.columns(2, vertical_alignment='center', gap="large")
    with col1:
        st.dataframe(df_cumulative)
    with col2:
        # plot cumulative volume over time
        fig = px.line(
            cumulative_trade_data,
            x='start_date',
            y='cumulative_volume_usd',
            color='token_name',
            title='Cumulative Volume Over Time by Token',
            labels={
                'cumulative_volume_usd': 'Cumulative Volume (USD)',
                'start_date': 'Date',
                'token_name': 'Token'
            },
            category_orders={ 'token_name': token_order }
        )
        st.plotly_chart(fig, use_container_width=True)  # display legend in descending order of total volume

# main app logic reruns upon any interaction
def main():
    st.info(f'Unit tokens: {", ".join(unit_token_mappings.values())}')

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

        with st.spinner(f'Loading trade history for {", ".join(accounts)}...'):
            volume_by_token, accounts_mapping, accounts_hitting_fills_limits, err = get_cached_unit_volumes(
                accounts, unit_token_mappings, exclude_subaccounts)

        # create container within placeholder for new content
        with output_placeholder.container():
            if err is not None:
                st.error(err)
            else:
                last_updated = datetime.now(timezone.utc)
                st.caption(
                    f"Last updated: {last_updated.strftime('%Y-%m-%d %H:%M:%S UTC')}")

                if len(accounts_hitting_fills_limits) > 0:
                    st.warning(
                        f'Unable to fetch all fills for accounts due to hitting API limits (contact me to check): {', '.join(accounts_hitting_fills_limits)}')

                df = create_volume_df(volume_by_token)
                display_volume_table(df, len(accounts_mapping))

                # display trade volume info
                display_trade_volume_info(df, cumulative_trade_data, unit_token_mappings, accounts)

                # show raw data in expander
                if not df.empty:
                    with st.expander("Raw Data"):
                        st.json(accounts_mapping)
                        st.json(volume_by_token)


if __name__ == '__main__':
    main()
