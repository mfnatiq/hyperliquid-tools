from datetime import datetime, timezone
import pandas as pd
from hyperliquid.utils.types import SpotAssetInfo
from hyperliquid.info import Info
from hyperliquid.utils import constants
import streamlit as st
import streamlit.components.v1 as components
import plotly.express as px

from utils import format_currency, get_current_timestamp_millis

st.set_page_config(
    'Hyperliquid Tools',
    "🔧",
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
    console.log("Copying text: " + copyText);
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
    console.log("Copy button found:", copyBtn);
    if (copyBtn && !copyBtn.hasAttribute('data-listener-attached')) {
        copyBtn.addEventListener('click', copy_to_clipboard);
        copyBtn.setAttribute('data-listener-attached', 'true');
        console.log("Event listener attached successfully");
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
            print(
                f'unable to find pair metadata for token {token_name}, skipping processing')

    return mapping

with st.spinner('Initializing token mappings...'):
    unit_token_mappings = get_cached_unit_token_mappings()


@st.cache_data(ttl=60, show_spinner=False)
def get_cached_unit_volumes(accounts: list[str], unit_token_mappings: dict[str, str], exclude_subaccounts: bool = False):
    """
    get unit volumes with caching
    """
    info = Info(constants.MAINNET_API_URL, skip_ws=True)

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
            subaccounts = info.query_sub_accounts(account)
            if subaccounts is not None:
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
            'Buy': {
                'Last Updated': None,
                'Volume': 0.0,
            },
            'Sell': {
                'Last Updated': None,
                'Volume': 0.0,
            },
            'Token Fees': 0.0,
            'USDC Fees': 0.0,
            'Num Trades': 0,
        }
        for t in unit_token_mappings.values()
    }

    fills = dict()  # { account: list of fills }
    try:
        for account in accounts_to_query:
            query_again = True
            endTimeMillis = get_current_timestamp_millis()
            num_fills_total = 0
            account_fills = []
            while query_again:
                fills_result = info.post("/info", {
                    "type": "userFillsByTime",  # up to 10k total, then need to query from s3
                    "user": account,
                    "aggregateByTime": True,
                    "startTime": endTimeMillis - 365 * 24 * 60 * 60 * 1000,  # set 1 year by default
                    "endTime": endTimeMillis - 1,     # exclude current millisecond in case of repeated fills
                })

                # if reached 2k fills, query again til no more
                num_fills = len(fills_result)
                num_fills_total += num_fills
                if num_fills == 2000:
                    earliest_timestamp = min(f['time'] for f in fills_result)
                    endTimeMillis = earliest_timestamp
                else:
                    query_again = False
                account_fills.extend(fills_result)
            fills[account] = account_fills
    except Exception:
        return dict(), dict(), [], 'Unable to fetch trade history - did you put a valid list of accounts? If you copied your Liminal institutional subaccount account, remember to remove the "HL:" prefix'

    # 10k - need get from s3
    accounts_hitting_fills_limits = []

    for account, fills_list in fills.items():
        if len(fills_list) == 10000:
            accounts_hitting_fills_limits.append(account)
        
        for f in fills_list:
            coin = f['coin']
            direction = f['dir']
            if coin in unit_token_mappings.keys():
                token_name = unit_token_mappings[coin]

                price = float(f['px'])

                trade_volume = float(f['sz']) * price
                trade_time = datetime.fromtimestamp(
                    f['time'] / 1000, tz=timezone.utc)
                prev_last_updated = volume_by_token[token_name][direction]['Last Updated']
                if prev_last_updated is None or trade_time > prev_last_updated:
                    volume_by_token[token_name][direction]['Last Updated'] = trade_time
                volume_by_token[token_name][direction]['Volume'] += trade_volume

                # only count unit fills
                volume_by_token[token_name]['Num Trades'] += 1
                accounts_mapping[account]['Num Trades'] += 1

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
                'Token Fees': volumes['Token Fees'],
                'USDC Fees': volumes['USDC Fees'],
                'Num Trades': volumes['Num Trades'],
                'Last Transaction': get_latest_txn_datetime(buys['Last Updated'], sells['Last Updated']),
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
        st.metric('Total Trades Made', df['Num Trades'].sum() if have_volume else 0)

    if not have_volume:
        return

    st.markdown("---")

    # format df for display
    display_df = df[['Token', 'Buy Volume', 'Sell Volume',
                    'Total Volume', 'Total Fees', 'Last Transaction']].copy()
    for col in ['Buy Volume', 'Sell Volume', 'Total Volume', 'Total Fees']:
        display_df[col] = display_df[col].apply(lambda x: f"${x:,.2f}")
    display_df['Last Transaction'] = display_df['Last Transaction'].apply(
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

def display_accounts_table(accounts_df: pd.DataFrame):
    st.dataframe(
        accounts_df,
        use_container_width=True,
        hide_index=True,
    )

# main app logic reruns upon any interaction
def main():
    st.info(f'Unit tokens: {", ".join(unit_token_mappings.values())}')

    col1, col2, col3 = st.columns([6, 3, 2])
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
        submitted = st.button("Fetch Data", type="primary")

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
                    st.warning(f'Unable to fetch all fills for accounts due to hitting API limits (contact me to check): {', '.join(accounts_hitting_fills_limits)}')
    
                df = create_volume_df(volume_by_token)
                display_volume_table(df, len(accounts_mapping))

                # show raw data in expander
                if not df.empty:
                    with st.expander("Raw Data"):
                        st.json(accounts_mapping)
                        st.json(volume_by_token)

if __name__ == '__main__':
    main()