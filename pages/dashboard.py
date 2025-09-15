from copy import deepcopy
import os
import time
from dotenv import load_dotenv
import requests
from src.auth.db_utils import init_db, PremiumType, get_user_premium_type, upgrade_to_premium, start_trial_if_new_user, get_user
from src.leaderboard.leaderboard_utils import get_leaderboard_last_updated, get_leaderboard
from src.utils.utils import DATE_FORMAT, format_currency, get_cached_unit_token_mappings, get_current_timestamp_millis
from datetime import datetime, timedelta, timezone
import pandas as pd
from hyperliquid.info import Info
from hyperliquid.utils import constants
from hyperliquid.utils.error import ClientError, ServerError
import streamlit as st
import streamlit.components.v1 as components
import plotly.express as px
from src.bridge.unit_bridge_api import UnitBridgeInfo
from src.utils.render_utils import footer_html, copy_script
from src.trade.trade_data import get_candlestick_data, get_user_fills
from src.bridge.unit_bridge_utils import create_bridge_summary, process_bridge_operations
from src.consts import unitStartTime, oneDayInS, acceptedPayments
import uuid

# setup and configure logging
import logging
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# umami analytics
umami_website_id = "d055b0ff-48a4-4617-a9fd-4124a5346705"
components.html(f"""
<script defer src="https://cloud.umami.is/script.js" data-website-id="{umami_website_id}">
</script>
""", height=0)

# # set up secrets manually as secrets.toml seems only readable in streamlit community cloud
# TODO doesn't work, st.secrets seems to be read-only
# try:
#     # check if the 'auth' key already exists in st.secrets
#     auth_config_exists = 'auth' in st.secrets
# except Exception as e:  # streamlit.errors.StreamlitSecretNotFoundError: No secrets found
#     # error is thrown if no secrets file is found at all
#     auth_config_exists = False
# if not auth_config_exists:
#     logger.info("auth config not found in st.secrets. Populating from environment variables...")
#     # create the 'auth' dictionary within st.secrets.
#     st.secrets['auth'] = {
#         'client_id': os.getenv('AUTH_CLIENT_ID'),
#         'client_secret': os.getenv('AUTH_CLIENT_SECRET'),
#         'redirect_uri': os.getenv('AUTH_REDIRECT_URI'),
#         'cookie_secret': os.getenv('AUTH_COOKIE_SECRET'),
#         'server_metadata_url': os.getenv('AUTH_SERVER_METADATA_URL')
#     }

st.set_page_config(
    'Hyperliquid Tools',
    "🔧",
    layout="wide",
)

# ensure db is ready, will only run once per user session
if "db_initialized" not in st.session_state:
    init_db(logger)
    st.session_state["db_initialized"] = True


# robust logic to handle user login/logout and initiate trials.
# runs only once when user's state changes
if 'user_email' not in st.session_state and st.user and 'email' in st.user:
    # runs only when the user has just logged in
    st.session_state["user_email"] = st.user.email
    # start a trial for the user if they are new to the system
    err = start_trial_if_new_user(st.session_state["user_email"], logger)
    if err:
        st.error(err)
elif 'user_email' in st.session_state and not (st.user and 'email' in st.user):
    # runs when the user has just logged out.
    del st.session_state["user_email"]
    if "user_object" in st.session_state:
        del st.session_state["user_object"]


def is_logged_in():
    if len(st.user) == 0:
        return False
    return st.user.is_logged_in

load_dotenv()

# track umami analytics
UMAMI_API = "https://cloud.umami.is/api/send"
def track_event(event_name, additional_data: dict = {}):
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Hyperliquid-Tools/1.0",
    }
    payload = {
        "type": "event",
        "payload": {
            "name": event_name,
            "url": "/",
            "hostname": os.getenv("HOSTNAME"),
            "language": "",
            "referrer": "",
            "screen": "",
            "title": "Hyperliquid Tools",
        }
    }
    if additional_data and isinstance(additional_data, dict):
        payload["payload"]["data"] = additional_data

    try:
        requests.post(UMAMI_API, json=payload, headers=headers, timeout=5)
        logger.info(f'successfully tracked event {event_name} to umami')
    except requests.RequestException as e:
        logger.warning(f"umami analytics tracking failed for {event_name}: {e}")

def handle_login_click():
    track_event('login')
    st.login()

def show_login_info(show_button_only: bool = False):
    if not show_button_only:
        st.markdown("Log in to view more detailed info", width="content")
    st.button(
        "Login",
        key=uuid.uuid4(),
        on_click=handle_login_click,
        icon=":material/login:",
        type="primary"
    )


col1, col2 = st.columns([1, 1], vertical_alignment='center')
with col1:
    st.title("Unit Volume Tracker")
with col2:
    with st.container(vertical_alignment='center', horizontal=True, horizontal_alignment="right"):
        if "user_email" in st.session_state:
            # display dynamic user status (premium, trial, expired)
            user = get_user(st.session_state['user_email'], logger)
            status_message = ""
            if user:
                user_premium_type = get_user_premium_type(st.session_state['user_email'], logger)

                if user_premium_type == PremiumType.FULL:
                    status_message = "<span style='color: #28a745;'>(Premium)</span>" # green
                elif user_premium_type == PremiumType.TRIAL:
                    expires_str = user.trial_expires_at.strftime('%Y-%m-%d')
                    status_message = f"<span style='color: #ffc107;'>(Trial ends {expires_str})</span>" # yellow
                else:
                    status_message = "<span style='color: #dc3545;'>(Trial Expired)</span>" # red

            st.markdown(
                f"Logged in as **{st.session_state['user_email']}** {status_message}",
                width="content",
                unsafe_allow_html=True
            )
            st.button(
                "Logout",
                key=f"logout_{uuid.uuid4()}",
                on_click=st.logout,
                icon=":material/logout:",
                type="secondary",
            )
        else:
            show_login_info(show_button_only=True)


# announcement shows only upon startup
# main prompt modal only shows for non-premium users
@st.dialog("Welcome to hyperliquid-tools!", width="large", on_dismiss="ignore")
def announcement():
    st.write("""
        This site lets you view your HyperUnit trading / bridging volume, along with some other metrics.

        If you like what you see, please consider subscribing :)

        Enjoy!
    """)
@st.dialog("Latest Updates", width="large", on_dismiss="ignore")
def updates_announcement():
    st.write("""
        🚨 2025-09-11: Updated trade data (previously missing some fills)

        🚨 2025-09-07: Added leaderboard data

        Enjoy!
    """)
if 'startup' not in st.session_state:
    is_full_premium = False
    if 'user_email' in st.session_state:
        user_premium_type = get_user_premium_type(st.session_state['user_email'], logger)
        if user_premium_type == PremiumType.FULL:
            is_full_premium = True

    if is_full_premium:
        updates_announcement()
    else:
        announcement()
    st.session_state['startup'] = True

try:
    info = Info(constants.MAINNET_API_URL, skip_ws=True)
except Exception as e:
    logger.error(f'client error occured: {e}')
    st.error(
        "Hyperliquid API rate limit reached, please try again in a short while")
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
    logger.info(f'unit token mappings: {unit_token_mappings}')
    token_list = [t for t, _ in unit_token_mappings.values()]
    cumulative_trade_data = _get_candlestick_data(
        [k for k in unit_token_mappings.keys()], token_list)

    return unit_token_mappings, token_list, cumulative_trade_data


@st.cache_data(ttl=3600, show_spinner=False)
def get_subaccounts_cached(account: str) -> list:
    subaccounts = info.query_sub_accounts(account)
    return subaccounts if subaccounts is not None else []


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

    try:
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
    except Exception as e:
        logger.error(f'unable to fetch subaccounts of {accounts}: {e}')
        return dict(), dict(), pd.DataFrame, 'Unable to fetch trade history - did you put a valid list of accounts?'

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
    user_fills_rows = []
    try:
        for account in accounts_to_query:
            num_fills_total = 0
            account_fills = []

            # initialisation
            startTime = unitStartTime

            # seems like 2k limit for endpoint counts from the start
            # so start from overall start time then move up til currtime
            while startTime < currTime:  # check back until this date
                endTime = min(currTime, startTime + \
                    int(timedelta(days=numDaysQuerySpan).total_seconds()) * 1000)

                fills_result = get_user_fills(account, startTime, endTime, logger)

                # query again til no more
                num_fills = len(fills_result)
                num_fills_total += num_fills

                logger.info(f'{num_fills} trades for {account} made from {startTime} to {endTime}')

                # logic:
                # 1) if hit limit (2k fills per api call), set start = latest fill timestamp + 1
                # 2) else, slide window fully i.e. start = end + 1
                # always set endTime as startTime + interval
                if num_fills == 2000:
                    latest_fill_timestamp = max(
                        f['time'] for f in fills_result)
                    startTime = latest_fill_timestamp + 1
                    logger.info(f'hit max fills within range, setting next starttime to {startTime}')
                else:
                    startTime = endTime + 1

                # process fills immediately to bin by start date
                # so as to not store too many individual fills in memory to prevent OOM
                for f in fills_result:
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

                        # keep record of all fills in DF
                        # normalise to day start (UTC midnight)
                        trade_day = trade_time.replace(
                            hour=0, minute=0, second=0, microsecond=0)
                        user_fills_rows.append({
                            'start_date': trade_day,
                            'token_name': token_name,
                            'volume_usd': trade_volume,
                            'fees_usd': fee_amt,
                        })

                account_fills.extend(fills_result)

            fills[account] = account_fills
    except Exception as e:
        logging.error(f'error fetching fills for some account(s) in {accounts_to_query}: {e}')
        # TODO need clearer error message
        return dict(), dict(), pd.DataFrame, 'Error fetching fills: did you put a valid list of accounts?'

    user_trades_df = pd.DataFrame(user_fills_rows)
    if not user_trades_df.empty:
        # aggregate to one row per (day, token)
        user_trades_df = (
            user_trades_df
            .groupby(['start_date', 'token_name'], as_index=False)
            .agg(volume_usd=('volume_usd', 'sum'), fees_usd=('fees_usd', 'sum'))
        )

        # make sure start_date is datetime (normalized to midnight UTC already)
        user_trades_df['start_date'] = pd.to_datetime(
            user_trades_df['start_date'], utc=True)

        # generate full date range from the first date to today
        # create multi-index with all date + unique token combinations
        # to reindex DF to fill in all missing dates and tokens
        date_range = pd.date_range(
            start=user_trades_df['start_date'].min(),
            end=pd.to_datetime(datetime.now(tz=timezone.utc).date(), utc=True),
            freq='D'
        )
        unique_tokens = user_trades_df['token_name'].unique()
        full_index = pd.MultiIndex.from_product(
            [date_range, unique_tokens],
            names=['start_date', 'token_name']
        )
        user_trades_df = user_trades_df.set_index(['start_date', 'token_name']).reindex(full_index)

        # fill missing values
        # fill volume and fees with 0
        user_trades_df[['volume_usd', 'fees_usd']] = user_trades_df[['volume_usd', 'fees_usd']].fillna(0)
        # fill the token name (which became NaN during reindexing)
        user_trades_df = user_trades_df.reset_index()
        user_trades_df['token_name'] = user_trades_df.groupby('start_date')['token_name'].ffill().bfill()

        # calculate cumulative sums by token
        user_trades_df['cumulative_volume_usd'] = (
            user_trades_df
            .sort_values(['token_name', 'start_date'])
            .groupby('token_name')['volume_usd']
            .cumsum()
        )
        user_trades_df['cumulative_fees_usd'] = (
            user_trades_df
            .sort_values(['token_name', 'start_date'])
            .groupby('token_name')['fees_usd']
            .cumsum()
        )

    return volume_by_token, accounts_mapping, user_trades_df, None


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


def display_upgrade_section(id: str):
    st.subheader("Your free trial has expired!")

    st.info("If you have previously donated, DM me to get your full access!")

    # update based on current hype market price
    stables_amount = acceptedPayments['USD₮0']['minAmount']
    hype_amt_override = round(stables_amount / get_curr_hype_price(), 2)
    acceptedPayments['HYPE']['minAmount'] = hype_amt_override
    user = get_user(st.session_state['user_email'], logger)
    millis = user.trial_expires_at.microsecond // 1000

    # add user-specific trial expiry date to accepted payments to ensure uniqueness
    uniqueAcceptedPayments = deepcopy(acceptedPayments)
    for k, v in uniqueAcceptedPayments.items():
        existingAmt = v['minAmount']

        if int(existingAmt) == existingAmt: # no decimals (USDT), add after decimals
            uniqueAcceptedPayments[k]['minAmount'] = existingAmt + millis / 1000
        else:   # append decimals as suffix
            uniqueAcceptedPayments[k]['minAmount'] = float(f'{str(existingAmt)}{millis}')

    formattedAmounts = [
        f'{values['minAmount']} {symbol}' for symbol, values in uniqueAcceptedPayments.items()]

    st.text(f"""
        You've seen what detailed analytics this dashboard has to offer. Ready to keep the insights coming?
    """)
    st.markdown("""
        As an early user, you get a :green[25% discount] (already applied)!
    """)
    st.text("""
        For a one-time payment, you get full access to:
        📊 Complete transaction and bridging history
        💼 Advanced breakdowns and comparisons by various metrics
        ✨ And all other future premium features!
    """)
    st.markdown(f"One-time payment: :green[{' or '.join(formattedAmounts)}] to the donation address below on the HyperEVM chain")
    st.markdown("Please send the :green[exact amount] shown (unique to your trial). Once done, submit the transaction hash below for instant reactivation!")
    st.markdown("*P.S. if you wish to pay in another way, DM me (no automated access atm)*")

    with st.form(f"submit_txn_hash_form_{id}"):
        payment_txn_hash = st.text_input(
            "Input your payment transaction hash here")
        # triggered by click or pressing enter
        submitted = st.form_submit_button("Reactivate Premium Access", type="primary")

    if submitted:
        logger.info(
            f'{st.session_state['user_email']} submitted txn hash {payment_txn_hash}, validating')
        error_message = upgrade_to_premium(
            st.session_state['user_email'], payment_txn_hash, 'hyperevm', uniqueAcceptedPayments, logger)
        if error_message:
            st.error(error_message)
        else:
            st.toast("You have successfully subscribed, enjoy the premium features! Reloading the page now...", icon="🔥")
            time.sleep(1)
            # auto-refresh on successful upgrade
            st.rerun()


# main app logic reruns upon any interaction
def main():
    # -------- initialisation and caching --------
    try:
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

        if "last_tab" not in st.session_state:
            st.session_state.last_tab = None

        user_premium_type = get_user_premium_type(
            st.session_state['user_email'], logger) if 'user_email' in st.session_state else False

        st.metric("Current HYPE Price", format_currency(get_curr_hype_price()))

        with st.container(vertical_alignment='center', horizontal=True):
            addresses_input: str = st.text_input(
                "Enter 1 or more hyperliquid accounts (comma-separated)",
                placeholder="Enter 1 or more hyperliquid accounts (comma-separated)",
                label_visibility='collapsed',
                key='hyperliquid_address_input',
            )
            exclude_subaccounts = st.checkbox("Exclude Subaccounts")
            submitted = st.button("Run", type="primary")
    except ClientError as e:
        status_code = e.status_code
        if status_code == 429:
            logger.error(f'client error occured: {e}')
            st.error(
                "Hyperliquid API rate limit reached, please try again in a short while")
        else:
            logger.error(f'client error occured: {e}')
            st.error("Unknown error occurred, please try again in a short while")
        return
    except ServerError as e:
        status_code = e.status_code
        if status_code == 429:
            logger.error(f'server error occured: {e}')
            st.error(
                "Hyperliquid API rate limit reached, please try again in a short while")
        else:
            logger.error(f'server error occured: {e}')
            st.error("Unknown error occurred, please try again in a short while")
        return

    # placeholder that can be cleared and rewritten
    output_placeholder = st.empty()

    accounts = []

    if submitted and addresses_input:
        track_event('run_analysis', { 'addresses_input': addresses_input })

        # upon account(es) update, clear the placeholder immediately
        # so that loading spinner only shows up after
        output_placeholder.empty()

        accounts = [a.strip() for a in addresses_input.split(",") if a]

        with st.spinner(f'Loading data for {", ".join(accounts)}...', show_time=True):
            volume_by_token, accounts_mapping, user_trades_df, err = get_cached_unit_volumes(
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
                    'user_trades_df': user_trades_df,
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
            tab1, tab2, tab3, tab4, tab5 = st.tabs(
                [
                    "💡 Summary",
                    "⚡ Trade Analysis",
                    "🌉 Bridge Analysis",
                    "🏆 Leaderboard (Beta!)",
                    "🔗 HyperEVM Trades (W.I.P)",
                ]
            )

            # default starting, so don't fire any event for it
            st.session_state.last_tab = "summary"

            with tab1:
                if st.session_state.last_tab != "summary":
                    track_event("summary", { 'addresses_input': addresses_input })
                    st.session_state.last_tab = "summary"

                display_summary(df_trade, df_bridging, top_bridged_asset)

            with tab2:
                if st.session_state.last_tab != "view_trade_details":
                    track_event("view_trade_details", { 'addresses_input': addresses_input })
                    st.session_state.last_tab = "view_trade_details"

                if not is_logged_in():
                    show_login_info()
                elif user_premium_type == PremiumType.NONE:
                    display_upgrade_section("trade_data")
                else:
                    # only runs for subscribed users
                    display_trade_data(
                        df_trade,
                        volume_data['accounts_mapping'],
                        volume_data['user_trades_df'],
                        cumulative_trade_data,
                        token_list,
                    )

            with tab3:
                if st.session_state.last_tab != "view_bridge_details":
                    track_event("view_bridge_details", { 'addresses_input': addresses_input })
                    st.session_state.last_tab = "view_bridge_details"

                if not is_logged_in():
                    show_login_info()
                elif user_premium_type == PremiumType.NONE:
                    display_upgrade_section("bridge_data")
                else:
                    # only runs for subscribed users
                    display_bridge_data(
                        raw_bridge_data,
                        df_bridging,
                        top_bridged_asset,
                        processed_bridge_data
                    )

            with tab4:
                if st.session_state.last_tab != "view_trade_leaderboard":
                    track_event("view_trade_leaderboard", { 'addresses_input': addresses_input })
                    st.session_state.last_tab = "view_trade_leaderboard"

                if not is_logged_in():
                    show_login_info()
                elif user_premium_type == PremiumType.NONE:
                    display_upgrade_section("leaderboard_data")
                else:
                    st.info('🚧 This feature is in beta')
                    leaderboard_last_updated = _get_leaderboard_last_updated()
                    st.markdown(f'Last Updated: **{leaderboard_last_updated}** (data is only recalculated every few hours)')

                    leaderboard = _get_leaderboard()
                    leaderboard['total_volume_usd'] = leaderboard['total_volume_usd'].apply(lambda x: format_currency(x))
                    leaderboard['user_address'] = leaderboard['user_address'].apply(lambda x: x[:6] + '...' + x[-6:])
                    leaderboard_formatted = leaderboard[['user_rank', 'user_address', 'total_volume_usd']]

                    # if searched addresses within leaderboard, display them separately
                    leaderboard_searched_addresses = leaderboard_formatted[leaderboard_formatted['user_address'].str.lower().isin([a.lower() for a in accounts])]
                    if not leaderboard_searched_addresses.empty:
                        st.subheader('Searched Addresses')
                        st.dataframe(
                            leaderboard_searched_addresses,
                            hide_index=True,
                            column_config={
                                'user_rank': st.column_config.TextColumn('Rank'),
                                'user_address': st.column_config.TextColumn('Address'),
                                'total_volume_usd': st.column_config.TextColumn('Total Volume (USD)'),
                            },
                        )

                    # display overall leaderboard
                    st.subheader('Overall Leaderboard')
                    st.dataframe(
                        leaderboard_formatted,
                        hide_index=True,
                        column_config={
                            'user_rank': st.column_config.TextColumn('Rank'),
                            'user_address': st.column_config.TextColumn('Address'),
                            'total_volume_usd': st.column_config.TextColumn('Total Volume (USD)'),
                        },
                    )

            with tab5:
                if st.session_state.last_tab != "view_hyperevm_analysis":
                    track_event("view_hyperevm_analysis", { 'addresses_input': addresses_input })
                    st.session_state.last_tab = "view_hyperevm_analysis"

                st.text("🚧 Under development, stay tuned!")

# region leaderboard data cached
@st.cache_data(ttl=3600, show_spinner=False)
def _get_leaderboard_last_updated():
    return get_leaderboard_last_updated(logger)

@st.cache_data(ttl=3600, show_spinner=False)
def _get_leaderboard():
    return get_leaderboard(logger)
# endregion

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
    """
    assumes df is not empty (handled externally)
    """

    # display metrics at top
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Trade Volume", format_currency(
            df['Total Volume'].sum()))
    with col2:
        most_traded = df.iloc[0]['Token']
        st.metric("Top Traded Token", most_traded)
    with col3:
        st.metric("Tokens Traded", len(df))

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Accounts Traded On", num_accounts)
    with col2:
        df['Total Fees'] = df['Token Fees'] + df['USDC Fees']
        total_fees = df['Total Fees'].sum()
        st.metric('Total Fees Paid', format_currency(total_fees))
    with col3:
        st.metric('Total Trades Made', df['Num Trades'].sum())

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
    accounts: list[str],
    user_trades_df: pd.DataFrame,
    token_list: list[str],
):
    """
    assumes trade_df is not empty (handled externally)
    """
    final_cumulative_volume = cumulative_trade_data.groupby('token_name').agg(
        final_cumulative_volume=('cumulative_volume_usd', 'last')
    ).reset_index()

    rows = []
    total_user_volume = 0
    total_exchange_volume = 0

    for token in token_list:
        user_volume = 0
        try:
            user_volume = trade_df[trade_df['Token']
                                   == token].iloc[0]['Total Volume']
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

    with st.expander("More Info about Fills vs. Trades:", expanded=False):
        st.text("""
In every trade, there are 2 parties: buyer and seller. In most central limit order books (CLOBs), including Hyperliquid, each side has a separate fill i.e. one trade contains two fills.

That means that actual trade volume is half of total fill volume, so the percentage of your volume vs. the exchange volume is your volume divided by exchange volume divided by 2.
        """)

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Your Volume (Total)", format_currency(total_user_volume))
    with col2:
        st.metric("Total Exchange Unit Trading Volume",
                  format_currency(total_exchange_volume))
    with col3:
        st.metric("Share of Total Exchange Unit Trading Volume",
                  f"{(total_user_volume / total_exchange_volume / 2.0 * 100):.10f}%")

    df_cumulative = pd.DataFrame(rows)
    df_cumulative = df_cumulative.sort_values(
        'Market Volume (USD)', ascending=False)

    # handle sorting before formatting values for display
    user_vol_token_order = (
        user_trades_df.groupby('token_name')['cumulative_volume_usd']
        .max().sort_values(ascending=False).index.tolist()
    )
    exchange_vol_token_order = (
        df_cumulative.groupby('Asset')['Market Volume (USD)']
        .max().sort_values(ascending=False).index.tolist()
    )
    user_fees_token_order = (
        user_trades_df.groupby('token_name')['cumulative_fees_usd']
        .max().sort_values(ascending=False).index.tolist()
    )

    # format for display only
    df_cumulative['Your Volume (USD)'] = df_cumulative['Your Volume (USD)'].apply(
        format_currency)
    df_cumulative['Market Volume (USD)'] = df_cumulative['Market Volume (USD)'].apply(
        format_currency)
    df_cumulative['Your Share (%)'] = df_cumulative['Your Share (%)'].map(
        lambda x: f"{x:.6f}%")

    with st.expander("Volume Trends and Breakdown", expanded=False):
        st.subheader("Trading Volume Breakdown")
        st.dataframe(df_cumulative, hide_index=True)

        # user volume
        col1, col2 = st.columns(2, gap="large")
        with col1:
            st.subheader("Daily User Volume")
            fig = px.bar(
                user_trades_df,
                x='start_date',
                y='volume_usd',
                color='token_name',
                labels={
                    'volume_usd': 'Volume (USD)',
                    'start_date': 'Date',
                    'token_name': 'Token'
                },
                category_orders={'token_name': user_vol_token_order}
            )
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            st.subheader("Cumulative User Volume")
            fig = px.bar(
                user_trades_df,
                x='start_date',
                y='cumulative_volume_usd',
                color='token_name',
                labels={
                    'cumulative_volume_usd': 'Cumulative Volume (USD)',
                    'start_date': 'Date',
                    'token_name': 'Token'
                },
                category_orders={'token_name': user_vol_token_order}
            )
            st.plotly_chart(fig, use_container_width=True)

        # exchange volume
        col1, col2 = st.columns(2, gap="large")
        with col1:
            st.subheader("Daily Exchange Volume")
            fig = px.bar(
                cumulative_trade_data,
                x='start_date',
                y='volume_usd',
                color='token_name',
                labels={
                    'volume_usd': 'Volume (USD)',
                    'start_date': 'Date',
                    'token_name': 'Token'
                },
                category_orders={'token_name': exchange_vol_token_order}
            )
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            st.subheader("Cumulative Exchange Volume")
            fig = px.bar(
                cumulative_trade_data,
                x='start_date',
                y='cumulative_volume_usd',
                color='token_name',
                labels={
                    'cumulative_volume_usd': 'Cumulative Volume (USD)',
                    'start_date': 'Date',
                    'token_name': 'Token'
                },
                category_orders={'token_name': exchange_vol_token_order}
            )
            st.plotly_chart(fig, use_container_width=True)

        # user fees
        col1, col2 = st.columns(2, gap="large")
        with col1:
            st.subheader("Daily User Fees")
            fig = px.bar(
                user_trades_df,
                x='start_date',
                y='fees_usd',
                color='token_name',
                labels={
                    'fees_usd': 'Fees (USD)',
                    'start_date': 'Date',
                    'token_name': 'Token'
                },
                category_orders={'token_name': user_fees_token_order}
            )
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            st.subheader("Cumulative User Fees")
            fig = px.bar(
                user_trades_df,
                x='start_date',
                y='cumulative_fees_usd',
                color='token_name',
                labels={
                    'cumulative_fees_usd': 'Cumulative Fees (USD)',
                    'start_date': 'Date',
                    'token_name': 'Token'
                },
                category_orders={'token_name': user_fees_token_order}
            )
            st.plotly_chart(fig, use_container_width=True)


def display_trade_data(
    df_trade: pd.DataFrame,
    accounts_mapping: dict,
    user_trades_df: pd.DataFrame,
    cumulative_trade_data,
    token_list: list[str],
):
    # show raw data in expander
    if df_trade.empty:
        st.warning(
            "No trades on Unit tokens found - if you think this is an error, contact me")
    else:
        display_trade_volume_table(df_trade, len(accounts_mapping))

        display_trade_volume_info(df_trade, cumulative_trade_data, list(
            accounts_mapping.keys()), user_trades_df, token_list)

        with st.expander("Raw Data"):
            st.json(accounts_mapping)
            st.dataframe(df_trade)


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
    st.markdown(footer_html, unsafe_allow_html=True)
    # render copy script in a separate component to avoid CSP issues
    components.html(copy_script, height=0)
    # endregion