from copy import deepcopy
import json
from typing import Any
import os
import sys
import time
from dotenv import load_dotenv
import requests
from src.auth.db_utils import init_db, PremiumType, get_user_premium_type, upgrade_to_premium, start_trial_if_new_user, get_user
from src.utils.utils import DATE_FORMAT, format_currency, get_current_timestamp_millis, get_kinetiq_token_mappings
from datetime import datetime, timedelta, timezone
import pandas as pd
from hyperliquid.info import Info
from hyperliquid.utils import constants
from hyperliquid.utils.error import ClientError, ServerError
import streamlit as st
import plotly.express as px
from src.trade.trade_data import get_candlestick_data
from src.consts import NON_LOGGED_IN_TRADES_TOTAL, kinetiqStartTime, oneDayInS, acceptedPayments, AccountStats
import uuid

# setup and configure logging
import logging
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

st.set_page_config(
    'Kinetiq Markets Tools',
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
    payload: dict[str, Any] = {
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
        key=str(uuid.uuid4()),
        on_click=handle_login_click,
        icon=":material/login:",
        type="primary"
    )


col1, col2 = st.columns([1, 1], vertical_alignment='center')
with col1:
    with st.container(vertical_alignment='center', horizontal=True, horizontal_alignment="left"):
        st.header("Kinetiq Volume Tracker")
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
                    assert user.trial_expires_at is not None
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


@st.cache_resource(show_spinner=False)
def safe_get_hyperliquid_info_obj() -> Info:
    """
    error handling for getting hyperliquid Info object

    will hard stop if have error getting object (e.g. rate limit errors)
    """
    try:
        return Info(constants.MAINNET_API_URL, skip_ws=True)
    except Exception as e:
        logger.error(f"Error creating Info client: {e}")
        st.error("Hyperliquid API rate limit reached, please try again in a short while")
        st.stop()   # hard stop


@st.cache_data(ttl=3600, show_spinner=False)
def _get_cached_kinetiq_token_mappings() -> list[str]:
    info = safe_get_hyperliquid_info_obj()
    return get_kinetiq_token_mappings(info)


@st.cache_data(ttl=oneDayInS, show_spinner=False)
def _get_candlestick_data(_token_ids: list[str], _token_names: list[str]):
    """
    cache daily just to get OHLCV prices
    """
    info = safe_get_hyperliquid_info_obj()
    return get_candlestick_data(info, _token_ids, _token_names)


@st.cache_data(ttl=60, show_spinner=False)
def get_mid_prices():
    info = safe_get_hyperliquid_info_obj()
    return info.all_mids()

@st.cache_data(ttl=60, show_spinner=False)
def get_curr_hype_price():
    prices = get_mid_prices()
    return float(prices['@107'])

@st.cache_data(ttl=60, show_spinner=False)
def get_curr_btc_price():
    prices = get_mid_prices()
    return float(prices['@142'])

def load_kinetiq_markets_data():
    token_list = _get_cached_kinetiq_token_mappings()
    candlestick_data = _get_candlestick_data(token_list, token_list)
    return token_list, candlestick_data


@st.cache_data(ttl=3600, show_spinner=False)
def get_subaccounts_cached(account: str) -> list:
    info = safe_get_hyperliquid_info_obj()
    subaccounts = info.query_sub_accounts(account)
    return subaccounts if subaccounts is not None else []


TRADE_COUNT_FETCH_LIMIT = -1    # -1: no limit


# error handling done externally
@st.cache_data(ttl=60, show_spinner=False)
def _get_cached_fills(account: str, startTime: int, endTime: int):
    load_dotenv()

    fills_result = requests.post(
        "https://api.hydromancer.xyz/info",
        data=json.dumps({
            "type": "userFillsByTime",  # up to 10k total, then need to query from s3
            "user": account,
            "aggregateByTime": True,
            "startTime": startTime,
            "endTime": endTime,
        }),
        headers={
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {os.getenv("HYDROMANCER_API_KEY")}'
        },
    )

    fills_result.raise_for_status()

    return fills_result.json()


def get_cached_trade_volumes(
    accounts_to_query: list[str],
    token_coin_name_mapping: dict[str, str],
    direction_mapping: dict[str, str],  # map from directions in fills api (for filtering) to display DF e.g. "Open Long": "Buy"
    volume_by_token: dict[str, dict],
    accounts_mapping: dict[str, AccountStats],
    total_trade_volume: dict[str, float],
    curr_timestamp_millis: int,
):
    # validation
    for v in direction_mapping.values():
        if v not in ['Buy', 'Sell']:
            raise Exception("Invalid direction mapping")

    numDaysQuerySpan = 30
    user_fills_rows = []

    non_logged_in_limit_trade_count = TRADE_COUNT_FETCH_LIMIT

    for account in accounts_to_query:
        num_fills_total = 0

        total_trade_volume_account = 0.0

        # initialisation
        startTime = kinetiqStartTime   # TODO change to kinetiq start time

        # seems like 2k limit for endpoint counts from the start
        # so start from overall start time then move up til currtime
        while startTime < curr_timestamp_millis:  # check back until this date
            endTime = min(curr_timestamp_millis, startTime + \
                int(timedelta(days=numDaysQuerySpan).total_seconds()) * 1000)

            fills_result = _get_cached_fills(account, startTime, endTime)

            # query again til no more
            num_fills = len(fills_result)
            num_fills_total += num_fills

            # limit num fills shown if not logged in
            if "user_email" not in st.session_state and num_fills_total > NON_LOGGED_IN_TRADES_TOTAL:
                non_logged_in_limit_trade_count = num_fills_total
                break

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
                if coin in token_coin_name_mapping.keys() and direction in direction_mapping.keys():
                    token_name = token_coin_name_mapping[coin]

                    # only count unit fills
                    volume_by_token[token_name]['Num Trades'] += 1
                    accounts_mapping[account]['Num Trades'] += 1

                    price = float(f['px'])

                    trade_volume = float(f['sz']) * price
                    trade_time = datetime.fromtimestamp(
                        f['time'] / 1000, tz=timezone.utc)

                    total_trade_volume_account += trade_volume

                    # direction
                    direction_mapped = direction_mapping[direction]
                    prev_first_txn = volume_by_token[token_name]['Direction'][direction_mapped]['First Txn']
                    if prev_first_txn is None or trade_time < prev_first_txn:
                        volume_by_token[token_name]['Direction'][direction_mapped]['First Txn'] = trade_time
                    prev_last_txn = volume_by_token[token_name]['Direction'][direction_mapped]['Last Txn']
                    if prev_last_txn is None or trade_time > prev_last_txn:
                        volume_by_token[token_name]['Direction'][direction_mapped]['Last Txn'] = trade_time
                    volume_by_token[token_name]['Direction'][direction_mapped]['Volume'] += trade_volume

                    # trade type (maker / taker)
                    trade_type = 'Taker' if f['crossed'] is True else 'Maker'
                    prev_first_txn = volume_by_token[token_name]['Type'][trade_type]['First Txn']
                    if prev_first_txn is None or trade_time < prev_first_txn:
                        volume_by_token[token_name]['Type'][trade_type]['First Txn'] = trade_time
                    prev_last_txn = volume_by_token[token_name]['Type'][trade_type]['Last Txn']
                    if prev_last_txn is None or trade_time > prev_last_txn:
                        volume_by_token[token_name]['Type'][trade_type]['Last Txn'] = trade_time
                    volume_by_token[token_name]['Type'][trade_type]['Volume'] += trade_volume

                    # TODO verify this for non-spot e.g. xyz
                    fee_in_stables = f['feeToken'] in ['USDC', 'USDH']

                    if fee_in_stables:
                        fee_amt = float(f['fee'])
                        accounts_mapping[account]['Quote Fees'] += fee_amt
                        volume_by_token[token_name]['Quote Fees'] += fee_amt
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

        total_trade_volume[account] = total_trade_volume_account

    return total_trade_volume, non_logged_in_limit_trade_count, user_fills_rows


@st.cache_data(ttl=60 * 5, show_spinner=False)
def get_cached_km_volumes(
    accounts: list[str],
    km_token_list: list[str],
    curr_timestamp_millis: int,
):
    """
    get unit volumes with caching
    """
    # TODO put this into separate helper function
    # account: { remarks (subaccount of another), num fills }
    accounts_mapping: dict[str, AccountStats] = dict()

    total_trade_volume: dict[str, float] = dict()  # { account: total trade volume }

    try:
        for account in accounts:
            accounts_mapping[account] = {
                "Name": "",
                "Num Trades": 0,
                "Token Fees": 0.0,
                "Quote Fees": 0.0,
            }
    except Exception as e:
        logger.error(f'unable to fetch subaccounts of {accounts}: {e}')
        return dict(), dict(), total_trade_volume, pd.DataFrame, False, 'Unable to fetch subaccounts - did you put a valid list of accounts?'

    accounts_to_query = list(accounts_mapping.keys())

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
            'Quote Fees': 0.0,
            'Num Trades': 0,
        }
        for t in km_token_list
    }

    # try:
    formatted_token_mapping = { t: t for t in km_token_list }
    direction_mapping = {   # must map to "Buy" or "Sell" only
        'Open Long': 'Buy',
        'Close Long': "Sell",
        'Open Short': "Sell",
        'Close Short': "Buy",
        'Long > Short': "Sell",
        'Short > Long': "Buy",
    }
    logger.info(f'getting trade volumes for tokens {", ".join(km_token_list)}')
    total_trade_volume, non_logged_in_limit_trade_count, user_fills_rows = get_cached_trade_volumes(
        accounts_to_query,
        formatted_token_mapping,
        direction_mapping,
        volume_by_token,
        accounts_mapping,
        total_trade_volume,
        curr_timestamp_millis
    )
    # except Exception as e:
    #     logging.error(f'error fetching xyz fills for some account(s) in {accounts_to_query}: {e}')
    #     # TODO need more explicit error message
    #     return dict(), dict(), total_trade_volume, pd.DataFrame, False, 'Error fetching Kinetiq Markets fills: did you put a valid list of accounts?'

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

    return volume_by_token, accounts_mapping, total_trade_volume, user_trades_df, non_logged_in_limit_trade_count, None


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
                'Quote Fees': volumes['Quote Fees'],
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
    if user is None or user.trial_expires_at is None:
        st.error("Unable to load user details. Please refresh.")
        return
    millis = user.trial_expires_at.microsecond // 1000

    # add user-specific trial expiry date to accepted payments to ensure uniqueness
    uniqueAcceptedPayments = deepcopy(acceptedPayments)
    for k, v in uniqueAcceptedPayments.items():
        existingAmt = float(str(v['minAmount']))

        if int(existingAmt) == existingAmt: # no decimals (USDT), add after decimals
            uniqueAcceptedPayments[k]['minAmount'] = existingAmt + millis / 1000
        else:   # append decimals as suffix
            uniqueAcceptedPayments[k]['minAmount'] = float(f'{str(existingAmt)}{millis}')

    formattedAmounts = [
        f'{values['minAmount']} {symbol}' for symbol, values in uniqueAcceptedPayments.items()]

    st.text(f"""
        You've seen what detailed analytics this dashboard has to offer. Ready to keep the insights coming?
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
                km_token_list, km_candlestick_data = load_kinetiq_markets_data()

                # save into session_state so don't re-init
                st.session_state.km_token_list = km_token_list
                st.session_state.km_candlestick_data = km_candlestick_data
                st.session_state.init_done = True
        else:
            # subsequent runs: refresh cached data
            km_token_list, km_candlestick_data = load_kinetiq_markets_data()

            # update session_state with latest values
            st.session_state.km_token_list = km_token_list
            st.session_state.km_candlestick_data = km_candlestick_data
        # use cached/session values
        km_token_list = st.session_state.km_token_list
        km_candlestick_data = st.session_state.km_candlestick_data

        if "last_tab" not in st.session_state:
            st.session_state.last_tab = None

        user_premium_type = get_user_premium_type(
            st.session_state['user_email'], logger) if 'user_email' in st.session_state else False

        with st.container(vertical_alignment='center', horizontal=True):
            st.metric("HYPE Price", f'${get_curr_hype_price()}', width="content")
            st.metric(" ", " ", width="content", label_visibility="hidden")    # hack for some horizontal spacing
            st.metric("BTC Price", f'${get_curr_btc_price()}', width="content")

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
        if not exclude_subaccounts:
            for account in accounts:
                subaccounts = get_subaccounts_cached(account)
                for sub in subaccounts:
                    subaccount = sub['subAccountUser']
                    accounts.append(subaccount)

        with st.spinner('Loading data...', show_time=True):
            curr_timestamp_millis = get_current_timestamp_millis()

            # TODO some of these return types are unused?
            volume_by_token_km, accounts_mapping_km, total_trade_volume_km, user_trades_df_km, non_logged_in_limit_trade_count, err_km = get_cached_km_volumes(
                accounts, km_token_list, curr_timestamp_millis)

        with output_placeholder.container():
            if err_km is not None:
                st.error(err_km)
            else:
                # store results in session state so they persist across tab switches
                st.session_state['volume_data_km'] = {
                    'volume_by_token': volume_by_token_km,
                    'accounts_mapping': accounts_mapping_km,
                    'user_trades_df': user_trades_df_km,
                }

                if non_logged_in_limit_trade_count == TRADE_COUNT_FETCH_LIMIT:
                    if st.session_state.get('non_logged_in_limit_trade_count', False):
                        del st.session_state['non_logged_in_limit_trade_count']
                else:
                    st.session_state['non_logged_in_limit_trade_count'] = non_logged_in_limit_trade_count

    # show content in output placeholder only if have data
    if 'volume_data_km' in st.session_state:
        with output_placeholder.container():
            volume_data_km = st.session_state['volume_data_km']
            df_trade_km = create_volume_df(volume_data_km['volume_by_token'])

            if st.session_state.get('non_logged_in_limit_trade_count', False):
                st.warning(f'Only showing latest {non_logged_in_limit_trade_count} trades: log in to see full data')

            # create tabs
            tab_summary, tab_km_trade = st.tabs(
                [
                    "💡 Summary",
                    "⚡ Kinetiq Markets Trade Analysis",
                    # "🏆 Kinetiq Markets Leaderboard (new)",
                ]
            )

            # default starting, so don't fire any event for it
            st.session_state.last_tab = "summary"

            with tab_summary:
                if st.session_state.last_tab != "summary":
                    track_event("summary", { 'addresses_input': addresses_input })
                    st.session_state.last_tab = "summary"

                display_summary(df_trade_km)

            with tab_km_trade:
                if st.session_state.last_tab != "view_km_trade_details":
                    track_event("view_km_trade_details", { 'addresses_input': addresses_input })
                    st.session_state.last_tab = "view_km_trade_details"

                if not is_logged_in():
                    show_login_info()
                elif user_premium_type == PremiumType.NONE:
                    display_upgrade_section("trade_data_km")
                else:
                    # only runs for subscribed users
                    display_trade_data(
                        df_trade_km,
                        volume_data_km['accounts_mapping'],
                        volume_data_km['user_trades_df'],
                        km_candlestick_data,
                        km_token_list,
                    )

            # with tab_km_leaderboard:
            #     if st.session_state.last_tab != "view_km_leaderboard":
            #         track_event("view_km_leaderboard", { 'addresses_input': addresses_input })
            #         st.session_state.last_tab = "view_km_leaderboard"

            #     if not is_logged_in():
            #         show_login_info()
            #     elif user_premium_type == PremiumType.NONE:
            #         display_upgrade_section("km_leaderboard_data")
            #     else:
            #         leaderboard_last_updated = _get_km_leaderboard_last_updated()
            #         st.markdown(f'Last Updated: **{leaderboard_last_updated}**')

            #         leaderboard = _get_km_leaderboard()
            #         leaderboard['total_volume_usd'] = leaderboard['total_volume_usd'].apply(lambda x: format_currency(x))
            #         leaderboard_formatted = leaderboard[['user_rank', 'user_address', 'total_volume_usd']]

            #         # if searched addresses within leaderboard, display them separately
            #         # else get data based on query
            #         accounts_lowercase = [a.lower() for a in accounts]
            #         leaderboard_searched_addresses = leaderboard_formatted[leaderboard_formatted['user_address'].str.lower().isin(accounts_lowercase)]
            #         st.subheader('Searched Addresses')

            #         # manually-obtained trade volumes
            #         ranks = [
            #             (f'{len(leaderboard_formatted)}+', addr.lower(), format_currency(total_vol))
            #             for addr, total_vol in total_trade_volume.items()
            #         ]
            #         ranks_df = pd.DataFrame(ranks, columns=["user_rank", "user_address", "total_volume_usd"])

            #         leaderboard_searched_addresses = pd.concat([leaderboard_searched_addresses, ranks_df], ignore_index=True)
            #         # keep first one i.e. add if not already in full searched list
            #         leaderboard_searched_addresses = leaderboard_searched_addresses.drop_duplicates(subset=["user_address"], keep="first")  # keep DataFrame rows over tuples
            #         leaderboard_searched_addresses.loc[:, 'user_address'] = leaderboard_searched_addresses['user_address'].apply(lambda x: x[:6] + '...' + x[-6:])
            #         st.dataframe(
            #             leaderboard_searched_addresses,
            #             hide_index=True,
            #             column_config={
            #                 'user_rank': st.column_config.TextColumn('Rank'),
            #                 'user_address': st.column_config.TextColumn('Address'),
            #                 'total_volume_usd': st.column_config.TextColumn('Total Volume (USD)'),
            #             },
            #         )

            #         # display overall leaderboard
            #         leaderboard_formatted.loc[:, 'user_address'] = leaderboard_formatted['user_address'].apply(lambda x: x[:6] + '...' + x[-6:])
            #         leaderboard_filtered = leaderboard_formatted.iloc[:1000]
            #         st.subheader('Overall Kinetiq Markets Leaderboard')
            #         st.dataframe(
            #             leaderboard_filtered,
            #             hide_index=True,
            #             column_config={
            #                 'user_rank': st.column_config.TextColumn('Rank'),
            #                 'user_address': st.column_config.TextColumn('Address'),
            #                 'total_volume_usd': st.column_config.TextColumn('Total Volume (USD)'),
            #             },
            #         )

# # region xyz leaderboard data cached
# @st.cache_data(ttl=3600, show_spinner=False)
# def _get_km_leaderboard_last_updated():
#     return get_km_leaderboard_last_updated()

# @st.cache_data(ttl=3600, show_spinner=False)
# def _get_km_leaderboard():
#     return get_km_leaderboard()
# # endregion


# --------------- display ---------------
def display_summary(
    df_trade_km: pd.DataFrame,
):
    # kinetiq trade data
    if df_trade_km.empty:
        st.warning(
            "No trades on Kinetiq Markets tokens found - if you think this is an error, contact me")
    else:
        st.subheader("Kinetiq Markets Trade Data")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Trade Volume", format_currency(
                df_trade_km['Total Volume'].sum()))
        with col2:
            st.metric("Top Traded Token", df_trade_km.iloc[0]['Token'])
        with col3:
            st.metric('Total Trades Made', df_trade_km['Num Trades'].sum())


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
        df['Total Fees'] = df['Token Fees'] + df['Quote Fees']
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
    candlestick_data: pd.DataFrame,
    accounts: list[str],
    user_trades_df: pd.DataFrame,
    token_list: list[str],
):
    """
    assumes trade_df is not empty (handled externally)
    """
    final_cumulative_volume = candlestick_data.groupby('token_name').agg(
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
                candlestick_data,
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
                candlestick_data,
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
    km_candlestick_data,
    token_list: list[str],
):
    if df_trade.empty:
        st.warning(
            "No trades on Kinetiq tokens found - if you think this is an error, contact me")
    else:
        display_trade_volume_table(df_trade, len(accounts_mapping))

        display_trade_volume_info(df_trade, km_candlestick_data, list(
            accounts_mapping.keys()), user_trades_df, token_list)


if __name__ == '__main__':
    main()