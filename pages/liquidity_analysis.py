import pandas as pd
import requests
import streamlit as st

st.set_page_config(
    'Liquidity Analysis',
    "📐",
    layout="wide",
)

st.header("Liquidity Analysis across Exchanges")

# setup and configure logging
import logging
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

CLIP_SIZES = [1_000, 10_000, 50_000, 100_000, 500_000]
TAKER_FEES_BPS = {
    "Hyperliquid": 0.045,
    "Extended": 0.025,
    "Lighter": 0.0,
    "Paradex": 0.0,
    "Pacifica": 0.04,
}

# region l2 orderbook fetchers
def fetch_hyperliquid_orderbook(token: str):
    resp = requests.post(
        "https://api.Hyperliquid.xyz/info",
        json={"type": "l2Book", "coin": token.upper()},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()

    if not data.get("levels") or len(data["levels"]) < 2:
        raise ValueError(f"Invalid orderbook for {token} on Hyperliquid")

    bids = [{"price": float(l["px"]), "qty": float(l["sz"])} for l in data["levels"][0]]
    asks = [{"price": float(l["px"]), "qty": float(l["sz"])} for l in data["levels"][1]]

    return {"bids": bids, "asks": asks, "exchange": "Hyperliquid", "token": token}


def fetch_paradex_orderbook(token: str):
    pair = f"{token.upper()}-USD-PERP"
    resp = requests.get(
        f"https://api.prod.Paradex.trade/v1/orderbook/{pair}/interactive?depth=50",
        headers={"Accept": "application/json"},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()

    if not data.get("bids") or not data.get("asks"):
        raise ValueError(f"Invalid orderbook for {token} on Paradex")

    bids = [{"price": float(px), "qty": float(qty)} for px, qty in data["bids"]]
    asks = [{"price": float(px), "qty": float(qty)} for px, qty in data["asks"]]

    rpi_data = None
    if data.get("best_bid_interactive") and data.get("best_ask_interactive"):
        api_bid = float(data["best_bid_api"][0]) if data.get("best_bid_api") else None
        api_ask = float(data["best_ask_api"][0]) if data.get("best_ask_api") else None
        rpi_bid = float(data["best_bid_interactive"][0])
        rpi_ask = float(data["best_ask_interactive"][0])

        api_spread_bps = (
            ((api_ask - api_bid) / api_bid) * 10_000 if api_bid and api_ask else None
        )
        rpi_spread_bps = ((rpi_ask - rpi_bid) / rpi_bid) * 10_000

        rpi_data = {
            "apiBid": api_bid,
            "apiAsk": api_ask,
            "apiSpreadBps": api_spread_bps,
            "rpiBid": rpi_bid,
            "rpiAsk": rpi_ask,
            "rpiSpreadBps": rpi_spread_bps,
        }

    return {
        "bids": bids,
        "asks": asks,
        "exchange": "Paradex",
        "token": token,
        "rpiData": rpi_data,
    }


def fetch_extended_orderbook(token: str):
    market = f"{token.upper()}-USD"
    resp = requests.get(
        f"https://api.starknet.Extended.exchange/api/v1/info/markets/{market}/orderbook",
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("status") != "OK" or "data" not in data:
        raise ValueError(f"Invalid orderbook for {token} on Extended")

    ob = data["data"]
    bids = [{"price": float(l["price"]), "qty": float(l["qty"])} for l in ob["bid"]]
    asks = [{"price": float(l["price"]), "qty": float(l["qty"])} for l in ob["ask"]]
    return {"bids": bids, "asks": asks, "exchange": "Extended", "token": token}


def fetch_lighter_orderbook(token: str):
    details = requests.get(
        "https://mainnet.zklighter.elliot.ai/api/v1/orderBookDetails", timeout=10
    )
    details.raise_for_status()
    d = details.json()

    market = next(
        (
            m
            for m in d.get("order_book_details", [])
            if str(m.get("symbol", "")).upper() == token.upper()
            and m.get("status") == "active"
        ),
        None,
    )
    if not market:
        raise ValueError(f"Token {token} not available on Lighter")

    mid = market["market_id"]
    res = requests.get(
        f"https://mainnet.zklighter.elliot.ai/api/v1/orderBookOrders?market_id={mid}&limit=50",
        timeout=10,
    )
    res.raise_for_status()
    data = res.json()

    bids = sorted(
        (
            {
                "price": float(l["price"]),
                "qty": float(l["remaining_base_amount"]),
            }
            for l in data.get("bids", [])
        ),
        key=lambda x: x["price"],
        reverse=True,
    )
    asks = sorted(
        (
            {
                "price": float(l["price"]),
                "qty": float(l["remaining_base_amount"]),
            }
            for l in data.get("asks", [])
        ),
        key=lambda x: x["price"],
    )

    return {"bids": bids, "asks": asks, "exchange": "Lighter", "token": token}


def fetch_pacifica_orderbook(token: str):
    symbol = token.upper()

    res = requests.get(
        f"https://api.Pacifica.fi/api/v1/book?symbol={symbol}",
        timeout=10,
    )
    res.raise_for_status()
    data = res.json()

    if not data.get("success") or not data.get("data") or not data["data"].get("l"):
        raise ValueError(f"Invalid orderbook for {token} on Pacifica")

    orderbook = data["data"]
    bids_raw = orderbook["l"][0] if orderbook["l"] and len(orderbook["l"]) > 0 else []
    asks_raw = orderbook["l"][1] if orderbook["l"] and len(orderbook["l"]) > 1 else []

    if not bids_raw or not asks_raw:
        raise ValueError(f"Empty orderbook for {token} on Pacifica")

    bids = [
        {"price": float(level["p"]), "qty": float(level["a"])}
        for level in bids_raw
    ]
    asks = [
        {"price": float(level["p"]), "qty": float(level["a"])}
        for level in asks_raw
    ]

    return {"bids": bids, "asks": asks, "exchange": "Pacifica", "token": token}
# endregion

ORDERBOOK_FETCHERS = {
    "Hyperliquid": fetch_hyperliquid_orderbook,
    "Paradex": fetch_paradex_orderbook,
    "Extended": fetch_extended_orderbook,
    "Lighter": fetch_lighter_orderbook,
    "Pacifica": fetch_pacifica_orderbook,
}

# region slippage calculations
def calculate_slippage(orderbook, size_usd, side="buy"):
    levels = (
        sorted(orderbook["asks"], key=lambda x: x["price"])
        if side == "buy"
        else sorted(orderbook["bids"], key=lambda x: -x["price"])
    )

    if len(levels) == 0:
        return {"slippage": None, "filled": False, "error": "No liquidity"}

    best_bid = max(b["price"] for b in orderbook["bids"])
    best_ask = min(a["price"] for a in orderbook["asks"])
    mid_price = (best_bid + best_ask) / 2
    best_price = best_ask if side == "buy" else best_bid

    remaining_usd = size_usd
    total_qty = 0
    total_cost = 0
    levels_used = 0
    depth_used_usd = 0
    worst_price = best_price

    # full pricefor
    for level in levels:
        price = level["price"]
        qty_available = level["qty"]
        value_at_level = qty_available * price

        levels_used += 1
        worst_price = price

        if remaining_usd <= value_at_level:
            qty_taken = remaining_usd / price
            total_qty += qty_taken
            total_cost += remaining_usd
            depth_used_usd += remaining_usd
            remaining_usd = 0
            break
        else:
            total_qty += qty_available
            total_cost += value_at_level
            depth_used_usd += value_at_level
            remaining_usd -= value_at_level

    effective_spread = abs((worst_price - best_price) / best_price) * 100

    # not fully filled - insufficient liquidity
    if remaining_usd > 0:
        filled_usd = size_usd - remaining_usd
        filled_percent = (filled_usd / size_usd) * 100

        if total_qty == 0:
            return {
                "slippage": None,
                "filled": False,
                "filledPercent": 0,
                "error": "No liquidity",
            }

        avg_price = total_cost / total_qty
        slippage = (
            ((avg_price - mid_price) / mid_price) * 100
            if side == "buy"
            else ((mid_price - avg_price) / mid_price) * 100
        )

        return {
            "slippage": round(slippage, 6),
            "slippageBps": round(slippage * 100, 2),
            "effectiveSpreadBps": round(effective_spread * 100, 2),
            "filled": False,
            "filledPercent": round(filled_percent, 2),
            "levelsUsed": levels_used,
            "depthUsedUsd": round(depth_used_usd),
            "bestPrice": round(best_price, 2),
            "worstPrice": round(worst_price, 2),
            "avgPrice": round(avg_price, 2),
        }

    # fully filled
    avg_price = total_cost / total_qty
    slippage = (
        ((avg_price - mid_price) / mid_price) * 100
        if side == "buy"
        else ((mid_price - avg_price) / mid_price) * 100
    )

    return {
        "slippage": round(slippage, 6),
        "slippageBps": round(slippage * 100, 2),
        "effectiveSpreadBps": round(effective_spread * 100, 2),
        "filled": True,
        "filledPercent": 100,
        "levelsUsed": levels_used,
        "depthUsedUsd": round(depth_used_usd),
        "bestPrice": round(best_price, 2),
        "worstPrice": round(worst_price, 2),
        "avgPrice": round(avg_price, 2),
    }


def analyze_orderbook(orderbook):
    if not orderbook["bids"] or not orderbook["asks"]:
        return {"exchange": orderbook["exchange"], "error": "Empty orderbook"}

    best_bid = max(b["price"] for b in orderbook["bids"])
    best_ask = min(a["price"] for a in orderbook["asks"])
    mid_price = (best_bid + best_ask) / 2
    spread = ((best_ask - best_bid) / best_bid) * 100

    taker_fee_bps = (TAKER_FEES_BPS.get(orderbook["exchange"], 0)) * 100

    result = {
        "exchange": orderbook["exchange"],
        "token": orderbook["token"],
        "midPrice": round(mid_price, 2),
        "spreadBps": round(spread * 100, 2),
        "takerFeeBps": taker_fee_bps,
        "rpiData": orderbook.get("rpiData"),
        "slippage": {},
    }

    for size in CLIP_SIZES:
        buy_slip = calculate_slippage(orderbook, size, "buy")
        sell_slip = calculate_slippage(orderbook, size, "sell")

        # avg slippage
        if buy_slip["slippage"] is not None and sell_slip["slippage"] is not None:
            avg_slip = (buy_slip["slippage"] + sell_slip["slippage"]) / 2
        else:
            avg_slip = buy_slip["slippage"] or sell_slip["slippage"]

        # avg effective spread
        if (
            "effectiveSpreadBps" in buy_slip
            and "effectiveSpreadBps" in sell_slip
        ):
            avg_eff_spread = (
                buy_slip["effectiveSpreadBps"] + sell_slip["effectiveSpreadBps"]
            ) / 2
        else:
            avg_eff_spread = (
                buy_slip.get("effectiveSpreadBps")
                or sell_slip.get("effectiveSpreadBps")
                or 0
            )

        avg_bps = round(avg_slip * 100, 2) if avg_slip is not None else None
        total_cost_bps = (
            round(avg_bps + taker_fee_bps, 2) if avg_bps is not None else None
        )

        size_key = f"${size/1000}k"

        result["slippage"][size_key] = {
            "avgBps": avg_bps,
            "takerFeeBps": taker_fee_bps,
            "totalCostBps": total_cost_bps,
            "effectiveSpreadBps": round(avg_eff_spread, 2),
            "filled": buy_slip["filled"] and sell_slip["filled"],
            "levels": {
                "buy": buy_slip.get("levelsUsed", 0),
                "sell": sell_slip.get("levelsUsed", 0),
            },
            "depthUsed": {
                "buy": buy_slip.get("depthUsedUsd", 0),
                "sell": sell_slip.get("depthUsedUsd", 0),
            },
        }

    return result


def rank_exchanges(analyses):
    rankings = {"bySlippage": {}, "byTotalCost": {}}

    for size_key in ["$1k", "$10k", "$100k", "$500k"]:
        exchange_data = []

        for analysis in analyses:
            if analysis.get("error"):
                continue

            sd = analysis["slippage"].get(size_key)
            if sd and sd["avgBps"] is not None:
                exchange_data.append({
                    "exchange": analysis["exchange"],
                    "slippageBps": sd["avgBps"],
                    "totalCostBps": sd["totalCostBps"],
                    "takerFeeBps": sd["takerFeeBps"],
                    "filled": sd["filled"],
                })

        # rank by slippage
        by_slip = sorted(exchange_data, key=lambda x: x["slippageBps"])
        rankings["bySlippage"][size_key] = [
            {
                "rank": i + 1,
                "exchange": item["exchange"],
                "slippageBps": item["slippageBps"],
                "filled": item["filled"],
            }
            for i, item in enumerate(by_slip)
        ]

        # rank by total cost
        by_total = sorted(exchange_data, key=lambda x: x["totalCostBps"])
        rankings["byTotalCost"][size_key] = [
            {
                "rank": i + 1,
                "exchange": item["exchange"],
                "slippageBps": item["slippageBps"],
                "takerFeeBps": item["takerFeeBps"],
                "totalCostBps": item["totalCostBps"],
                "filled": item["filled"],
            }
            for i, item in enumerate(by_total)
        ]

    return rankings

# endregion


TAKER_FEES_BPS = dict(sorted(TAKER_FEES_BPS.items()))
ORDERBOOK_FETCHERS = dict(sorted(ORDERBOOK_FETCHERS.items()))
df_taker_fees_bps = pd.DataFrame(
    [{ "Exchange": k, "Taker Fee (bps)": v * 100 } for k, v in TAKER_FEES_BPS.items()]
)
st.subheader("Base Taker Fees")
st.dataframe(df_taker_fees_bps, hide_index=True)

placeholder = st.empty()

exchanges = list(TAKER_FEES_BPS.keys())

# region organise data
def get_all_analysis(token: str):
    analysis_list = []
    for name, fetcher in ORDERBOOK_FETCHERS.items():
        try:
            orderbook = fetcher(token)  # TODO convert to async?
            analysis = analyze_orderbook(orderbook)
            analysis_list.append(analysis)
        except Exception as e:
            analysis_list.append({"exchange": name, "error": str(e)})
    return analysis_list

def reorganize_by_clip_size(analysis_list):
    # collect all available size keys (e.g., '$1.0k', '$10.0k')
    size_keys = set()
    for analysis in analysis_list:
        if "slippage" in analysis:
            size_keys.update(analysis["slippage"].keys())

    result = {}

    for size in size_keys:
        rows = []
        for analysis in analysis_list:
            slip = analysis.get("slippage", {}).get(size)
            if slip:
                rows.append({
                    "exchange": analysis["exchange"],
                    "avgBps": slip.get("avgBps"),
                    "takerFeeBps": slip.get("takerFeeBps"),
                    "totalCostBps": slip.get("totalCostBps")
                })

        # create df for this clip size
        df = pd.DataFrame(rows)
        # sort by smallest total cost first
        df = df.sort_values("totalCostBps").reset_index(drop=True)

        result[size] = df

    return result
# endregion

# fixed list of tokens for user selection
TOKEN_OPTIONS = ["BTC", "ETH", "SOL", "XRP", "HYPE", "BNB"]
token = st.selectbox("Select Token", TOKEN_OPTIONS, index=0)

with st.spinner(show_time=True):
    analysis_list = get_all_analysis(token.lower())
    tables = reorganize_by_clip_size(analysis_list)

    st.subheader(f"Orderbook snapshot for {token.upper()}")
    for clip_size, clip_size_data in sorted(tables.items()):
        st.text(f'Clip Size: {clip_size}')
        st.dataframe(
            clip_size_data,
            column_config={
                'exchange': st.column_config.TextColumn('Exchange', width='small'),
                'avgBps': st.column_config.NumberColumn('Avg Bps', width='small'),
                'takerFeeBps': st.column_config.NumberColumn('Taker Fee (Bps)', width='small'),
                'totalCostBps': st.column_config.NumberColumn('Total Cost (Bps)', width='small'),
            },
            hide_index=True,
        )

# TODO convert to async
# TODO add auto refresh every 5min? put last updated datetime
# TODO check actual pricefor calculation if correct