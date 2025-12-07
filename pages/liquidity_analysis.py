import asyncio
from datetime import datetime
from typing import Any
import aiohttp
import pandas as pd
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

TAKER_FEES_BPS = pd.DataFrame(
    [
        { "Exchange": "Hyperliquid", "Taker Fee (Bps)": 4, "Assumption": ">5M 14D volume, 0 HYPE staked" },
        { "Exchange": "Extended", "Taker Fee (Bps)": 2.5, "Assumption": "" },
        { "Exchange": "Lighter", "Taker Fee (Bps)": 2, "Assumption": "Premium Account" },
        { "Exchange": "Paradex", "Taker Fee (Bps)": 0, "Assumption": "" },
        { "Exchange": "Pacifica", "Taker Fee (Bps)": 4, "Assumption": "" },
    ]
)
TAKER_FEES_BPS = TAKER_FEES_BPS.sort_values('Exchange')


async def _fetch_json(
    session: aiohttp.ClientSession,
    method: str,
    url: str,
    *,
    json_body: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = 15,
) -> Any:
    """generic helper to fetch JSON via aiohttp"""
    kwargs: dict[str, Any] = {"headers": headers, "timeout": aiohttp.ClientTimeout(total=timeout)}
    if json_body is not None:
        kwargs["json"] = json_body

    async with session.request(method.upper(), url, **kwargs) as resp:
        if resp.status >= 400:
            text = await resp.text()
            raise RuntimeError(f"HTTP {resp.status} for {url}: {text}")
        return await resp.json()


# region l2 orderbook fetchers
async def fetch_hyperliquid_orderbook(session: aiohttp.ClientSession, token: str):
    data = await _fetch_json(
        session,
        "POST",
        "https://api.hyperliquid.xyz/info",
        json_body={"type": "l2Book", "coin": token.upper()},
        timeout=15,
    )

    if not data.get("levels") or len(data["levels"]) < 2:
        raise ValueError(f"Invalid orderbook for {token} on Hyperliquid")

    bids = [{"price": float(l["px"]), "qty": float(l["sz"])} for l in data["levels"][0]]
    asks = [{"price": float(l["px"]), "qty": float(l["sz"])} for l in data["levels"][1]]

    return {"bids": bids, "asks": asks, "exchange": "Hyperliquid", "token": token}


async def fetch_paradex_orderbook(session: aiohttp.ClientSession, token: str):
    pair = f"{token.upper()}-USD-PERP"
    data = await _fetch_json(
        session,
        "GET",
        f"https://api.prod.Paradex.trade/v1/orderbook/{pair}/interactive?depth=100",
        headers={"Accept": "application/json"},
        timeout=10,
    )

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


async def fetch_extended_orderbook(session: aiohttp.ClientSession, token: str):
    market = f"{token.upper()}-USD"
    data = await _fetch_json(
        session,
        "GET",
        f"https://api.starknet.Extended.exchange/api/v1/info/markets/{market}/orderbook",
        timeout=10,
    )

    if data.get("status") != "OK" or "data" not in data:
        raise ValueError(f"Invalid orderbook for {token} on Extended")

    ob = data["data"]
    bids = [{"price": float(l["price"]), "qty": float(l["qty"])} for l in ob["bid"]]
    asks = [{"price": float(l["price"]), "qty": float(l["qty"])} for l in ob["ask"]]
    return {"bids": bids, "asks": asks, "exchange": "Extended", "token": token}


async def fetch_lighter_orderbook(session: aiohttp.ClientSession, token: str):
    d = await _fetch_json(
        session,
        "GET",
        "https://mainnet.zklighter.elliot.ai/api/v1/orderBookDetails",
        timeout=10,
    )

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
    data = await _fetch_json(
        session,
        "GET",
        f"https://mainnet.zklighter.elliot.ai/api/v1/orderBookOrders?market_id={mid}&limit=50",
        timeout=10,
    )

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


async def fetch_pacifica_orderbook(session: aiohttp.ClientSession, token: str):
    symbol = token.upper()
    data = await _fetch_json(
        session,
        "GET",
        f"https://api.Pacifica.fi/api/v1/book?symbol={symbol}",
        timeout=10,
    )

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
ORDERBOOK_FETCHERS = dict(sorted(ORDERBOOK_FETCHERS.items()))

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
    worst_price = best_price

    # full pricefor
    for level in levels:
        price = level["price"]
        qty_available = level["qty"]
        value_at_level = qty_available * price

        worst_price = price

        if remaining_usd <= value_at_level:
            qty_taken = remaining_usd / price
            total_qty += qty_taken
            total_cost += remaining_usd
            remaining_usd = 0
            break
        else:
            total_qty += qty_available
            total_cost += value_at_level
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
    }


def analyze_orderbook(orderbook):
    if not orderbook["bids"] or not orderbook["asks"]:
        return {"exchange": orderbook["exchange"], "error": "Empty orderbook"}

    best_bid = max(b["price"] for b in orderbook["bids"])
    best_ask = min(a["price"] for a in orderbook["asks"])
    mid_price = (best_bid + best_ask) / 2
    spread = ((best_ask - best_bid) / best_bid) * 100

    try:
        taker_fee_bps = TAKER_FEES_BPS.loc[TAKER_FEES_BPS['Exchange'] == orderbook['exchange']].iloc[0]['Taker Fee (Bps)']
    except Exception as e:
        logger.exception(e)
        taker_fee_bps = 0

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
        bid_slippage = calculate_slippage(orderbook, size, "buy")
        ask_slippage = calculate_slippage(orderbook, size, "sell")

        # avg slippage
        if bid_slippage["slippage"] is not None and ask_slippage["slippage"] is not None:
            avg_slippage = (bid_slippage["slippage"] + ask_slippage["slippage"]) / 2
        elif not bid_slippage["slippage"]:
            logger.error(f'Unable to calculate slippage for {orderbook['exchange']} for buy side for size {size}')
        else:
            logger.error(f'Unable to calculate slippage for {orderbook['exchange']} for ask side for size {size}')

        # avg effective spread
        if (
            "effectiveSpreadBps" in bid_slippage
            and "effectiveSpreadBps" in ask_slippage
        ):
            avg_eff_spread = (
                bid_slippage["effectiveSpreadBps"] + ask_slippage["effectiveSpreadBps"]
            ) / 2
        else:
            avg_eff_spread = (
                bid_slippage.get("effectiveSpreadBps")
                or ask_slippage.get("effectiveSpreadBps")
                or 0
            )

        slippage_bps = round(avg_slippage * 100, 2) if avg_slippage is not None else None
        total_cost_bps = (
            round(slippage_bps + taker_fee_bps, 2) if slippage_bps is not None else None
        )

        result["slippage"][size] = {
            "slippageBps": slippage_bps,
            "takerFeeBps": taker_fee_bps,
            "totalCostBps": total_cost_bps,
            "effectiveSpreadBps": round(avg_eff_spread, 2),
            "filled": bid_slippage["filled"] and ask_slippage["filled"],
            "levels": {
                "buy": bid_slippage.get("levelsUsed", 0),
                "sell": ask_slippage.get("levelsUsed", 0),
            },
        }

    return result
# endregion


# region organise data
async def _run_single_analysis(
    session: aiohttp.ClientSession,
    name: str,
    fetcher,
    token: str,
) -> dict[str, Any]:
    try:
        orderbook = await fetcher(session, token)
        analysis = analyze_orderbook(orderbook)
        return analysis
    except Exception as e:
        logger.warning("Error fetching/analyzing %s for %s: %s", name, token, e)
        return {"exchange": name, "error": str(e)}

async def get_all_analysis(token: str) -> list[dict[str, Any]]:
    analysis_list: list[dict[str, Any]] = []
    async with aiohttp.ClientSession() as session:
        tasks = []
        for name, fetcher in ORDERBOOK_FETCHERS.items():
            tasks.append(_run_single_analysis(session, name, fetcher, token))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        analysis_list = results
    return analysis_list

def reorganize_by_clip_size(analysis_list: list):
    size_keys = set()
    for analysis in analysis_list:
        if "slippage" in analysis:
            size_keys.update(analysis["slippage"].keys())

    result = {}

    for size in size_keys:
        rows = []
        for analysis in analysis_list:
            slippage = analysis.get("slippage", {}).get(size)
            if not slippage:
                continue

            filled = slippage.get("filled", True)

            # if not fully filled, set infinity
            if not filled:
                slippage_bps = float("inf")
                total_cost_bps = float("inf")
                note = "* insufficient liquidity"
            else:
                slippage_bps = slippage.get("slippageBps")
                total_cost_bps = slippage.get("totalCostBps")
                note = ""

            rows.append({
                "exchange": analysis["exchange"],
                "slippageBps": slippage_bps,
                "takerFeeBps": slippage.get("takerFeeBps"),
                "totalCostBps": total_cost_bps,
                "note": note,
            })

        df = pd.DataFrame(rows)

        # sort: finite values first, then inf
        df = df.sort_values(
            by="totalCostBps",
            key=lambda col: col.replace({float("inf"): 1e18}),
        ).reset_index(drop=True)

        result[size] = df

    return result
# endregion

# fixed list of tokens for user selection
TOKEN_OPTIONS = ["BTC", "ETH", "SOL", "XRP", "HYPE", "BNB"]
token = st.selectbox("Select Token", TOKEN_OPTIONS, index=0)

with st.spinner("Fetching orderbooks and computing slippage..."):
    # run async workflow once per rerun
    analysis_list = asyncio.run(get_all_analysis(token.lower()))
    tables = reorganize_by_clip_size(analysis_list)

st.text("Assumptions")
st.markdown("""
- Full clip size can be taken against existing orderbook (this is unlikely in actual execution as market makers can react to taker orders and adjust / cancel as they deem fit)
- Slippage is calculated based on average of buy side / ask side book
- Taker fees as follows:
""")
st.dataframe(
    TAKER_FEES_BPS,
    column_order=('Exchange', 'Taker Fee (Bps)', 'Assumption'),
    hide_index=True
)

st.subheader("Slippage Rankings")
st.caption(datetime.now().strftime("Last updated: %Y-%m-%d %H:%M:%S"))

MEDALS = {1: "🥇", 2: "🥈", 3: "🥉"}
def build_rankings_table(df: pd.DataFrame) -> pd.DataFrame:
    df['full_remarks'] = df.apply(
        lambda r: f"{r['totalCostBps']:.2f} bps (slippage {r['slippageBps']:.2f} + taker fee {r['takerFeeBps']:.2f})",
        axis=1,
    )
    # sort by total cost bps (lower is better)
    ranked = df.sort_values("totalCostBps", ascending=True).reset_index(drop=True)
    ranked.insert(0, "Rank", ranked.index + 1)
    ranked["Medal"] = ranked["Rank"].map(MEDALS).fillna("")
    cols = ["Medal", "Rank", "exchange", "full_remarks"]
    return ranked[cols]

def render_rankings_text(df):
    lines = []
    for _, row in df.iterrows():
        medal = row["Medal"] or "  "    # TODO fix formatting
        line = (
            f"{medal} "
            f"#{int(row['Rank']):<2} "
            f"{row['exchange']:<15} "
            f"{row['full_remarks']}"
        )
        lines.append(line)
    text = "\n".join(lines)

    # code block: monospaced, no borders, left aligned
    st.code(text, language=None)

for clip_size, clip_size_data in sorted(tables.items()):
    st.markdown(f"**${clip_size/1000}k**")

    rankings_df = build_rankings_table(clip_size_data)
    render_rankings_text(rankings_df)

with st.expander("Detailed Breakdown", expanded=False):
    for clip_size, clip_size_data in sorted(tables.items()):
        clip_size_formatted = f"${clip_size/1000}k"
        st.markdown(f'Clip Size: **{clip_size_formatted}**')
        st.dataframe(
            clip_size_data[["exchange", "slippageBps", "takerFeeBps", "totalCostBps", "note"]],
            column_config={
                'exchange': st.column_config.TextColumn('Exchange', width='small'),
                'slippageBps': st.column_config.NumberColumn('Slippage (Bps)', width='small'),
                'takerFeeBps': st.column_config.NumberColumn('Taker Fee (Bps)', width='small'),
                'totalCostBps': st.column_config.NumberColumn('Total Cost (Bps)', width='small'),
                'note': st.column_config.TextColumn('Note', width='small'),
            },
            hide_index=True,
        )