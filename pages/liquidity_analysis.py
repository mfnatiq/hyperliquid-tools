import asyncio
from datetime import datetime
import html
import sys
from typing import Any
import aiohttp
import pandas as pd
import streamlit as st

# css to tighten vertical space for selectbox labels that are collapsed
st.markdown(
    """
    <style>
    div[data-testid="stSelectbox"] > label {
        min-height: 0 !important;
        margin-bottom: 0 !important;
        padding-bottom: 0 !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

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
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

CLIP_SIZES = [1_000, 10_000, 50_000, 100_000, 500_000]



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

    # gateways (e.g. Lighter's) intermittently return 502/503/504; retry those briefly
    for attempt in range(3):
        async with session.request(method.upper(), url, **kwargs) as resp:
            if resp.status in (502, 503, 504) and attempt < 2:
                await asyncio.sleep(0.5 * (attempt + 1))
                continue
            if resp.status >= 400:
                text = (await resp.text())[:200]
                raise RuntimeError(f"HTTP {resp.status} for {url}: {text}")
            return await resp.json()


# tokens by asset class, alphabetical within each; tradfi names are the ones most listed across these venues
TOKEN_GROUPS = {
    "Indices": ["QQQ", "US500"],
    "Stocks": ["AAPL", "COIN", "GOOGL", "HOOD", "META", "MSTR", "NVDA", "TSLA"],
    "Commodities": ["GOLD", "OIL", "SILVER"],
    "Crypto": ["BNB", "BTC", "ETH", "HYPE", "SOL", "XRP"],
}
CRYPTO_TOKENS = TOKEN_GROUPS["Crypto"]
DEFAULT_GROUP, DEFAULT_TOKEN = "Indices", "US500"
SYMBOL_ALIASES = {
    "US500": ["US500", "SPX", "SP500", "SPY", "USA500"],
    "QQQ": ["QQQ", "US100", "NDX", "NAS100", "XYZ100"],
    "GOLD": ["GOLD", "XAU"],
    "SILVER": ["SILVER", "XAG"],
    "OIL": ["OIL", "CL", "WTI", "USOIL"],
}
# Hyperliquid lists tradfi perps on the trade.xyz HIP-3 dex, as "xyz:<SYMBOL>"
HYPERLIQUID_HIP3_DEX = "xyz"


class NotListed(ValueError):
    """the venue has no market for this symbol; expected for most venue / token pairs, so not logged as a warning"""


def _is_not_listed(e: Exception) -> bool:
    # venues answer an unknown symbol with a 400 / 404 (e.g. Pacifica "Book not found: QQQ")
    return isinstance(e, NotListed) or (isinstance(e, RuntimeError) and str(e).startswith(("HTTP 400", "HTTP 404")))


# region l2 orderbook fetchers
async def fetch_hyperliquid_orderbook(session: aiohttp.ClientSession, token: str, *, aggregated: bool=False):
    coin = token.upper() if token.upper() in CRYPTO_TOKENS else f"{HYPERLIQUID_HIP3_DEX}:{token.upper()}"
    body: dict[str, Any] = {"type": "l2Book", "coin": coin}
    if aggregated:  # special case for hyperliquid as l2 book only returns 20 levels, which may be insufficient for larger clip sizes
        # coarser buckets to get larger notional per level
        body.update({"nSigFigs": 4})

    data = await _fetch_json(
        session,
        "POST",
        "https://api.hyperliquid.xyz/info",
        json_body=body,
    )

    if not data or not data.get("levels") or len(data["levels"]) < 2:
        raise NotListed(f"Token {token} not available on Hyperliquid")

    bids = [{"price": float(l["px"]), "qty": float(l["sz"])} for l in data["levels"][0]]
    asks = [{"price": float(l["px"]), "qty": float(l["sz"])} for l in data["levels"][1]]

    return {"bids": bids, "asks": asks, "exchange": "Hyperliquid", "token": token, "aggregated": aggregated, "venueSymbol": coin}
async def fetch_hyperliquid_orderbook_default(session, token):
    return await fetch_hyperliquid_orderbook(session, token, aggregated=False)


async def fetch_extended_orderbook(session: aiohttp.ClientSession, token: str):
    # stock markets are named e.g. NVDA_24_5-USD (24/5 trading); other tradfi markets may not carry the suffix
    markets = [f"{token.upper()}-USD"] if token.upper() in CRYPTO_TOKENS else [f"{token.upper()}_24_5-USD", f"{token.upper()}-USD"]
    data: dict[str, Any] = {}
    for market in markets:
        try:
            data = await _fetch_json(
                session,
                "GET",
                f"https://api.starknet.extended.exchange/api/v1/info/markets/{market}/orderbook",
            )
        except RuntimeError:
            continue
        if data.get("status") == "OK" and (data.get("data") or {}).get("bid") and data["data"].get("ask"):
            break

    if data.get("status") != "OK" or not (data.get("data") or {}).get("bid") or not data["data"].get("ask"):
        raise NotListed(f"Token {token} not available on Extended")

    ob = data["data"]
    bids = [{"price": float(l["price"]), "qty": float(l["qty"])} for l in ob["bid"]]
    asks = [{"price": float(l["price"]), "qty": float(l["qty"])} for l in ob["ask"]]

    # RFQ markets (isRfq) only publish an indicative book, not one you can take against
    note = ""
    try:
        info = await _fetch_json(
            session,
            "GET",
            f"https://api.starknet.extended.exchange/api/v1/info/markets?market={market}",
        )
        m: dict[str, Any] = next((x for x in info.get("data", []) if x.get("name") == market), {})
        if m.get("isRfq"):
            note = "RFQ market: indicative book only"
        elif m.get("isOffHours"):
            note = "market closed (off-hours)"
    except Exception as e:
        logger.warning("Extended market info for %s failed: %s", market, e)
    return {"bids": bids, "asks": asks, "exchange": "Extended", "token": token, "note": note, "venueSymbol": market}


LIGHTER_URL = "https://mainnet.zklighter.elliot.ai"
# Lighter's Robinhood Chain deployment (apidocs.rh.lighter.xyz); assumed to serve the same REST API as mainnet
LIGHTER_RH_URL = "https://api.rh.lighter.xyz"


async def fetch_lighter_orderbook(session: aiohttp.ClientSession, token: str, *, base_url: str=LIGHTER_URL, exchange: str="Lighter"):
    d = await _fetch_json(
        session,
        "GET",
        f"{base_url}/api/v1/orderBookDetails",
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
        raise NotListed(f"Token {token} not available on {exchange}")

    mid = market["market_id"]
    data = await _fetch_json(
        session,
        "GET",
        f"{base_url}/api/v1/orderBookOrders?market_id={mid}&limit=250",
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

    return {"bids": bids, "asks": asks, "exchange": exchange, "token": token, "venueSymbol": market["symbol"]}


async def fetch_lighter_rh_orderbook(session: aiohttp.ClientSession, token: str):
    return await fetch_lighter_orderbook(session, token, base_url=LIGHTER_RH_URL, exchange="Lighter on Robinhood")


def _levels(raw: list | None) -> list[dict[str, float]]:
    """[price, qty] pairs or {price, quantity|qty|size} dicts -> [{price, qty}]"""
    out = []
    for l in raw or []:
        if isinstance(l, dict):
            px, qty = l.get("price") or l.get("px"), l.get("quantity") or l.get("qty") or l.get("size") or l.get("sz")
        else:
            px, qty = l[0], l[1]
        if px is not None and qty is not None and float(qty) > 0:
            out.append({"price": float(px), "qty": float(qty)})
    return out


def _base_symbol(sym: str) -> str:
    """BTC-PERP / BTC-USD / BTCUSD / BTC-USDC -> BTC"""
    s = str(sym).upper().split("/")[0]
    for suffix in ("-PERP", "_PERP", "PERP", "-USDC", "-USDT", "-USD", "USDC", "USDT", "USD"):
        if s.endswith(suffix) and len(s) > len(suffix):
            s = s[: -len(suffix)]
    return s.split("-")[0]


async def fetch_risex_orderbook(session: aiohttp.ClientSession, token: str):
    # RISEx public REST, same calls as trading-tools funding_scan.Rise
    d = await _fetch_json(session, "GET", "https://api.rise.trade/v1/markets")
    d = d.get("data", d)
    market = next(
        (
            m
            for m in d.get("markets", [])
            if _base_symbol(m.get("display_base_asset_symbol") or m.get("base_asset_symbol") or (m.get("config") or {}).get("name", "")) == token.upper()
            and m.get("active") is not False
        ),
        None,
    )
    if not market:
        raise NotListed(f"Token {token} not available on RISEx")

    ob = await _fetch_json(session, "GET", f"https://api.rise.trade/v1/orderbook?market_id={market['market_id']}&limit=250")
    ob = ob.get("data", ob)
    return {"bids": _levels(ob.get("bids")), "asks": _levels(ob.get("asks")), "exchange": "RISEx", "token": token,
            "venueSymbol": market.get("display_base_asset_symbol") or market.get("base_asset_symbol") or (market.get("config") or {}).get("name")}


async def fetch_phoenix_orderbook(session: aiohttp.ClientSession, token: str):
    # Phoenix perps public REST, same calls as trading-tools funding_scan.Phoenix
    url = "https://perp-api.phoenix.trade"
    markets = await _fetch_json(session, "GET", f"{url}/v1/view/exchange/markets")
    markets = markets.get("data", markets) if isinstance(markets, dict) else markets
    market = next(
        (m for m in markets if _base_symbol(m.get("symbol", "")) == token.upper() and m.get("marketStatus") == "active"),
        None,
    )
    if not market:
        raise NotListed(f"Token {token} not available on Phoenix")

    # the book path takes either the base ticker or the full market symbol
    last_err: Exception | None = None
    for sym in dict.fromkeys([token.upper(), market["symbol"]]):
        try:
            b = await _fetch_json(session, "GET", f"{url}/v1/view/orderbook/{sym}?include_splines=true")
            return {"bids": _levels(b.get("bids")), "asks": _levels(b.get("asks")), "exchange": "Phoenix", "token": token, "venueSymbol": sym}
        except RuntimeError as e:
            last_err = e
    raise last_err or ValueError(f"No orderbook for {token} on Phoenix")


async def fetch_qfex_orderbook(session: aiohttp.ClientSession, token: str):
    # QFEX public REST, same calls as trading-tools funding_scan.QFEX
    url = "https://api.qfex.com"
    headers = {"Accept": "application/json"}
    d = await _fetch_json(session, "GET", f"{url}/md/contracts", headers=headers)
    contracts = d.get("data", d) if isinstance(d, dict) else d
    ticker_id = next(
        (c["ticker_id"] for c in contracts or [] if c.get("ticker_id") and _base_symbol(c["ticker_id"]) == token.upper()),
        None,
    )
    if not ticker_id:
        raise NotListed(f"Token {token} not available on QFEX")

    ob = await _fetch_json(session, "GET", f"{url}/md/orderbook/{ticker_id}", headers=headers)
    ob = ob.get("data", ob) if isinstance(ob, dict) else {}
    return {"bids": _levels(ob.get("bids")), "asks": _levels(ob.get("asks")), "exchange": "QFEX", "token": token, "venueSymbol": ticker_id}


async def fetch_pacifica_orderbook(session: aiohttp.ClientSession, token: str):
    symbol = token.upper()
    data = await _fetch_json(
        session,
        "GET",
        f"https://api.pacifica.fi/api/v1/book?symbol={symbol}&agg_level=100",
        timeout=10,
    )

    if not data.get("success") or not data.get("data") or not data["data"].get("l"):
        raise ValueError(f"Invalid orderbook for {token} on Pacifica")

    orderbook = data["data"]
    bids_raw = orderbook["l"][0] if orderbook["l"] and len(orderbook["l"]) > 0 else []
    asks_raw = orderbook["l"][1] if orderbook["l"] and len(orderbook["l"]) > 1 else []

    if not bids_raw or not asks_raw:
        raise NotListed(f"Empty orderbook for {token} on Pacifica")

    bids = [
        {"price": float(level["p"]), "qty": float(level["a"])}
        for level in bids_raw
    ]
    asks = [
        {"price": float(level["p"]), "qty": float(level["a"])}
        for level in asks_raw
    ]

    return {"bids": bids, "asks": asks, "exchange": "Pacifica", "token": token, "venueSymbol": symbol}


async def fetch_nado_orderbook(session: aiohttp.ClientSession, token: str):
    market = f"{token.upper()}-PERP"
    data = await _fetch_json(
        session,
        "GET",
        "https://gateway.prod.nado.xyz/v1/query?type=symbols&product_type=perp",
    )

    product_id = (
        data.get("data", {})
            .get("symbols", {})
            .get(market, {})
            .get("product_id")
    )

    if product_id is None:
        raise NotListed(f"Token {token} not available on Nado")

    orderbook_data = await _fetch_json(
        session,
        "GET",
        f"https://gateway.prod.nado.xyz/v1/query?type=market_liquidity&product_id={product_id}&depth=100",
    )

    ob = orderbook_data["data"]
    fixed_point_ratio = 1e18
    bids = [{"price": float(l[0]) / fixed_point_ratio, "qty": float(l[1]) / fixed_point_ratio} for l in ob["bids"]]
    asks = [{"price": float(l[0]) / fixed_point_ratio, "qty": float(l[1]) / fixed_point_ratio} for l in ob["asks"]]
    return {"bids": bids, "asks": asks, "exchange": "Nado", "token": token, "venueSymbol": market}


async def fetch_variational_stats(session: aiohttp.ClientSession) -> dict[str, Any]:
    VARIATIONAL_BASE_URL = "https://omni-client-api.prod.ap-northeast-1.variational.io"
    data = await _fetch_json(
        session,
        "GET",
        f"{VARIATIONAL_BASE_URL}/metadata/stats",
    )
    # response shape: { "total_volume_24h": "...", "listings": [ {...}, ... ] }
    listings = data.get("listings", [])
    # index by ticker upper for convenience
    return {str(l["ticker"]).upper(): l for l in listings}

async def fetch_variational_orderbook(session: aiohttp.ClientSession, token: str):
    stats_by_ticker = await fetch_variational_stats(session)
    listing = stats_by_ticker.get(token.upper())
    if not listing:
        raise NotListed(f"Token {token} not available on Variational")

    # mark_price and base_spread_bps are strings; convert as needed
    mark_price = float(listing["mark_price"])
    base_spread_bps = float(listing.get("base_spread_bps", "0") or 0.0)

    quotes = listing.get("quotes", {})

    # custom analyze function to compute slippage per CLIP_SIZES from quotes
    return {
        "exchange": "Variational",
        "token": token,
        "venueSymbol": listing.get("ticker"),
        "markPrice": mark_price,
        "baseSpreadBps": base_spread_bps,
        "quotes": quotes,
        # dummy fields so existing checks don't blow up
        "bids": [],
        "asks": [],
    }

# endregion


ORDERBOOK_FETCHERS = {
    "Hyperliquid": fetch_hyperliquid_orderbook_default,
    "Extended": fetch_extended_orderbook,
    "Lighter": fetch_lighter_orderbook,
    "Lighter on Robinhood": fetch_lighter_rh_orderbook,
    "RISEx": fetch_risex_orderbook,
    "Phoenix": fetch_phoenix_orderbook,
    "QFEX": fetch_qfex_orderbook,
    "Pacifica": fetch_pacifica_orderbook,
    "Nado": fetch_nado_orderbook,
    "Variational": fetch_variational_orderbook,
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


def analyze_orderbook_pure(orderbook: dict, agg_orderbook: dict | None = None):
    """
    analyse orderbook slippage only, excluding any fee tiers
    """
    exchange = orderbook["exchange"]

    if not orderbook["bids"] or not orderbook["asks"]:
        return {"exchange": exchange, "error": "Empty orderbook"}

    best_bid = max(b["price"] for b in orderbook["bids"])
    best_ask = min(a["price"] for a in orderbook["asks"])
    mid_price = (best_bid + best_ask) / 2
    spread = ((best_ask - best_bid) / best_bid) * 100

    result = {
        "exchange": exchange,
        "token": orderbook["token"],
        "venueSymbol": orderbook.get("venueSymbol"),
        "midPrice": round(mid_price, 2),
        "spreadBps": round(spread * 100, 2),
        "slippage": {},
    }

    for size in CLIP_SIZES:
        book_to_use = orderbook
        is_approximation = False
        if agg_orderbook is not None and book_depth_usd(orderbook) < size:
            logger.debug("using agg orderbook for %s for clip size %s", exchange, size)
            book_to_use = agg_orderbook
            is_approximation = True

        bid_slippage = calculate_slippage(book_to_use, size, "buy")
        ask_slippage = calculate_slippage(book_to_use, size, "sell")

        if bid_slippage["slippage"] is not None and ask_slippage["slippage"] is not None:
            avg_slippage = (bid_slippage["slippage"] + ask_slippage["slippage"]) / 2
        else:
            avg_slippage = None

        if (
            "effectiveSpreadBps" in bid_slippage
            and "effectiveSpreadBps" in ask_slippage
        ):
            avg_eff_spread = (
                bid_slippage["effectiveSpreadBps"]
                + ask_slippage["effectiveSpreadBps"]
            ) / 2
        else:
            avg_eff_spread = (
                bid_slippage.get("effectiveSpreadBps")
                or ask_slippage.get("effectiveSpreadBps")
                or 0
            )

        slippage_bps = round(avg_slippage * 100, 3) if avg_slippage is not None else None

        result["slippage"][size] = {
            "slippageBps": slippage_bps,
            "effectiveSpreadBps": round(avg_eff_spread, 3),
            "filled": bid_slippage["filled"] and ask_slippage["filled"],
            "levels": {
                "buy": bid_slippage.get("levelsUsed", 0),
                "sell": ask_slippage.get("levelsUsed", 0),
            },
            "is_approximation": is_approximation or bool(orderbook.get("note")),
            "approximation_reason": orderbook.get("note") or None,
        }

    return result

# endregion


# compute slippage equivalent from variational api
def _variational_slippage_from_quotes(listing: dict, size_usd: int) -> dict[str, Any]:
    mark_price = float(listing["markPrice"])
    quotes = listing.get("quotes", {})

    # exact mapping only
    size_field_map = {
        1_000: "size_1k",
        100_000: "size_100k",
        # you can also include 1_000_000: "size_1m" if you add 1m to CLIP_SIZES
    }
    size_field = size_field_map.get(size_usd)

    # if we don't have an exact mapping, treat as unavailable
    if size_field is None:
        return {
            "slippageBps": None,
            "effectiveSpreadBps": None,
            "filled": False,
            "filledPercent": 0.0,
            "is_approximation": True,
            "approximation_reason": "no RFQ quote at this size (Variational quotes $1k and $100k only)",
        }

    quote = quotes.get(size_field)
    if not quote:
        return {
            "slippageBps": None,
            "effectiveSpreadBps": None,
            "filled": False,
            "filledPercent": 0.0,
            "is_approximation": True,
            "approximation_reason": f"missing RFQ for {size_field}",
        }

    bid = float(quote["bid"])
    ask = float(quote["ask"])

    buy_slip_pct = (ask - mark_price) / mark_price * 100
    sell_slip_pct = (mark_price - bid) / mark_price * 100
    avg_slip_pct = (buy_slip_pct + sell_slip_pct) / 2

    eff_spread_bps = (ask - bid) / ((ask + bid) / 2) * 10_000

    return {
        "slippageBps": round(avg_slip_pct * 100, 3),
        "effectiveSpreadBps": round(eff_spread_bps, 3),
        "filled": True,
        "filledPercent": 100.0,
        "is_approximation": True,  # using RFQ quotes
        "approximation_reason": "using RFQ",
    }



# region organise data
def book_depth_usd(orderbook: dict) -> float:
    # conservative: use the shallower side
    bid_depth = sum(b["price"] * b["qty"] for b in orderbook["bids"])
    ask_depth = sum(a["price"] * a["qty"] for a in orderbook["asks"])
    return min(bid_depth, ask_depth)

async def _run_single_analysis(
    session: aiohttp.ClientSession,
    name: str,
    fetcher,
    token: str,
) -> dict[str, Any]:
    try:
        ob_or_stats = None
        errors: dict[str, Exception] = {}
        for symbol in SYMBOL_ALIASES.get(token.upper(), [token]):
            try:
                ob = await fetcher(session, symbol)
            except Exception as e:
                errors[symbol] = e
                continue
            # an empty book usually means the venue knows the name but has no such market; try the next alias
            if name != "Variational" and (not ob.get("bids") or not ob.get("asks")):
                errors[symbol] = NotListed(f"Empty orderbook for {symbol} on {name}")
                continue
            ob_or_stats = ob
            break
        if ob_or_stats is None:
            tried = "; ".join(f"{sym}: {err}" for sym, err in errors.items())
            if all(_is_not_listed(err) for err in errors.values()):
                logger.debug("%s does not list %s (tried %s)", name, token, tried)
                return {"exchange": name, "error": f"not listed on {name}"}
            raise RuntimeError(f"tried {tried}")

        if name == "Variational":
            # build a result similar to analyze_orderbook_pure, but using quotes
            listing_info = {
                "exchange": ob_or_stats["exchange"],
                "token": ob_or_stats["token"],
                "markPrice": ob_or_stats["markPrice"],
            }

            result = {
                "exchange": "Variational",
                "token": token,
                "venueSymbol": ob_or_stats.get("venueSymbol"),
                "midPrice": round(ob_or_stats["markPrice"], 2),
                "spreadBps": ob_or_stats.get("baseSpreadBps"),  # already in bps
                "slippage": {},
            }
            for size in CLIP_SIZES:
                slip = _variational_slippage_from_quotes(ob_or_stats, size)
                result["slippage"][size] = slip
            return result

        # existing path for real orderbooks
        orderbook = ob_or_stats

        # hyperliquid-specific: if depth is insufficient for largest clip, fetch with aggregation of levels
        agg_orderbook = None
        if name == "Hyperliquid":
            depth = book_depth_usd(orderbook)
            max_clip = max(CLIP_SIZES)
            if depth < max_clip:
                # fetch with aggregated book to be used separately
                agg_orderbook = await fetch_hyperliquid_orderbook(
                    session, orderbook["token"], aggregated=True
                )

        analysis = analyze_orderbook_pure(orderbook, agg_orderbook=agg_orderbook)
        return analysis
    except Exception as e:
        logger.warning("Error fetching/analyzing %s for %s: %s", name, token, e)
        return {"exchange": name, "error": str(e)}

async def _get_all_analysis_pure(token: str) -> list[dict[str, Any]]:
    analysis_list: list[dict[str, Any]] = []
    async with aiohttp.ClientSession() as session:
        tasks = []
        for name, fetcher in ORDERBOOK_FETCHERS.items():
            tasks.append(_run_single_analysis(session, name, fetcher, token))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        analysis_list = [r for r in results if isinstance(r, dict)]
    return analysis_list

@st.cache_data(ttl=60)  # cache data
def get_all_analysis_pure_cached(token: str) -> list[dict[str, Any]]:
    return asyncio.run(_get_all_analysis_pure(token))

# notes that only say "this is RFQ"; the (RFQ) tag on the name replaces them
RFQ_ONLY_NOTES = {"using RFQ", "RFQ market: indicative book only"}

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
                # e.g. variational has no RFQ quote at this size; otherwise the book ran out
                note = slippage.get("approximation_reason") or "insufficient liquidity"
            else:
                slippage_bps = slippage.get("slippageBps")
                total_cost_bps = slippage.get("totalCostBps")
                is_approximation = slippage.get("is_approximation", False)
                approximation_reason = slippage.get("approximation_reason", None)
                note = ""
                if is_approximation:
                    note = approximation_reason or "slippage approximated due to aggregated levels (API limitation)"

            # RFQ venues / markets are tagged on the name, e.g. "Variational (RFQ)", rather than in the notes
            exchange = analysis["exchange"]
            if "RFQ" in (slippage.get("approximation_reason") or ""):
                exchange = f"{exchange} (RFQ)"
                if note in RFQ_ONLY_NOTES:
                    note = ""

            rows.append({
                "exchange": exchange,
                "symbol": analysis.get("venueSymbol") or "",
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
st.subheader("Select Token")
group = st.radio("Asset class", list(TOKEN_GROUPS), index=list(TOKEN_GROUPS).index(DEFAULT_GROUP), horizontal=True)
group_tokens = TOKEN_GROUPS[group]
token = st.selectbox(
    "Token",
    group_tokens,
    index=group_tokens.index(DEFAULT_TOKEN) if DEFAULT_TOKEN in group_tokens else 0,
    label_visibility="collapsed",
)

st.text("Assumptions")
st.markdown("""
- Full clip size can be taken against existing orderbook (this is unlikely in actual execution as market makers can react to taker orders and adjust / cancel as they deem fit)
- Slippage is calculated based on average of buy side / ask side book
- Venues marked (RFQ) are priced from quotes, not a live orderbook: Variational slippage comes from its quoted sizes, and Extended RFQ markets only publish an indicative book
- Taker fees default to each exchange's base tier; change them under **Taker fees** below
""")

# region custom fees per exchange
# first entry is the default; every exchange also gets a "Custom" option
TAKER_FEE_PROFILES: dict[str, list[dict[str, Any]]] = {
    "Extended": [{"bps": 2.5, "assumption": ""}],
    "Hyperliquid": [
        {"bps": 4.5, "assumption": "<5M 14D volume, 0 HYPE staked"},
        {"bps": 4.0, "assumption": ">5M 14D volume, 0 HYPE staked"},
        {"bps": 3.5, "assumption": ">25M 14D volume, 0 HYPE staked"},
        {"bps": 3.0, "assumption": ">100M 14D volume, 0 HYPE staked"},
        {"bps": 2.8, "assumption": ">500M 14D volume, 0 HYPE staked"},
        {"bps": 2.6, "assumption": ">2B 14D volume, 0 HYPE staked"},
        {"bps": 2.4, "assumption": ">7B 14D volume, 0 HYPE staked"},
    ],
    "Lighter": [
        {"bps": 0.0, "assumption": "Free Account"},
        {"bps": 2.0, "assumption": "Premium Account"},
    ],
    # assumed to follow mainnet Lighter's account types; not checked against the Robinhood chain docs
    "Lighter on Robinhood": [
        {"bps": 0.0, "assumption": "Free Account"},
        {"bps": 2.0, "assumption": "Premium Account"},
    ],
    "Nado": [
        {"bps": 3.5, "assumption": "<5M 30D volume"},
        {"bps": 3.3, "assumption": ">5M 30D volume"},
        {"bps": 3.0, "assumption": ">25M 30D volume"},
        {"bps": 2.8, "assumption": ">100M 30D volume"},
        {"bps": 2.5, "assumption": ">500M 30D volume"},
        {"bps": 1.8, "assumption": ">1B 30D volume"},
        {"bps": 1.5, "assumption": ">5B 30D volume"},
    ],
    "Pacifica": [{"bps": 4.0, "assumption": ""}],
    # Phoenix sets the fee per market; 3.5 bps is the fallback trading-tools uses when a market has none
    "Phoenix": [{"bps": 3.5, "assumption": "base, varies by market"}],
    # QFEX tiers from trading-tools bot/venues.toml ([qfex.tiers.tier5], under $2m 30D volume)
    "QFEX": [
        {"bps": 10.0, "assumption": "<2M 30D volume, single stocks"},
        {"bps": 5.0, "assumption": "<2M 30D volume, indices / commodities"},
    ],
    "RISEx": [{"bps": 3.0, "assumption": ""}],
    "Variational": [{"bps": 0.0, "assumption": "spread built into quotes"}],
}
CUSTOM_FEE = "Custom"
FEE_GRID_COLUMNS = 4

def _fee_label(p: dict) -> str:
    return f"{p['bps']:.2f} bps" + (f" ({p['assumption']})" if p["assumption"] else "")

fee_map: dict[str, float] = {}
with st.expander("Taker fees", expanded=False):
    exchanges = list(ORDERBOOK_FETCHERS)
    for row_start in range(0, len(exchanges), FEE_GRID_COLUMNS):
        cols = st.columns(FEE_GRID_COLUMNS)
        for col, exch in zip(cols, exchanges[row_start:row_start + FEE_GRID_COLUMNS]):
            profiles = TAKER_FEE_PROFILES[exch]
            options = [_fee_label(p) for p in profiles] + [CUSTOM_FEE]
            with col:
                choice = st.selectbox(exch, options, index=0, key=f"profile_{exch}")
                if choice == CUSTOM_FEE:
                    fee_map[exch] = st.number_input(
                        f"{exch} taker fee (bps)",
                        min_value=0.0,
                        max_value=100.0,
                        value=float(profiles[0]["bps"]),
                        step=0.1,
                        format="%.2f",
                        key=f"custom_fee_{exch}",
                    )
                else:
                    fee_map[exch] = profiles[options.index(choice)]["bps"]
# endregion

# region cache actual orderbook fetching / analysis + add custom taker fees separately
# 1) fetch / cache raw, fee-less analysis
with st.spinner("Fetching orderbooks and computing slippage..."):
    raw_analysis_list = get_all_analysis_pure_cached(token.lower())

# 2) apply current taker fees to the raw analysis
analysis_list: list[dict[str, Any]] = []
for a in raw_analysis_list:
    if "slippage" not in a:
        analysis_list.append(a)
        continue

    exch = a["exchange"]
    taker_fee_bps = fee_map.get(exch, 0.0)

    new_a = {**a}
    new_a["takerFeeBps"] = taker_fee_bps

    new_slip = {}
    for size, s in a["slippage"].items():
        sbps = s["slippageBps"]
        total_cost_bps = sbps + taker_fee_bps if sbps is not None else None

        new_slip[size] = {
            **s,
            "takerFeeBps": taker_fee_bps,
            "totalCostBps": total_cost_bps,
        }

    new_a["slippage"] = new_slip
    analysis_list.append(new_a)

# 3) build tables from analysis_list (now includes customised fees)
tables = reorganize_by_clip_size(analysis_list)
# endregion


st.markdown('---')
st.subheader("Slippage Rankings")
st.caption(datetime.now().strftime("Last updated: %Y-%m-%d %H:%M:%S"))

MEDALS = {1: "🥇", 2: "🥈", 3: "🥉"}
RANK_BY = {"Full cost": "totalCostBps", "Slippage only": "slippageBps"}

def _bps(v, places: int) -> str:
    return "" if v is None or pd.isna(v) else f"{v:.{places}f}"

def build_rankings_table(df: pd.DataFrame, sort_col: str) -> pd.DataFrame:
    # unfilled clips are inf; sort them last and show them blank (the note says why)
    ranked = df.sort_values(sort_col, key=lambda c: c.replace({float("inf"): 1e18})).reset_index(drop=True)
    finite = lambda v: None if v == float("inf") else v
    return pd.DataFrame({
        # only venues that filled the clip are ranked (and get medals)
        "Rank": [
            f"{MEDALS.get(i, '')} {i}".strip() if finite(v) is not None and not pd.isna(v) else "–"
            for i, v in enumerate(ranked[sort_col], start=1)
        ],
        "Exchange": ranked["exchange"],
        "Symbol": ranked["symbol"],
        "Total (bps)": [_bps(finite(v), 3) for v in ranked["totalCostBps"]],
        "Slippage (bps)": [_bps(finite(v), 3) for v in ranked["slippageBps"]],
        "Fee (bps)": [_bps(v, 2) for v in ranked["takerFeeBps"]],
        "Notes": ranked["note"],
    }).set_index("Rank")

# hand-built HTML table: st.dataframe can't be restyled and st.table greys out the rank and spaces columns unevenly.
# fixed widths keep exchange / symbol / numbers compact so notes get the remaining space
RANKINGS_COLUMNS = [  # (header, width, numeric)
    ("Rank", "6%", False),
    ("Exchange", "14%", False),
    ("Symbol", "11%", False),
    ("Total (bps)", "10%", True),
    ("Slippage (bps)", "10%", True),
    ("Fee (bps)", "10%", True),
    ("Notes", "39%", False),
]
st.markdown(
    """
    <style>
    table.rankings { width: 100%; table-layout: fixed; border-collapse: collapse; font-size: 1rem; border: 1px solid rgba(128, 128, 128, 0.3); }
    table.rankings th, table.rankings td { padding: 0.45rem 0.75rem; border: 1px solid rgba(128, 128, 128, 0.2); text-align: left; overflow-wrap: anywhere; }
    table.rankings thead th { background: rgba(128, 128, 128, 0.08); }
    table.rankings th { font-weight: 600; opacity: 0.75; white-space: nowrap; }
    table.rankings .num { text-align: right; font-variant-numeric: tabular-nums; }
    table.rankings td.rank { white-space: nowrap; }
    button[data-baseweb="tab"] p { font-size: 1.1rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

def rankings_html(df: pd.DataFrame) -> str:
    rows = df.reset_index()
    cols = "".join(f'<col style="width:{w}">' for _, w, _ in RANKINGS_COLUMNS)
    head = "".join(f'<th class="{"num" if n else ""}">{h}</th>' for h, _, n in RANKINGS_COLUMNS)
    body = "".join(
        "<tr>" + "".join(
            f'<td class="{"num" if n else ("rank" if h == "Rank" else "")}">{html.escape(str(r[h]))}</td>'
            for h, _, n in RANKINGS_COLUMNS
        ) + "</tr>"
        for _, r in rows.iterrows()
    )
    return f'<table class="rankings"><colgroup>{cols}</colgroup><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>'

rank_by = st.radio("Rank by", list(RANK_BY), horizontal=True)

if not tables:
    st.info(f"None of the exchanges returned an orderbook for {token}.")
else:
    clip_sizes = sorted(tables)
    for tab, clip_size in zip(st.tabs([f"${c / 1000:g}k" for c in clip_sizes]), clip_sizes):
        with tab:
            st.markdown(rankings_html(build_rankings_table(tables[clip_size], RANK_BY[rank_by])), unsafe_allow_html=True)
