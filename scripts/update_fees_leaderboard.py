"""
refresh fees for all addresses in the fees_leaderboard table.

to be ran periodically so that fees stay current as new trades accumulate

address seeding logic (mirrors update_bridging_leaderboard.py):
- on first run: pulls from trade leaderboard table (top 10k, populated by allium)
- subsequent runs: re-processes all addresses already in fees_leaderboard
- any address queried via the dashboard that isn't in the trade leaderboard
  gets added via the inline update in fees_leaderboard.update_fees_leaderboard()
"""
from datetime import timedelta
import json
import os
import sys
import time
import logging
import requests
from dotenv import load_dotenv
from hyperliquid.info import Info
from hyperliquid.utils import constants
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, Integer, inspect, select

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from src.consts import unitStartTime
from src.utils.utils import get_unit_token_mappings
from src.trade.fees_leaderboard import update_fees_leaderboard, fees_leaderboard_table, metadata as fees_metadata

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

load_dotenv()

HYDROMANCER_URL = "https://api.hydromancer.xyz/info"
HYDROMANCER_API_KEY = os.getenv("HYDROMANCER_API_KEY")
NUM_DAYS_WINDOW = 30
BATCH_SIZE = 50  # hydromancer is per-address so keep batches modest

db_url = os.getenv("DATABASE_URL", "sqlite:///leaderboard.db")
try:
    engine = create_engine(db_url, echo=False, future=True)
except Exception as e:
    logger.error(f"error creating database engine: {e}")
    exit(1)

db_dialect = inspect(engine).dialect.name
is_postgresql = db_dialect == 'postgresql'

metadata = MetaData()

# reference table: trade leaderboard (seeded by allium via update_leaderboard.py)
ref_leaderboard_table = Table(
    "leaderboard",
    metadata,
    Column("user_address", String, primary_key=True),
    Column("total_volume_usd", Float),
    Column("user_rank", Integer),
)


def initialize_database_schema() -> None:
    fees_metadata.create_all(engine)
    logger.info("fees_leaderboard table checked / created")


def get_addresses_from_trade_leaderboard(limit: int, offset: int) -> list[str]:
    try:
        with engine.connect() as conn:
            results = conn.execute(
                select(ref_leaderboard_table.c.user_address)
                .order_by(ref_leaderboard_table.c.user_address)
                .limit(limit)
                .offset(offset)
            )
            return [row[0] for row in results.fetchall()]
    except Exception as e:
        logger.error(f"error reading addresses from trade leaderboard: {e}")
        return []


def fetch_fills(account: str, start_time: int, end_time: int) -> list:
    response = requests.post(
        HYDROMANCER_URL,
        data=json.dumps({
            "type": "userFillsByTime",
            "user": account,
            "aggregateByTime": True,
            "startTime": start_time,
            "endTime": end_time,
        }),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {HYDROMANCER_API_KEY}",
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def compute_fees(account: str, token_coin_name_mapping: dict[str, str]) -> float:
    """fetch all unit fills for account and return total fees in USD"""
    curr_time_ms = int(time.time() * 1000)
    start_time = unitStartTime
    total_fees = 0.0

    while start_time < curr_time_ms:
        end_time = min(curr_time_ms, start_time + int(timedelta(days=NUM_DAYS_WINDOW).total_seconds()) * 1000)

        fills = fetch_fills(account, start_time, end_time)
        num_fills = len(fills)

        for f in fills:
            if f['coin'] not in token_coin_name_mapping:
                continue
            price = float(f['px'])
            fee_in_stables = f['feeToken'] in ['USDC', 'USDH']
            fee_amt = float(f['fee']) if fee_in_stables else float(f['fee']) * price
            total_fees += fee_amt

        if num_fills == 2000:
            start_time = max(f['time'] for f in fills) + 1
        else:
            start_time = end_time + 1

    return total_fees


def update_all():
    info = Info(constants.MAINNET_API_URL, skip_ws=True)
    unit_token_mappings = get_unit_token_mappings(info, logger)
    token_coin_name_mapping = {k: v[0] for k, v in unit_token_mappings.items()}

    total_processed = 0
    offset = 0
    addresses = get_addresses_from_trade_leaderboard(BATCH_SIZE, offset)

    while addresses:
        logger.info(f"processing {len(addresses)} addresses (offset {offset})")

        fees_by_address = {}
        for address in addresses:
            try:
                total_fees = compute_fees(address, token_coin_name_mapping)
                fees_by_address[address] = {"Token Fees": total_fees, "Quote Fees": 0.0}
                logger.info(f"{address}: ${total_fees:.4f}")
            except Exception as e:
                logger.error(f"{address}: failed - {e}")

        if fees_by_address:
            update_fees_leaderboard(fees_by_address)

        total_processed += len(addresses)
        logger.info(f"batch done - {total_processed} addresses processed so far")

        offset += BATCH_SIZE
        addresses = get_addresses_from_trade_leaderboard(BATCH_SIZE, offset)

    if total_processed == 0:
        logger.warning("no addresses found in trade leaderboard - run update_leaderboard.py first to seed addresses")
    else:
        logger.info(f"done - processed {total_processed} addresses total")


if __name__ == "__main__":
    start = time.time()
    initialize_database_schema()
    update_all()
    logger.info(f"finished in {time.time() - start:.2f}s")
