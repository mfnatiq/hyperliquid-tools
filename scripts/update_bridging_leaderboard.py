from datetime import datetime, timedelta, timezone
import os
from dotenv import load_dotenv
import time
import pandas as pd
import requests
import logging
from sqlalchemy import DateTime, create_engine, or_, select, MetaData, Table, Column, String, Float, Integer, inspect, DateTime
from sqlalchemy.dialects.postgresql import TIMESTAMP # for pg specific type
from sqlalchemy.exc import SQLAlchemyError
from hyperliquid.info import Info
from hyperliquid.utils import constants

import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# add src root to search path so src import works
sys.path.insert(0, project_root)

from src.utils.utils import get_cached_unit_token_mappings
from src.bridge.unit_bridge_api import UnitBridgeInfo
from src.trade.trade_data import get_candlestick_data
from src.bridge.unit_bridge_utils import create_bridge_summary, process_bridge_operations

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

load_dotenv()

# logic for fetching addresses to query:
# if bridging leaderboard does not exist, fetch from trading leaderboard (top 10k)
# for anyone else querying whose addresses are not in leaderboard, add manually
# for subsequent runs, use existing addresses in bridging leaderboard to be refetched

db_url = os.getenv("DATABASE_URL", "sqlite:///leaderboard.db")
try:
    engine = create_engine(db_url, echo=False, future=True)
except Exception as e:
    logger.error(f"error creating database engine: {e}, skipping leaderboard update")
    exit(1)

db_dialect = inspect(engine).dialect.name
is_postgresql = db_dialect == 'postgresql'
logger.info(f"detected database dialect: {db_dialect}")

unit_bridge_info = UnitBridgeInfo()

# region table definitions
metadata = MetaData()

ref_leaderboard_table = Table(
    "leaderboard",
    metadata,
    Column("user_address", String, primary_key=True),
    Column("total_volume_usd", Float),
    Column("user_rank", Integer),
)

leaderboard_table = Table(
    "bridging_leaderboard",
    metadata,
    Column("user_address", String, primary_key=True),
    Column("total_volume_usd", Float),
    Column("top_bridged_asset", String),
    Column("last_updated", TIMESTAMP(timezone=True) if is_postgresql else DateTime),   # possibly different as users search and have data added
)
# endregion

def initialize_database_schema():
    """helper to create tables if don't exist"""
    try:
        with engine.connect() as conn:
            # use metadata.create_all() to create tables based on definitions
            metadata.create_all(conn)
            logger.info("database schema initialised, tables checked / created")
    except SQLAlchemyError as e:
        logger.error(f"error initializing database schema: {e}")
        raise

def get_ref_leaderboard(limit: int) -> pd.DataFrame:
    try:
        with engine.connect() as conn:
            # only fetch those from too long ago
            prev_time = datetime.now(timezone.utc) - timedelta(hours=12)
            results = conn.execute(
                select(ref_leaderboard_table.c.user_address)
                .outerjoin(leaderboard_table, ref_leaderboard_table.c.user_address == leaderboard_table.c.user_address)
                .where(
                    or_(
                        leaderboard_table.c.user_address == None,
                        leaderboard_table.c.last_updated < prev_time
                    )
                )
                .limit(limit)
            )
            leaderboard_rows = results.fetchall()
            column_names = results.keys()
            leaderboard_df = pd.DataFrame(leaderboard_rows, columns=column_names)
            return leaderboard_df
    except Exception as e:
        logger.error(f'unable to fetch leaderboard: {e}')
    return pd.DataFrame()


def load_data():
    info = Info(constants.MAINNET_API_URL, skip_ws=True)
    unit_token_mappings = get_cached_unit_token_mappings(info, logger)
    logger.info(f'unit token mappings: {unit_token_mappings}')
    token_list = [t for t, _ in unit_token_mappings.values()]
    candlestick_data = get_candlestick_data(
        info, [k for k in unit_token_mappings.keys()], token_list)

    return unit_token_mappings, token_list, candlestick_data

# copied from dashboard.py to prevent full loading from there
# TODO change so it puts every address separately
def format_bridge_data(
    raw_bridge_data: dict,
    unit_token_mappings: dict[str, tuple[str, int]],
    candlestick_data: pd.DataFrame,
):
    all_operations_df = pd.DataFrame()
    for _, data in raw_bridge_data.items():
        processed_df = process_bridge_operations(
            data, unit_token_mappings, candlestick_data, logger)
        if processed_df is not None and not processed_df.empty:
            all_operations_df = pd.concat(
                [all_operations_df, processed_df], ignore_index=True)
    return all_operations_df

if __name__ == "__main__":
    try:
        unit_token_mappings, token_list, candlestick_data = load_data()

        initialize_database_schema()
        limit = 100
        addresses = list(get_ref_leaderboard(limit)['user_address'])
        logger.info(f'fetching bridging data for {len(addresses)} addresses')
        operations = unit_bridge_info.get_operations(addresses)

        rows_to_insert = []

        for addr, ops in operations.items():
            bridge_operations_for_addr = { addr: ops }
            processed_bridge_data = format_bridge_data(
                bridge_operations_for_addr, unit_token_mappings, candlestick_data)
            df_bridging, top_bridged_asset = create_bridge_summary(
                processed_bridge_data)

            if df_bridging is not None:
                total_vol = df_bridging['Total (USD)'].sum()

                rows_to_insert.append((
                    addr, total_vol, top_bridged_asset, datetime.now(timezone.utc)
                ))

        breakpoint()
        # success = update_bridging_leaderboard_data()
        # if success:
        #     logger.info("bridging leaderboard update completed successfully")
        # else:
        #     logger.error("bridging leaderboard update failed")
    except Exception as e:
        logger.critical(f"error during script execution: {e}")