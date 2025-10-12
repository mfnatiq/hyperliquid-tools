from datetime import datetime, timedelta, timezone
import os
import time
from dotenv import load_dotenv
import pandas as pd
import logging
from sqlalchemy import DateTime, create_engine, func, or_, select, MetaData, Table, Column, String, Float, Integer, inspect, DateTime
from sqlalchemy.dialects.postgresql import TIMESTAMP # for pg specific type
from sqlalchemy.exc import SQLAlchemyError
from hyperliquid.info import Info
from hyperliquid.utils import constants
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

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
    Column(
        "last_updated",
        TIMESTAMP(timezone=True) if is_postgresql else DateTime,
        server_default=func.now(),  # handles inserts
        server_onupdate=func.now(), # handles updates
    ),
)
# endregion

def initialize_database_schema():
    """helper to create tables if don't exist"""
    try:
        # use metadata.create_all() to create tables based on definitions
        metadata.create_all(engine)
        logger.info("database schema initialised, tables checked / created")
    except SQLAlchemyError as e:
        logger.error(f"error initializing database schema: {e}")
        raise

def get_addresses_to_query(limit: int) -> pd.DataFrame:
    try:
        with engine.connect() as conn:
            # only fetch those from too long ago
            prev_time = datetime.now(timezone.utc) - timedelta(hours=12)

            # get either those in trading leaderboard table
            # or those in bridging table that were fetched a while ago
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

def update_bridging_leaderboard(data: list):
    with engine.begin() as conn:
        logger.info("starting database transaction for bridging leaderboard updates")

        stmt = pg_insert(leaderboard_table).values(data) if is_postgresql else sqlite_insert(leaderboard_table).values(data)
        update_cols = {col: getattr(stmt.excluded, col)
                        for col in leaderboard_table.c.keys() if col != "user_address"}
        upsert_stmt = stmt.on_conflict_do_update(
            index_elements=['user_address'],
            set_=update_cols
        )
        conn.execute(upsert_stmt)
        logger.info(f"inserted {len(data)} rows")

        conn.commit()
        logger.info("db txn committed successfully")

        return True

if __name__ == "__main__":
    try:
        unit_token_mappings, token_list, candlestick_data = load_data()

        start = time.time()

        initialize_database_schema()
        limit = 100

        num_addresses_processed = 0

        addresses_to_update = list(get_addresses_to_query(limit)['user_address'])

        while len(addresses_to_update) > 0:
            logger.info(f'fetching bridging data for {len(addresses_to_update)} addresses')
            operations = unit_bridge_info.get_operations(addresses_to_update, show_logs=False)

            rows_to_insert = []

            for addr, ops in operations.items():
                processed_bridge_data = process_bridge_operations(
                    ops, unit_token_mappings, candlestick_data, logger
                )
                df_bridging, top_bridged_asset = create_bridge_summary(
                    processed_bridge_data)

                if df_bridging is not None:
                    total_vol = df_bridging['Total (USD)'].sum()

                    rows_to_insert.append({
                        'user_address': addr,
                        'total_volume_usd': total_vol,
                        'top_bridged_asset': top_bridged_asset,
                    })
                else:
                    # prevent infinite looping
                    rows_to_insert.append({
                        'user_address': addr,
                        'total_volume_usd': 0,
                        'top_bridged_asset': None,
                    })

            num_addresses_processed += len(rows_to_insert)

            success = update_bridging_leaderboard(rows_to_insert)
            if success:
                logger.info(f"bridging leaderboard update completed successfully for {len(rows_to_insert)} rows; processed {num_addresses_processed} so far")
            else:
                logger.error("bridging leaderboard update failed")

            addresses_to_update = list(get_addresses_to_query(limit)['user_address'])

        logger.info(f'updating entire leaderboard DB ({num_addresses_processed} addresses) took {(time.time() - start)}s')
    except Exception as e:
        logger.critical(f"error during script execution: {e}")