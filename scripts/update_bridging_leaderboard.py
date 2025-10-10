from datetime import datetime, timezone
import os
from dotenv import load_dotenv
import time
import pandas as pd
import requests
import logging
from sqlalchemy import DateTime, create_engine, text, MetaData, Table, Column, String, Float, Integer, inspect, DateTime
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
    Column("user_rank", Integer),
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

def get_ref_leaderboard() -> pd.DataFrame:
    try:
        with engine.connect() as conn:
            results = conn.execute(
                ref_leaderboard_table.
                select()
            )
            leaderboard_rows = results.fetchall()
            column_names = results.keys()
            leaderboard_df = pd.DataFrame(leaderboard_rows, columns=column_names)
            return leaderboard_df
    except Exception as e:
        logger.error(f'unable to fetch leaderboard: {e}')
    return pd.DataFrame()


if __name__ == "__main__":
    try:
        initialize_database_schema()
        accounts = list(get_ref_leaderboard()['user_address'])
        operations = unit_bridge_info.get_operations(accounts)
        breakpoint()
        # success = update_leaderboard_data()
        # if success:
        #     logger.info("bridging leaderboard update completed successfully")
        # else:
        #     logger.error("bridging leaderboard update failed")
    except Exception as e:
        logger.critical(f"error during script execution: {e}")