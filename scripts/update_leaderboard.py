from datetime import datetime, timezone
import os
from dotenv import load_dotenv
import time
import requests
import logging
from sqlalchemy import DateTime, create_engine, text, MetaData, Table, Column, String, Float, Integer, inspect
from sqlalchemy.dialects.postgresql import TIMESTAMP # for pg specific type
from sqlalchemy.exc import SQLAlchemyError
from hyperliquid.info import Info
from hyperliquid.utils import constants

import sys
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# add src root to search path so src import works
sys.path.insert(0, project_root)
from src.utils.utils import get_cached_unit_token_mappings

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

load_dotenv()

db_url = os.getenv("DATABASE_URL", "sqlite:///leaderboard.db")
try:
    engine = create_engine(db_url, echo=False, future=True)
except Exception as e:
    logger.error(f"error creating database engine: {e}, skipping leaderboard update")
    exit(1)

db_dialect = inspect(engine).dialect.name
is_postgresql = db_dialect == 'postgresql'
logger.info(f"detected database dialect: {db_dialect}")

# region table definitions
metadata = MetaData()

leaderboard_table = Table(
    "leaderboard",
    metadata,
    Column("user_address", String, primary_key=True),
    Column("total_volume_usd", Float),
    Column("user_rank", Integer),
)
metadata_table = Table(
    "leaderboard_metadata",
    metadata,
    Column("id", Integer, primary_key=True, default=1),
    # for pg, use TIMESTAMP WITH TIME ZONE
    # for sqlite (local testing), handle default value in the INSERT statement
    Column("last_updated_at", TIMESTAMP(timezone=True) if is_postgresql else DateTime)
)
# endregion

# region allium api details
ALLIUM_BASE_URL = "https://api.allium.so/api/v1/explorer"
ALLIUM_API_KEY = os.getenv("ALLIUM_API_KEY")
if not ALLIUM_API_KEY:
    logger.error("ALLIUM_API_KEY environment variable not set")
    exit(1)

def initialize_database_schema():
    """helper to create tables if don't exist"""
    try:
        with engine.connect() as conn:
            # use metadata.create_all() to create tables based on definitions
            metadata.create_all(conn)
            logger.info("database schema initialised, tables checked / created")

            # ensure leaderboard metadata table has at least one row if it's empty so that subsequent updates can use update()
            result = conn.execute(text(f"SELECT COUNT(*) FROM {metadata_table.name}")).scalar()
            if result == 0:
                logger.info("metadata table is empty, inserting initial timestamp")

                conn.execute(metadata_table.insert().values(last_updated_at=datetime.now(tz=timezone.utc)))
                conn.commit()
    except SQLAlchemyError as e:
        logger.error(f"error initializing database schema: {e}")
        raise
# endregion

# region main leaderboard update function
def update_leaderboard_data():
    start_time = time.time()
    logger.info("starting leaderboard update process")

    info = Info(constants.MAINNET_API_URL, skip_ws=True)
    unit_token_mappings = get_cached_unit_token_mappings(info, logger)

    params = {
        "coin_list": ','.join([f"'{coin}'" for coin in unit_token_mappings.keys()])
    }
    run_config = { 'limit': 1000 }

    headers = { "X-API-Key": ALLIUM_API_KEY }
    # TODO temporarily set to different query id to combine raw and dex trades until updated indexing
    leaderboard_query_id = os.getenv("ALLIUM_LEADERBOARD_QUERY_ID")
    if not leaderboard_query_id:
        logger.error("ALLIUM_LEADERBOARD_QUERY_ID environment variable not set")
        return False

    all_rows = []
    query_run_id = None
    status = "queued"
    poll_interval = 2

    try:
        # step 1: trigger async allium query
        logger.info(f"querying leaderboard (query id {leaderboard_query_id})")
        response = requests.post(
            f"{ALLIUM_BASE_URL}/queries/{leaderboard_query_id}/run-async",
            json={ "parameters": params, 'run_config': run_config },
            headers=headers,
        )
        response.raise_for_status()
        query_run_id = response.json()['run_id']
        logger.info(f"leaderboard query triggered with run id {query_run_id}")

        # step 2: poll for results
        while status in ['queued', 'running']:
            time.sleep(poll_interval)
            response = requests.get(
                f"{ALLIUM_BASE_URL}/query-runs/{query_run_id}",
                headers=headers,
            )
            response.raise_for_status()
            data = response.json()
            status = data['status']
            logger.info(f"query results polling status: {status}")

        if status == 'failed':
            logger.error(f"query failed: {data.get('error', 'Unknown error')}")
            return False
        elif status != 'success':
            logger.error(f"query did not succeed, final status: {status}, data: {data}")
            return False

        logger.info("query successful, fetching results")

        # step 3: get results once done
        response = requests.get(f"{ALLIUM_BASE_URL}/query-runs/{query_run_id}/results", headers=headers)
        response.raise_for_status()
        data_response = response.json()
        all_rows = data_response.get("data", [])

        if not all_rows:
            logger.warning("leaderboard query returned no data, skipping update")
            return False

        with engine.begin() as conn:
            logger.info("starting database transaction for leaderboard updates")

            conn.execute(leaderboard_table.delete())
            logger.info(f"deleted all rows from table {leaderboard_table.name}")

            conn.execute(leaderboard_table.insert(), all_rows)
            logger.info(f"inserted {len(all_rows)} rows")

            conn.execute(metadata_table.update().values(last_updated_at=datetime.now(tz=timezone.utc)))
            logger.info("updated leaderboard metadata table")

            conn.commit()
            logger.info("db txn committed successfully")

            return True
    except requests.exceptions.RequestException as e:
        logger.error(f"HTTP Request error during allium api call: {e}")
        return False
    except SQLAlchemyError as e:
        logger.error(f"SQLAlchemy error during database operation: {e}")
        return False
    except Exception as e:
        logger.error(f"unexpected error occurred: {e}")
        return False
    finally:
        logger.info(f"leaderboard update finished in {time.time() - start_time:.2f} seconds")
# endregion

if __name__ == "__main__":
    try:
        initialize_database_schema()
        success = update_leaderboard_data()
        if success:
            logger.info("leaderboard update completed successfully")
        else:
            logger.error("leaderboard update failed")
    except Exception as e:
        logger.critical(f"error during script execution: {e}")