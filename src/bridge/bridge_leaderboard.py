from datetime import datetime
import pandas as pd
import os
from dotenv import load_dotenv
import logging
from sqlalchemy import DateTime, create_engine, MetaData, Table, Column, String, Float, func, inspect, DateTime, select
from sqlalchemy.dialects.postgresql import TIMESTAMP # for pg specific type

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

metadata = MetaData()

leaderboard_table = Table(
    "bridging_leaderboard",
    metadata,
    Column("user_address", String, primary_key=True),
    Column("total_volume_usd", Float),
    Column("top_bridged_asset", String),
    Column("last_updated", TIMESTAMP(timezone=True) if is_postgresql else DateTime),
)

def get_bridge_leaderboard_with_datetime_last_updated() -> tuple[datetime, pd.DataFrame]:
    """
    returns bridging leaderboard and datetime of earliest update
    """
    try:
        with engine.connect() as conn:
            # define the rank window function
            # this creates a new column named 'user_rank' in the SQL query result
            rank_window = func.rank().over(
                order_by=leaderboard_table.c.total_volume_usd.desc()
            ).label('user_rank')

            # add the rank_window to your select statement
            query = (
                select(leaderboard_table, rank_window)
                .order_by(leaderboard_table.c.total_volume_usd.desc())
            )

            results = conn.execute(query)
            leaderboard_rows = results.fetchall()
            column_names = results.keys()
            leaderboard_df = pd.DataFrame(leaderboard_rows, columns=column_names)

            return leaderboard_df['last_updated'].min(), leaderboard_df
    except Exception as e:
        logger.error(f'unable to fetch bridging leaderboard: {e}')
    return datetime.now(), pd.DataFrame()