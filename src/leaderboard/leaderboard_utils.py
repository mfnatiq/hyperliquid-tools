
import logging
import os

from dotenv import load_dotenv
from sqlalchemy import TIMESTAMP, Column, DateTime, Float, Integer, MetaData, String, Table, create_engine, inspect


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

def get_leaderboard_last_updated(logger: logging.Logger):
    try:
        with engine.connect() as conn:
            results = conn.execute(metadata_table.select())
            latest_date = results.all()[0][1]
            return latest_date
    except Exception as e:
        logger.error(f'unable to fetch last updated date of leaderboard: {e}')
    return None