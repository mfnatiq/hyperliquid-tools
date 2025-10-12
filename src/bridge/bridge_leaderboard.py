from datetime import datetime
import pandas as pd
import os
from dotenv import load_dotenv
import logging
from sqlalchemy import DateTime, create_engine, MetaData, Table, Column, String, Float, func, inspect, DateTime, select
from sqlalchemy.dialects.postgresql import TIMESTAMP # for pg specific type
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

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

metadata = MetaData()

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

def update_bridge_leaderboard(bridge_data_by_address: list) -> bool:
    """
    returns true if there were updates and false otherwise
    """
    with engine.begin() as conn:
        logger.info(f'inserting up to {len(bridge_data_by_address)} rows if not already in bridging leaderboard table')
        rows = [
            (bridge_address, bridge_df_by_address['Total (USD)'].sum(), top_bridged_asset_by_address)
            for bridge_address, bridge_df_by_address, top_bridged_asset_by_address in bridge_data_by_address
        ]

        stmt = pg_insert(leaderboard_table).values(rows) if is_postgresql else sqlite_insert(leaderboard_table).values(rows)

        # on conflict do nothing: if address already exists, wait for its volume update from cron job subsequently
        # i.e. reduce need for table fetching cache invalidation
        insert_if_not_exists_stmt = stmt.on_conflict_do_nothing(
            index_elements=['user_address']
        )

        # for postgres, add a RETURNING clause to reliably get the affected rows
        # for sqlite, can use rowcount
        if is_postgresql:
            insert_if_not_exists_stmt = insert_if_not_exists_stmt.returning(leaderboard_table.c.user_address)

        result = conn.execute(insert_if_not_exists_stmt)

        # determine if updates happened
        if is_postgresql:
            # for pg, count the rows returned by the RETURNING clause
            inserted_rows = result.fetchall()
            num_inserted = len(inserted_rows)
        else:
            num_inserted = result.rowcount

        logger.info(f"inserted {num_inserted} new address(es)")

        return num_inserted > 0
