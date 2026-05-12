from datetime import datetime
import pandas as pd
import os
import sys
from dotenv import load_dotenv
import logging
from typing import Any
from sqlalchemy import DateTime, create_engine, MetaData, Table, Column, String, Float, func, inspect, select
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

load_dotenv()

db_url = os.getenv("DATABASE_URL", "sqlite:///leaderboard.db")
try:
    engine = create_engine(db_url, echo=False, future=True)
except Exception as e:
    logger.error(f"error creating database engine: {e}")
    exit(1)

db_dialect = inspect(engine).dialect.name
is_postgresql = db_dialect == 'postgresql'

metadata = MetaData()

fees_leaderboard_table = Table(
    "fees_leaderboard",
    metadata,
    Column("user_address", String, primary_key=True),
    Column("total_fees_usd", Float),
    Column(
        "last_updated",
        TIMESTAMP(timezone=True) if is_postgresql else DateTime,
        server_default=func.now(),
        server_onupdate=func.now(),
    ),
)


def get_fees_leaderboard_with_datetime_last_updated() -> tuple[datetime, pd.DataFrame]:
    try:
        with engine.connect() as conn:
            rank_window = func.rank().over(
                order_by=fees_leaderboard_table.c.total_fees_usd.desc()
            ).label('user_rank')

            query = (
                select(fees_leaderboard_table, rank_window)
                .order_by(fees_leaderboard_table.c.total_fees_usd.desc())
            )

            results = conn.execute(query)
            leaderboard_rows = results.fetchall()
            column_names = results.keys()
            leaderboard_df = pd.DataFrame(leaderboard_rows, columns=column_names)

            return leaderboard_df['last_updated'].min(), leaderboard_df
    except Exception as e:
        logger.error(f'unable to fetch fees leaderboard: {e}')
    return datetime.now(), pd.DataFrame()


def update_fees_leaderboard(accounts_mapping: dict) -> bool:
    """
    Upsert fees for each queried address. Returns True if any rows were inserted/updated.
    """
    rows = []
    for address, stats in accounts_mapping.items():
        total_fees = float(stats['Token Fees'] + stats['Quote Fees'])
        if total_fees > 0:
            rows.append({
                "user_address": address,
                "total_fees_usd": total_fees,
            })

    if not rows:
        return False

    try:
        with engine.begin() as conn:
            stmt = pg_insert(fees_leaderboard_table).values(rows) if is_postgresql else sqlite_insert(fees_leaderboard_table).values(rows)

            upsert_stmt: Any = stmt.on_conflict_do_update(
                index_elements=['user_address'],
                set_={"total_fees_usd": stmt.excluded.total_fees_usd},
            )

            if is_postgresql:
                upsert_stmt = upsert_stmt.returning(fees_leaderboard_table.c.user_address)

            result = conn.execute(upsert_stmt)

            num_affected = len(result.fetchall()) if is_postgresql else result.rowcount
            logger.info(f"upserted fees for {num_affected} address(es)")
            return num_affected > 0
    except Exception as e:
        logger.error(f"error upserting fees leaderboard: {e}")
        return False
