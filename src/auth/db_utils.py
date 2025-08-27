import os
import json
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# DATABASE SETUP
db_url = os.getenv("DATABASE_URL", "sqlite:///users.db")  # fallback to sqlite
engine = create_engine(db_url, echo=False, future=True)

USERS_TABLE = "users"

# TODO replace all with logger
def init_db():
    """create table if not exists and seed initial data if table is empty"""
    try:
        with engine.begin() as conn:
            # create table if not exists
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {USERS_TABLE} (
                    email TEXT PRIMARY KEY,
                    payment_txn_hash TEXT,
                    payment_chain TEXT,
                    bypass_payment BOOLEAN,
                    remarks TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))

            # load seed data
            seed_json = os.getenv("INITIAL_USERS")
            if not seed_json:
                print("no initial seed data provided")
                return
            try:
                seed_data = json.loads(seed_json)
                if not isinstance(seed_data, list):
                    raise ValueError("seed data must be a list of dicts")
            except (json.JSONDecodeError, ValueError) as e:
                print(f"error decoding INITIAL_USERS secret: {e}")
                return

            # check if table already has data
            count = conn.execute(text(f"SELECT COUNT(*) FROM {USERS_TABLE}")).scalar()
            if count > 0:
                print("users table already has data, skipping seeding")
                return

            # insert seed data
            for row in seed_data:
                try:
                    conn.execute(
                        text(f"""
                            INSERT INTO {USERS_TABLE}
                            (email, payment_txn_hash, payment_chain, bypass_payment, remarks)
                            VALUES (:email, :txn, :chain, :bypass, :remarks)
                        """),
                        {
                            "email": row["email"],
                            "txn": row.get("payment_txn_hash"),
                            "chain": row.get("payment_chain"),
                            "bypass": row.get("bypass_payment", False),
                            "remarks": row.get("remarks")
                        }
                    )
                except SQLAlchemyError as e:
                    print(f"failed to insert seed row {row.get('email')}: {e}")

            print("seed data inserted successfully")

    except SQLAlchemyError as e:
        print(f"DB initialisation error: {e}")


def is_premium_user(email: str) -> bool:
    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT COUNT(*) FROM {USERS_TABLE} WHERE email = :email"),
            {"email": email}
        ).scalar()
        return result > 0


def upgrade_to_premium(email: str, payment_txn_hash: str, payment_chain: str):
    if is_premium_user(email):
        return  # already premium

    # TODO: verify_valid_payment(email, payment_txn_hash, payment_chain)

    with engine.begin() as conn:
        conn.execute(text(f"""
            INSERT INTO {USERS_TABLE} (email, payment_txn_hash, payment_chain, bypass_payment)
            VALUES (:email, :txn, :chain, :bypass)
            ON CONFLICT (email) DO UPDATE
            SET payment_txn_hash = EXCLUDED.payment_txn_hash,
                payment_chain = EXCLUDED.payment_chain
        """), {
            "email": email,
            "txn": payment_txn_hash,
            "chain": payment_chain,
            "bypass": False
        })


def verify_valid_payment(email: str, payment_txn_hash: str, payment_chain: str):
    # TODO: implement actual blockchain payment verification
