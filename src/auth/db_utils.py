from decimal import Decimal
from enum import Enum
import os
import json
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, Row
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from web3 import Web3
from logging import Logger
from src.utils.render_utils import donation_address
from eth_abi import decode
from eth_utils import to_checksum_address

load_dotenv()

# db setup
db_url = os.getenv("DATABASE_URL", "sqlite:///users.db")  # fallback to sqlite
engine = create_engine(db_url, echo=False, future=True)

USERS_TABLE = "users"


@dataclass
class User:
    """represents a user record from DB"""
    email: str
    payment_txn_hash: Optional[str]
    payment_chain: Optional[str]
    trial_expires_at: Optional[datetime]
    upgraded_at: Optional[datetime]
    bypass_payment: bool
    remarks: Optional[str]
    created_at: datetime


def _to_datetime(value, logger: Logger) -> Optional[datetime]:
    """
    safely converts a value from the database (str, int, float, or datetime)
    to a timezone-aware datetime object
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        # if already datetime, ensure timezone aware
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
    if isinstance(value, str):
        try:
            # handle ISO 8601 format strings, common for many DBs
            return datetime.fromisoformat(value.replace('Z', '+00:00')).replace(tzinfo=timezone.utc)
        except ValueError:
            logger.error(f"could not parse datetime string: {value}")
            return None
    if isinstance(value, (int, float)):
        try:
            # handle unix timestamps
            return datetime.fromtimestamp(value, tz=timezone.utc)
        except (ValueError, OSError):
            logger.error(f"could not convert timestamp to datetime: {value}")
            return None
    logger.warning(f"unhandled type for datetime conversion: {type(value)}")
    return None


def _row_to_user_object(row: Optional[Row], logger: Logger) -> Optional[User]:
    """
    safely converts a SQLAlchemy Row object to a User dataclass object,
    handling potential type mismatches from DB
    """
    if row is None:
        return None
    try:
        return User(
            email=row.email,
            payment_txn_hash=row.payment_txn_hash,
            payment_chain=row.payment_chain,
            trial_expires_at=_to_datetime(row.trial_expires_at, logger),
            # explicitly cast to boolean to handle integers (0/1)
            upgraded_at=_to_datetime(row.upgraded_at, logger),
            bypass_payment=bool(row.bypass_payment),
            remarks=row.remarks,
            created_at=_to_datetime(row.created_at, logger)
        )
    except (TypeError, AttributeError, ValueError) as e:
        logger.error(
            f"Failed to deserialize database row to User object. Row: {dict(row._mapping)}. Error: {e}")
        return None


def init_db(logger: Logger):
    """create table if not exists and seed initial data if table is empty"""
    try:
        with engine.begin() as conn:
            # create table if not exists
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {USERS_TABLE} (
                    email TEXT PRIMARY KEY,
                    payment_txn_hash TEXT UNIQUE,
                    payment_chain TEXT,
                    trial_expires_at TIMESTAMP,
                    upgraded_at TIMESTAMP,
                    bypass_payment BOOLEAN DEFAULT FALSE,
                    remarks TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))

            # insert/update admin emails with full privileges (bypass)
            admin_emails_str = os.getenv("ADMIN_EMAILS", "")
            admin_emails = [email.strip().lower()
                            for email in admin_emails_str.split(",") if email.strip()]
            for admin_email in admin_emails:
                try:
                    conn.execute(text(f"""
                        INSERT INTO {USERS_TABLE}
                        (email, bypass_payment, remarks, created_at)
                        VALUES (:email, :bypass, :remarks, :created_at)
                        ON CONFLICT (email) DO UPDATE
                        SET bypass_payment = TRUE,
                            remarks = 'Admin account - bypass payment'
                    """), {
                        "email": admin_email,
                        "bypass": True,
                        "remarks": "Admin account - bypass payment",
                        "created_at": datetime.now(tz=timezone.utc),
                    })
                    logger.info(f"Ensured admin bypass for: {admin_email}")
                except SQLAlchemyError as e:
                    logger.error(
                        f"Failed to setup admin account for {admin_email}: {e}")

            # load seed data, then check if table already has initial data; if not, seed
            seed_json = os.getenv("INITIAL_USERS")
            if not seed_json:
                logger.info("no initial seed data provided")
                return
            try:
                seed_data = json.loads(seed_json)
                if not isinstance(seed_data, list):
                    raise ValueError("seed data must be a list of dicts")
            except (json.JSONDecodeError, ValueError) as e:
                logger.error(f"error decoding INITIAL_USERS: {e}")
                return
            count = conn.execute(
                text(f"SELECT COUNT(*) FROM {USERS_TABLE}")).scalar()
            if count > 0:
                logger.info("users table already has data, skipping seeding")
                return
            for row in seed_data:
                try:
                    conn.execute(
                        text(f"""
                            INSERT INTO {USERS_TABLE}
                            (email, payment_txn_hash, payment_chain, trial_expires_at, upgraded_at, bypass_payment, remarks, created_at)
                            VALUES (:email, :txn, :chain,  :trial_expires_at, :upgraded_at, :bypass, :remarks, :created_at)
                        """),
                        {
                            "email": row["email"],
                            "txn": row.get("payment_txn_hash"),
                            "chain": row.get("payment_chain"),
                            "trial_expires_at": row.get("trial_expires_at"),
                            "upgraded_at": row.get("upgraded_at"),
                            "bypass": row.get("bypass_payment", False),
                            "remarks": row.get("remarks"),
                            "created_at": row.get("created_at", datetime.now(tz=timezone.utc)),
                        }
                    )
                except SQLAlchemyError as e:
                    logger.error(
                        f"failed to insert seed row {row.get('email')}: {e}")

            logger.info("seed data inserted successfully")

    except SQLAlchemyError as e:
        logger.error(f"DB initialisation error: {e}")


def get_user(email: str, logger: Logger) -> Optional[User]:
    """fetches a user record from the database and returns it as a User object"""
    with engine.connect() as conn:
        result_row = conn.execute(
            text(f"SELECT * FROM {USERS_TABLE} WHERE email = :email"),
            {"email": email}
        ).first()
        return _row_to_user_object(result_row, logger)


NUM_TRIAL_DAYS = 7


def start_trial_if_new_user(email: str, logger: Logger) -> str | None:
    """
    if a user does not exist, create a new record with a limited-time trial

    returns error message if any
    """
    if get_user(email, logger) is None:
        try:
            with engine.begin() as conn:
                trial_end_date = datetime.now(
                    timezone.utc) + timedelta(days=NUM_TRIAL_DAYS)
                conn.execute(
                    text(f"""
                        INSERT INTO {USERS_TABLE} (email, trial_expires_at)
                        VALUES (:email, :trial_expires)
                    """),
                    {"email": email, "trial_expires": trial_end_date}
                )
                logger.info(
                    f"started {NUM_TRIAL_DAYS}-day trial for new user: {email}")
        except IntegrityError:
            # race condition: another process inserted the user just now
            logger.warning(
                f"user {email} was created by another process concurrently")
            return f"Failed to start trial for {email} as {email} was already created, please try another email"
        except SQLAlchemyError as e:
            logger.error(f"failed to start trial for {email}: {e}")
            return f"Failed to start trial for {email} due to a database error, please try again later"
        except Exception as e:
            logger.error(f'failed to start free trial with unknown error {e}')
            return f"Failed to start trial for {email} due to an unknown error, please try again later"

    return None


class PremiumType(Enum):
    FULL = 1
    TRIAL = 2
    NONE = 3


def _is_full_premium_user(user: User) -> bool:
    """
    check if a user is fully premium, either by having a valid payment or bypass_payment flag (excl. trial)
    """
    # check bypass_payment first for infinite access
    if user.bypass_payment:
        return True

    # in case of manually updating DB (e.g. payment issues), user.upgraded_at will be none
    is_paid = user.payment_txn_hash is not None or user.upgraded_at is not None

    return user.bypass_payment or is_paid


def get_user_premium_type(email: str, logger: Logger) -> PremiumType:
    """
    check if a user is premium (includes trial)
    """
    user = get_user(email, logger)
    if not user:
        return PremiumType.NONE

    if _is_full_premium_user(user):
        return PremiumType.FULL

    is_trial_active = user.trial_expires_at is not None and user.trial_expires_at > datetime.now(
        timezone.utc)
    if is_trial_active:
        return PremiumType.TRIAL

    return PremiumType.NONE


def upgrade_to_premium(
    email: str,
    payment_txn_hash: str,
    payment_chain: str,
    acceptedPayments: dict,
    logger: Logger,
) -> str | None:  # error message if any (if None, verification was successful)
    # must be mutually exclusive with get_user_premium_type()
    user = get_user(email, logger)
    payment_verification_error = _verify_valid_payment(
        email, user, payment_txn_hash, payment_chain, acceptedPayments, logger)
    if payment_verification_error is not None:
        return payment_verification_error

    try:
        with engine.begin() as conn:
            # check if txn hash has already been used by another user
            existing_user_row = conn.execute(
                text(
                    f"SELECT email FROM {USERS_TABLE} WHERE payment_txn_hash = :txn"),
                {"txn": payment_txn_hash}
            ).first()
            if existing_user_row and existing_user_row.email != email:
                logger.error(
                    f"txn hash {payment_txn_hash} already used by {existing_user_row.email}")
                return "This payment transaction has already been registered by another user, please use a unique transaction. If you think someone sniped your transaction hash submission, please contact me"

            # insert or update user record
            # ON CONFLICT handles new users and trial users upgrading
            # UPDATE sets payment info
            conn.execute(text(f"""
                INSERT INTO {USERS_TABLE} (email, payment_txn_hash, payment_chain, upgraded_at, bypass_payment)
                VALUES (:email, :txn, :chain, :upgraded_at, :bypass)
                ON CONFLICT (email) DO UPDATE
                SET payment_txn_hash = EXCLUDED.payment_txn_hash,
                    upgraded_at = EXCLUDED.upgraded_at,
                    payment_chain = EXCLUDED.payment_chain,
            """), {
                "email": email,
                "txn": payment_txn_hash,
                "chain": payment_chain,
                'upgraded_at': datetime.now(tz=timezone.utc),
                "bypass": False
            })

        # check successful upgrade
        updated_user = get_user(email, logger)
        if updated_user and updated_user.payment_txn_hash == payment_txn_hash:
            # TODO update remarks column to say when upgraded instead
            logger.info(
                f"successfully upgraded {email} to premium with txn {payment_txn_hash}")
            return None  # Success
        else:
            logger.error(
                f"failed to verify database update for {email} after premium upgrade")
            return "An unexpected error occurred while updating your account: please contact me"

    except IntegrityError:
        logger.error(
            f"IntegrityError: txn hash {payment_txn_hash} is likely already in use")
        return "This payment transaction has already been registered by another user, please use a unique transaction. If you think someone sniped your transaction hash submission, please contact me"
    except SQLAlchemyError as e:
        logger.error(
            f"Database error during premium upgrade for {email} with txn hash {payment_txn_hash}: {e}")
        return "A backend error occurred, please try again later or contact me"


# ABI snippet for getting erc20 token details
ERC20_ABI = [
    {
        "constant": True,
        "inputs": [],
        "name": "name",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "symbol",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "type": "function",
    },
]


def _verify_valid_payment(
    email: str,
    user: User,
    payment_txn_hash: str,
    payment_chain: str,
    acceptedPayments: dict,
    logger: Logger,
) -> str | None:  # error message if any (if None, verification was successful)
    """
    does not handle checks for repeated txn hash, only checks that a hash is valid
    """
    HYPERLIQUID_RPC_URL = os.getenv("HYPERLIQUID_RPC_URL")

    # initialise and check web3 connection
    w3 = Web3(Web3.HTTPProvider(HYPERLIQUID_RPC_URL))
    if not w3.is_connected():
        logger.error(
            "setup connection to hyperliquid endpoint failed, check RPC")
        return "Unable to connect to the Hyperliquid RPC, please refresh or try again later. If this keeps happening, contact me"

    logger.info(f"fetching txn receipt for hash: {payment_txn_hash}")

    try:
        tx_receipt = w3.eth.get_transaction_receipt(payment_txn_hash)

        # sanity check that payment was after trial started
        block = w3.eth.get_block(tx_receipt['blockNumber'])
        tx_timestamp = datetime.fromtimestamp(
            block['timestamp'], tz=timezone.utc)
        if tx_timestamp < user.created_at:
            logger.warning(
                f"txn {payment_txn_hash} for user {email} is too old: {tx_timestamp} vs. user created date {user.created_at}")
            return f"""
                This transaction is invalid, please make a new payment

                If you have previously already donated, please contact me for manual access!
            """

        if tx_receipt and tx_receipt['logs']:
            # SC call i.e. not transferring native HYPE
            # check if transferred correct amount of USD₮0
            logger.info(
                f"found {len(tx_receipt['logs'])} logs in txn receipt")

            if len(tx_receipt['logs']):  # check for simple transfer
                log = tx_receipt['logs'][0]

                # extract and decode indexed topics
                # "from" and "to" addresses are stored as padded hex strings in topics[1] and topics[2]
                # slice the last 40 chars (20 bytes) to get address, then convert it to a checksum address
                from_address = to_checksum_address(
                    log['topics'][1].hex()[-40:])
                recipient_address = to_checksum_address(
                    log['topics'][2].hex()[-40:])

                # Logging the from_address is useful for debugging and tracking payments.
                logger.info(f"Decoded ERC20 transfer from: {from_address}")

                # decode the non-indexed data
                # "value" is uint256 and is stored in the `data` field
                # use eth_abi.decode with a list of the data types to decode.
                decoded_data = decode(['uint256'], log['data'])
                value_wei = decoded_data[0]

                # get token details from the contract
                token_contract = w3.eth.contract(
                    address=log['address'], abi=ERC20_ABI)
                token_symbol = token_contract.functions.symbol().call()
                token_decimals = token_contract.functions.decimals().call()

                # convert to human-readable value
                value_formatted = value_wei / (10 ** token_decimals)

                logger.info(f"""
                    txn status: {'success' if tx_receipt['status'] == 1 else 'failed'}
                    from address: {tx_receipt['from']}
                    to address: {recipient_address}
                    txn value: {value_formatted} {token_symbol}
                """)

                transferred_token_correct_address = log['address'].lower(
                ) == acceptedPayments[token_symbol]['address'].lower()

                # no need conversion as value_formatted is float
                transferred_exact_amount = value_formatted == acceptedPayments[
                    token_symbol]['minAmount']
                recipient_address_correct = recipient_address.lower() == donation_address.lower()
                if transferred_token_correct_address \
                    and transferred_exact_amount \
                    and recipient_address_correct:
                    return None
                else:
                    logger.warning(
                        f'{email} transferred {value_formatted} {token_symbol}, invalid vs. accepted payments {acceptedPayments}')
                    return f"""
                        The submitted txn shows a transfer of {value_formatted} {token_symbol} to {recipient_address}, which does not fulfill the subscription requirements

                        If you think there has been an error, please DM me!
                    """
        else:
            # for native token (hype), value is in txn object
            # not in txn hash since no SC i.e. no logs
            txn = w3.eth.get_transaction(payment_txn_hash)

            # convert txn value of hype from wei to ether (same number of decimals as HYPE)
            tx_value_wei = txn['value']
            tx_value_hype = w3.from_wei(tx_value_wei, 'ether')

            recipient_address = tx_receipt['to']

            logger.info(f"""
                txn status: {'success' if tx_receipt['status'] == 1 else 'failed'}
                from address: {tx_receipt['from']}
                to address: {recipient_address}
                txn value: {tx_value_hype} HYPE
            """)

            # need exact decimal string comparison
            transferred_exact_amount = tx_value_hype == Decimal(str(acceptedPayments['HYPE']['minAmount']))
            recipient_address = tx_receipt['to']
            recipient_address_correct = recipient_address.lower() == donation_address.lower()
            if transferred_exact_amount \
                and recipient_address_correct:
                return None
            else:
                logger.warning(
                    f'{email} transferred {tx_value_hype} HYPE, invalid vs. accepted payments {acceptedPayments}')
                return f"""
                    The submitted txn shows a transfer of {tx_value_hype} HYPE to {recipient_address}, which does not fulfill the subscription requirements

                    If you think there has been an error, please DM me!
                """

    except Exception as e:
        # Print a detailed error message to help with debugging
        logger.error(f"error validating payment hash: {e}")
        return """
            You have not entered a valid hyperevm transaction: please ensure the transaction hash is valid and on the correct network

            If you have paid a valid amount but on the wrong network, please contact me!
        """
