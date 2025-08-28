import os
import json
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from web3 import Web3
from logging import Logger
from utils.render_utils import donation_address
from eth_abi import decode
from eth_utils import to_checksum_address
from hexbytes import HexBytes

# db setup
db_url = os.getenv("DATABASE_URL", "sqlite:///users.db")  # fallback to sqlite
engine = create_engine(db_url, echo=False, future=True)

# only stores premium users i.e. logging in without payment doesn't affect this DB
USERS_TABLE = "users"

def init_db(logger: Logger):
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
                logger.info("no initial seed data provided")
                return
            try:
                seed_data = json.loads(seed_json)
                if not isinstance(seed_data, list):
                    raise ValueError("seed data must be a list of dicts")
            except (json.JSONDecodeError, ValueError) as e:
                logger.error(f"error decoding INITIAL_USERS secret: {e}")
                return

            # check if table already has data
            count = conn.execute(text(f"SELECT COUNT(*) FROM {USERS_TABLE}")).scalar()
            if count > 0:
                logger.info("users table already has data, skipping seeding")
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
                    logger.error(f"failed to insert seed row {row.get('email')}: {e}")

            logger.info("seed data inserted successfully")

    except SQLAlchemyError as e:
        logger.error(f"DB initialisation error: {e}")


def is_premium_user(email: str, logger: Logger) -> bool:
    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT COUNT(*) FROM {USERS_TABLE} WHERE email = :email"),
            {"email": email}
        ).scalar()
        logger.info(f"email {email} found as {'not ' if result == 0 else ''} premium user")
        return result > 0


def upgrade_to_premium(
    email: str,
    payment_txn_hash: str,
    payment_chain: str,
    logger: Logger
) -> str | None:  # error message if any (if None, verification was successful)
    """should be mutually exclusive with is_premium_user() but have check just in case"""
    if is_premium_user(email):
        return  # already premium

    payment_verification_error = verify_valid_payment(email, payment_txn_hash, payment_chain)
    if payment_verification_error is not None:
        return payment_verification_error

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

    # TODO verify correctness of inserting data,
    # need refresh page or will auto refresh?

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

def verify_valid_payment(
    email: str,
    payment_txn_hash: str,
    payment_chain: str,
    logger: Logger
) -> str | None:  # error message if any (if None, verification was successful)
    HYPERLIQUID_RPC_URL = os.getenv("HYPERLIQUID_RPC_URL")

    # initialise and check web3 connection
    w3 = Web3(Web3.HTTPProvider(HYPERLIQUID_RPC_URL))
    if not w3.is_connected():
        logger.error("setup connection to hyperliquid endpoint failed, check RPC")
        return "Unable to connect to the Hyperliquid RPC, please refresh or try again later. If this keeps happening, contact me"

    # TODO: implement actual blockchain payment verification

    # TODO verify web3 payment
    # TODO then separately verify hash not already stored somewhere

    print(f"\nFetching transaction receipt for hash: {payment_txn_hash}")

    try:
        tx_receipt = w3.eth.get_transaction_receipt(payment_txn_hash)

        if tx_receipt:
            # SC call i.e. not transferring native HYPE
            # check if transferred correct amount of USD₮0
            if tx_receipt['logs']:
                print(f"Found {len(tx_receipt['logs'])} logs in the transaction receipt.")
                print("\n--- Decoded Events ---")

                if len(tx_receipt['logs']): # simple transfer
                    log = tx_receipt['logs'][0]

                    # 1. Extract and decode the indexed topics.
                    # The 'from' and 'to' addresses are stored as padded hex strings in topics[1] and topics[2].
                    # We can slice the last 40 characters (20 bytes) to get the address and then convert it to a checksum address.
                    from_address = to_checksum_address(log['topics'][1].hex()[-40:])
                    to_address = to_checksum_address(log['topics'][2].hex()[-40:])

                    # 2. Decode the non-indexed data.
                    # The `value` is a `uint256` and is stored in the `data` field.
                    # We use `eth_abi.decode` with a list of the data types to decode.
                    decoded_data = decode(['uint256'], log['data'])
                    value_wei = decoded_data[0]

                    # 3. Get token details from the contract
                    token_contract = w3.eth.contract(address=log['address'], abi=ERC20_ABI)
                    token_name = token_contract.functions.name().call()
                    token_symbol = token_contract.functions.symbol().call()
                    token_decimals = token_contract.functions.decimals().call()

                    # 4. Calculate the final human-readable value.
                    value_formatted = value_wei / (10 ** token_decimals)


                    print(f"Transaction Status: {'Success' if tx_receipt['status'] == 1 else 'Failed'}")
                    print(f"From Address: {tx_receipt['from']}")
                    print(f"To Address: {tx_receipt['to']}")
                    print(f"Transaction Value: {value_formatted} {token_symbol}")
                    transferred_token_correct_usdt = log['address'].lower() == '0xB8CE59FC3717ada4C02eaDF9682A9e934F625ebb'.lower()
                    transferred_min_amount = value_formatted >= 10
                    # TODO make these reference desired amounts
                    if transferred_token_correct_usdt \
                        and transferred_min_amount \
                        and to_address == donation_address:
                        # TODO check no repeated transactions - or check that later
                        return None
                    print("-" * 25)
            else:
                # native token
                # value is in txn object (not hash)
                # since no SC i.e. no logs
                txn = w3.eth.get_transaction(payment_txn_hash)

                # Extract gas and price information for fee calculation
                gas_used = tx_receipt['gasUsed']
                effective_gas_price = tx_receipt['effectiveGasPrice']

                # Convert the gas price from wei to gwei for better readability
                gas_price_gwei = w3.from_wei(effective_gas_price, 'gwei')

                # Calculate the transaction fee in wei and gwei
                tx_fee_wei = gas_used * effective_gas_price
                tx_fee_gwei = w3.from_wei(tx_fee_wei, 'gwei')

                # Extract transaction value from the transaction details
                tx_value_wei = txn['value']

                # Convert the transaction value from wei to ether (or HYPE in this case, since it uses 18 decimals)
                tx_value_hype = w3.from_wei(tx_value_wei, 'ether')

                print(f"Transaction Status: {'Success' if tx_receipt['status'] == 1 else 'Failed'}")
                print(f"From Address: {tx_receipt['from']}")
                print(f"To Address: {tx_receipt['to']}")
                print(f"Transaction Value: {tx_value_hype} HYPE")
                print("-" * 25)
        else:
            print("No logs found for this transaction hash.")

    except Exception as e:
        # Print a detailed error message to help with debugging
        print(f"An error occurred: {e}")
        print("Please ensure the transaction hash is valid and on the correct network.")