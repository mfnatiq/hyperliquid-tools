import requests
import json
import sys
from utils.render_utils import donation_address
from eth_abi import decode
from eth_utils import to_checksum_address
from hexbytes import HexBytes

# HEADERS = {
#     'accept': 'application/json',
#     'content-type': 'application/json'
# }

# # Define the JSON payload as a Python dictionary
# # This dictionary contains all the parameters for the alchemy_getAssetTransfers method
# PAYLOAD = {
#     "id": 1,
#     "jsonrpc": "2.0",
#     "method": "alchemy_getAssetTransfers",
#     "params": [
#         {
#             "fromAddress": "abc",
#             "toBlock": "latest",
#             "toAddress": donation_address,
#             "withMetadata": False,
#             "excludeZeroValue": True,
#             "maxCount": "0x3e8",
#             "category": ["external", "erc20"]
#         }
#     ]
# }

# print("Attempting to connect to the Alchemy API...")

# try:
#     # Make the POST request to the API with the URL, headers, and JSON payload
#     # The json=PAYLOAD argument automatically serializes the dictionary to a JSON string
#     response = requests.post(HYPERLIQUID_URL, headers=HEADERS, json=PAYLOAD)

#     # Raise an exception if the request was not successful
#     response.raise_for_status()

#     # Get the JSON response from the server
#     data = response.json()

#     # Print the full response in a formatted, readable way
#     # The 'indent' parameter makes the JSON output easy to read
#     print("API call successful! Here is the response:")
#     print(json.dumps(data, indent=4))

# except requests.exceptions.RequestException as e:
#     # Handle any errors that occur during the request
#     # This includes network issues, invalid URLs, or bad responses
#     print(f"An error occurred during the API call: {e}", file=sys.stderr)
# except json.JSONDecodeError:
#     # Handle cases where the response is not valid JSON
#     print("Failed to decode JSON from the response.", file=sys.stderr)
#     print(f"Response content: {response.text}", file=sys.stderr)
# except Exception as e:
#     print(f"An unexpected error occurred: {e}", file=sys.stderr)


from dotenv import load_dotenv
import os

load_dotenv()

from web3 import Web3
from eth_abi import decode
from eth_utils import to_checksum_address

HYPERLIQUID_RPC_URL = os.getenv("HYPERLIQUID_RPC_URL")

# Initialize Web3 connection
w3 = Web3(Web3.HTTPProvider(HYPERLIQUID_RPC_URL))
# Check the connection to the blockchain
if not w3.is_connected():
    print("Connection to Hyperliquid endpoint failed. Please check your network URL.")
    exit()

print("Successfully connected to the Hyperliquid network!")

# -------------------------------------------------------------------------
# Define the transaction hash to get the logs from
# -------------------------------------------------------------------------


# ERC-20 ABI snippets for getting token details
# The full ABI is complex, but we only need the functions for name, symbol, and decimals.
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

# A valid transaction hash from Hyperliquid that includes a "Transfer" event.
# This hash is for a USDC transfer.
tx_hashes = [
    os.getenv("TX_HASH_ONE"),
    os.getenv("TX_HASH_TWO"),
]

for tx_hash_string in tx_hashes:
    print(f"\nFetching transaction receipt for hash: {tx_hash_string}")

    try:
        tx_receipt = w3.eth.get_transaction_receipt(tx_hash_string)

        if tx_receipt:
            if tx_receipt['logs']:  # SC call i.e. not transferring native HYPE
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
                    print("-" * 25)
            else:
                # native token
                # value is in txn object (not hash)
                # since no SC i.e. no logs
                txn = w3.eth.get_transaction(tx_hash_string)

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