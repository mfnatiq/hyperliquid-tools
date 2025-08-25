from web3 import Web3
from dotenv import load_dotenv
import os
from utils.render_utils import donation_address

load_dotenv()

HYPERLIQUID_RPC_URL = os.getenv("HYPERLIQUID_RPC_URL")
FEE_WALLET = donation_address.lower()
TOKEN_ADDRESS = "0xTokenContract".lower()

# native token has no address
MIN_AMOUNT = Web3.to_wei(0.1, "ether")  # e.g. require 1 HYPE

w3 = Web3(Web3.HTTPProvider(HYPERLIQUID_RPC_URL))

ERC20_ABI = [
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "from", "type": "address"},
            {"indexed": True, "name": "to", "type": "address"},
            {"indexed": False, "name": "value", "type": "uint256"},
        ],
        "name": "Transfer",
        "type": "event",
    }
]

token_contract = w3.eth.contract(address=TOKEN_ADDRESS, abi=ERC20_ABI)

# TODO consider using allium data?