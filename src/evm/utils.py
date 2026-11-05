import os
from dotenv import load_dotenv
import requests

startblock = 0

load_dotenv()
ADDRESS = os.getenv("ADDRESS")
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")

txn_list = []

offset = 1000

done = False

while not done:
    print(f'querying from startblock {startblock}')
    query = f"""
        https://api.etherscan.io/v2/api
        ?chainid=999
        &module=account
        &action=txlist
        &address={ADDRESS}
        &startblock={startblock}
        &endblock=latest
        &page=1
        &offset={offset}
        &sort=asc
        &apikey={ETHERSCAN_API_KEY}
    """

    resp = requests.get(query.replace("\n", "").replace(" ", ""))

    results = resp.json()['result']
    txn_list.extend(results)

    if len(results) == offset:
        # set next startblock
        startblock = max(int(r['blockNumber']) for r in results) - 1
    else:
        done = True

swap_txns = [t for t in txn_list if ('swap' in t['functionName'] or 'Swap' in t['functionName']) and t['isError'] == '0']

from collections import defaultdict
txns_mapping = defaultdict(list)

for txn in swap_txns:
    txns_mapping[txn['functionName']].append(txn['hash'])

for func, hashes in txns_mapping.items():
    print(f'function name {func}:')
    print(f'txn hashes: {hashes}\n')

breakpoint()
