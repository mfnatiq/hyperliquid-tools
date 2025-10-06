import os
from dotenv import load_dotenv
import requests

address = '0x8D6F070e5e3F73758426007dA680324C10C2869C'
startblock = 0

load_dotenv()
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
        &address={address}
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
breakpoint()
