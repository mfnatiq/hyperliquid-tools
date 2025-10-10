import json
import logging
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

UNIT_API_MAINNET_URL = 'https://api.hyperunit.xyz'

# number of addresses to process in a single parallel batch
BATCH_SIZE = 10
# number of seconds to wait between processing each batch
DELAY_SECONDS = 2

class UnitBridgeInfo():
    def __init__(self):
        self.base_url = UNIT_API_MAINNET_URL

        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
        })
        self._logger = logging.getLogger(__name__)

    def get_operations(self, addresses: list[str]) -> dict:
        """
        fetches list of operations for all addresses in parallel,
        handling rate limits by processing them in batches with a delay
        returns dict mapping each address to its operations data
        """
        all_results = {}

        # split addresses into smaller chunks
        address_batches = [
            addresses[i:i + BATCH_SIZE]
            for i in range(0, len(addresses), BATCH_SIZE)
        ]

        for i, batch in enumerate(address_batches):
            self._logger.info(
                f"processing batch {i + 1}/{len(address_batches)} with {len(batch)} addresses"
            )

            with ThreadPoolExecutor(max_workers=len(batch)) as executor:
                future_to_address = {
                    executor.submit(self._get_operations_for_address, address): address
                    for address in batch
                }

                # collects results for the current batch as they are completed
                for future in as_completed(future_to_address):
                    address = future_to_address[future]
                    try:
                        operations_data = future.result()
                        all_results[address] = operations_data
                    except Exception as e:
                        self._logger.error(f'error fetching operations for {address}: {e}')
                        all_results[address] = {"error": str(e)}

            # if this is not the last batch, wait before starting the next one
            if i < len(address_batches) - 1:
                self._logger.info(f"sleeping for {DELAY_SECONDS}s before next batch")
                time.sleep(DELAY_SECONDS)

        return all_results

    def _get_operations_for_address(self, address: str):
        """
        internal method to fetch operations for a single address.
        """
        url_path = f"/operations/{address}"
        return self._get(url_path)

    def _get(self, url_path: str):
        url = self.base_url + url_path
        try:
            response = self.session.get(url)
            self._handle_exception(response)
            return response.json()
        except requests.RequestException as e:
            raise Exception(f"Request failed for {url}: {e}")

    def _handle_exception(self, response):
        status_code = response.status_code
        if status_code < 400:
            return
        if 400 <= status_code < 500:
            try:
                err = response.json()
            except json.JSONDecodeError:
                raise Exception(status_code, None, response.text, None, response.headers)
            if err is None:
                raise Exception(status_code, None, response.text, None, response.headers)
            error_data = err.get("data")
            raise Exception(status_code, err["code"], err["msg"], response.headers, error_data)
        raise Exception(status_code, response.text)