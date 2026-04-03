import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from hyperliquid.info import Info
from hyperliquid.utils import constants


class UnitBridgeInfo():
    def __init__(self, max_concurrent=10):
        self._info = Info(constants.MAINNET_API_URL, skip_ws=True)
        self._max_concurrent = max_concurrent
        self._logger = logging.getLogger(__name__)

    def get_operations(self, addresses: list[str], start_time: int = 0, show_logs=True) -> dict:
        """
        fetches userNonFundingLedgerUpdates for multiple addresses via the HL API
        with max_concurrent requests running in parallel
        completed slots pick up the next address immediately rather than waiting for a full batch to finish

        returns dict mapping each address to its list of raw ledger entries
        """
        if show_logs:
            self._logger.info(f"fetching ledger for {len(addresses)} addresses (max {self._max_concurrent} concurrent)")

        all_results = {}

        with ThreadPoolExecutor(max_workers=self._max_concurrent) as executor:
            future_to_address = {
                executor.submit(self._get_operations_for_address, address, start_time): address
                for address in addresses
            }
            for future in as_completed(future_to_address):
                address = future_to_address[future]
                try:
                    all_results[address] = future.result()
                except Exception:
                    self._logger.exception(f'error fetching operations for {address}')
                    all_results[address] = []

        return all_results

    # TODO check from HL discord if this endpoint has max limit then refactor if needed, currently just fetches everything
    def _get_operations_for_address(self, address: str, start_time: int) -> list:
        return self._info.post("/info", {
            "type": "userNonFundingLedgerUpdates",
            "user": address,
            "startTime": start_time,
        })
