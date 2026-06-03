import requests
from hyperliquid.utils import constants

class HyperliquidClient:
    def post(self, endpoint: str, payload: dict):
        resp = requests.post(f'{constants.MAINNET_API_URL}{endpoint}', json=payload)
        resp.raise_for_status()
        return resp.json()

    def spot_meta(self) -> dict:
        return self.post("/info", {"type": "spotMeta"})

    def meta(self, dex: str = "") -> dict:
        return self.post("/info", {"type": "meta", "dex": dex})

    def all_mids(self) -> dict:
        return self.post("/info", {"type": "allMids"})

    def query_sub_accounts(self, user: str) -> list:
        return self.post("/info", {"type": "subAccounts", "user": user})

    def candles_snapshot(self, name: str, interval: str, startTime: int, endTime: int) -> list:
        return self.post("/info", {
            "type": "candleSnapshot",
            "req": {"coin": name, "interval": interval, "startTime": startTime, "endTime": endTime},
        })
