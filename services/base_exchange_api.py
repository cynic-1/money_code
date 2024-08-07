from typing import Optional
import requests
from pandas import DataFrame

INTERVAL_MS_MAP: dict[str, int] = {
    '8h': 8 * 3600 * 1000,
    '4h': 4 * 3600 * 1000,
    '1h': 1 * 3600 * 1000,
    '30m': 30 * 60 * 1000,
    '15m': 15 * 60 * 1000,
}


class BaseExchangeAPI:
    def __init__(self, base_url, limit):
        self.base_url = base_url
        self.limit = limit
        self.session = requests.Session()

    def get_local_time(self):
        raise NotImplementedError("Subclasses must implement this method")

    def get_token_full_name(self):
        raise NotImplementedError("Subclasses must implement this method")

    def get_candle_sticks(self, symbol: str, start: str, end: str, base: str,
                          limit: int, interval: str
                          ):
        raise NotImplementedError("Subclasses must implement this method")

    def init_history_price(self, symbol: str, max_entries: int, limit: int, interval: str) -> Optional[DataFrame]:
        raise NotImplementedError("Subclasses must implement this method")

    def get_history_price(self, symbol: str, last_time, end_time, limit: int, interval: str):
        raise NotImplementedError("Subclasses must implement this method")
