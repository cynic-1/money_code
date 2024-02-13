from typing import Optional, Dict

import requests
from config.settings import Settings
from pandas import DataFrame
from utils.timestamp import get_current_hour_timestamp_ms
from utils.transform import list2df_kline, pair2token, list2symbol_fullname
import time
from utils.logger import Logger
from requests.exceptions import HTTPError

INTERVAL_MS_MAP: dict[str, int] = {
    '8h': 8 * 3600 * 1000,
    '4h': 4 * 3600 * 1000,
    '1h': 1 * 3600 * 1000,
    '30m': 30 * 60 * 1000,
    '15m': 15 * 60 * 1000,
}


class BaseExchangeAPI:
    def __init__(self, base_url=Settings.API_URL, limit=Settings.API_LIMIT):
        self.base_url = base_url
        self.limit = limit
        self.session = requests.Session()

    def get_token_full_name(self):
        raise NotImplementedError("Subclasses must implement this method")

    def get_candle_sticks(self, symbol: str, start: str, end: str, base: str = Settings.DEFAULT_BASE,
                          limit: int = Settings.API_LIMIT, interval: str = Settings.DEFAULT_INTERVAL
                          ):
        raise NotImplementedError("Subclasses must implement this method")

    def init_history_price(self, symbol: str, limit: int = Settings.API_LIMIT, interval: str = Settings.DEFAULT_INTERVAL) -> Optional[DataFrame]:
        raise NotImplementedError("Subclasses must implement this method")

    def get_history_price(self, symbol: str, last_time, end_time, limit: int = Settings.API_LIMIT, interval: str = Settings.DEFAULT_INTERVAL):
        raise NotImplementedError("Subclasses must implement this method")
