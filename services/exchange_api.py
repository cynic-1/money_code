from typing import Optional, Dict

import requests
from config.settings import Settings
from pandas import DataFrame
from utils.timestamp import get_current_hour_timestamp
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


class ExchangeAPI:
    def __init__(self, base_url=Settings.API_URL, limit=Settings.API_LIMIT):
        self.base_url = base_url
        self.limit = limit
        self.session = requests.Session()

    # Deprecated for now
    def get_token_list(self):
        response = self.session.get(self.base_url + '/defaultSymbols')
        response.raise_for_status()
        data = response.json()['data']
        Logger.get_logger().info('Get all token list.')
        return pair2token(data)

    def get_token_full_name(self):
        response = self.session.get(self.base_url + '/exchangeInfo')
        response.raise_for_status()
        data = response.json()['symbols']
        Logger.get_logger().info('Get token fullname.')
        return list2symbol_fullname(data)

    def get_candle_sticks(self, symbol: str, start: str, end: str, base: str = Settings.DEFAULT_BASE,
                          limit: int = Settings.API_LIMIT, interval: str = Settings.DEFAULT_INTERVAL
                          ):
        params = {
            'symbol': f'{symbol}{base}',
            'interval': interval,
            'startTime': start,
            'endTime': end,
            'limit': limit
        }
        Logger.get_logger().debug(f'Requesting {symbol}{base} from {start} to {end}')
        response = self.session.get(self.base_url + '/klines', params=params)
        response.raise_for_status()
        return response.json()

    def init_history_price(self, symbol: str, limit: int = Settings.API_LIMIT, interval: str = Settings.DEFAULT_INTERVAL) -> Optional[DataFrame]:
        candle_sticks = []
        end = get_current_hour_timestamp()

        while True:
            start = end - limit * INTERVAL_MS_MAP[interval]
            try:
                tmp = self.get_candle_sticks(symbol, start=start, end=end, limit=limit, interval=interval)
                n_entries = len(tmp)
                candle_sticks += tmp
                end = start
                if n_entries < limit:
                    break
                time.sleep(0.2)
            except HTTPError as http_err:
                print(f"An error occurred: {http_err}")
                return None
            except Exception as e:
                print(f"An error occurred: {e}")
                time.sleep(0.5)

        if not candle_sticks:
            return None
        return list2df_kline(candle_sticks)

    def get_history_price(self, symbol: str, last_time, end_time, limit: int = Settings.API_LIMIT, interval: str = Settings.DEFAULT_INTERVAL):
        if end_time - last_time < INTERVAL_MS_MAP[interval]:
            return None

        candle_sticks = []
        end = end_time

        while True:
            start = end - limit * INTERVAL_MS_MAP[interval]
            if start < last_time:
                start = last_time
            if end <= start:
                break
            try:
                tmp = self.get_candle_sticks(symbol, start=start, end=end)
                candle_sticks += tmp
                end = start
                time.sleep(0.2)
            except HTTPError as http_err:
                print(f"An error occurred: {http_err}")
                return None
            except Exception as e:
                print(f"An error occurred: {e}")
                time.sleep(0.5)

        if len(candle_sticks) == 0:
            return None
        else:
            return list2df_kline(candle_sticks)
