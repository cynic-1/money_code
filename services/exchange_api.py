from typing import Optional

import requests
from config.settings import Settings
from pandas import DataFrame
from utils.timestamp import get_current_hour_timestamp
from utils.transform import list2df_kline, pair2token
import time
from utils.logger import Logger
from requests.exceptions import HTTPError


class ExchangeAPI:
    def __init__(self, base_url=Settings.API_URL, limit=Settings.API_LIMIT):
        self.base_url = base_url
        self.limit = limit

    def get_token_list(self):
        response = requests.get(self.base_url+'/defaultSymbols')
        response.raise_for_status()
        data = response.json()['data']
        Logger.get_logger().info('Get all token list.')
        return pair2token(data)

    def get_candle_sticks(self, symbol: str, start: str, end: str, base: str = Settings.DEFAULT_BASE,
                          interval: str = Settings.DEFAULT_INTERVAL, limit: str = Settings.API_LIMIT
                          ):
        params = {
            'symbol': f'{symbol}{base}',
            'interval': interval,
            'startTime': start,
            'endTime': end,
            'limit': limit
        }
        Logger.get_logger().debug(f'Requesting {symbol}{base} from {start} to {end}')
        response = requests.get(self.base_url+'/klines', params=params)
        response.raise_for_status()
        return response.json()

    def init_history_price(self, symbol: str, limit: int = Settings.API_LIMIT) -> Optional[DataFrame]:
        candle_sticks = []
        end = get_current_hour_timestamp()

        while True:
            start = end - limit * 8 * 3600 * 1000
            try:
                tmp = self.get_candle_sticks(symbol, start=start, end=end)
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

        return list2df_kline(candle_sticks)

    def get_history_price(self, symbol: str, last_time, end_time, limit: int = Settings.API_LIMIT):
        if end_time - last_time < 8 * 3600 * 1000:
            return None

        candle_sticks = []
        end = end_time

        while True:
            start = end - limit * 8 * 3600 * 1000
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
