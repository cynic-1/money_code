import requests
from config.settings import Settings
from pandas import DataFrame
from utils.timestamp import get_current_hour_timestamp
import time


class ExchangeAPI:
    def __init__(self, base_url=Settings.API_URL, limit=Settings.API_LIMIT):
        self.base_url = base_url
        self.limit = limit

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
        response = requests.get(self.base_url, params=params)
        response.raise_for_status()
        return response.json()

    def get_history_price(self, symbol: str) -> DataFrame:
        candle_sticks = []
        end = get_current_hour_timestamp()

        while True:
            start = end - Settings.API_LIMIT * 8 * 3600 * 1000
            try:
                tmp = self.get_candle_sticks(symbol, start=start, end=end)
                n_entries = len(tmp)
                print(n_entries)
                candle_sticks += tmp
                end = start
                if n_entries < 500:
                    break
                time.sleep(0.2)
            except Exception as e:
                time.sleep(0.5)
        df = DataFrame(candle_sticks)
        df.columns = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'turnover']
        df.drop(['high', 'low', 'close', 'volume', 'close_time', 'turnover'], axis=1, inplace=True)
        df.set_index('open_time', inplace=True)
        return df
