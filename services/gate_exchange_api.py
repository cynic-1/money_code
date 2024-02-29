from typing import Optional, Dict
from services.base_exchange_api import BaseExchangeAPI
from config.settings import Settings
from pandas import DataFrame
from utils.timestamp import get_current_hour_timestamp_s
from utils.transform import gate_list2df_kline, pair2token, list2symbol_fullname
import time
from utils.logger import Logger
from requests.exceptions import HTTPError

INTERVAL_S_MAP: dict[str, int] = {
    '8h': 8 * 3600,
    '4h': 4 * 3600,
    '1h': 1 * 3600,
    '30m': 30 * 60,
    '15m': 15 * 60,
}


class GateExchangeAPI(BaseExchangeAPI):
    def __init__(self, base_url, limit=Settings.API_LIMIT):
        super().__init__(base_url, limit)

    def get_local_time(self):
        return get_current_hour_timestamp_s()

    def get_token_full_name(self):
        response = self.session.get('https://data.gateapi.io/api2/1/marketlist')
        response.raise_for_status()
        data = response.json()['data']
        Logger.get_logger().info('Get all token list.')
        res = []
        for symbol_map in data:
            if len(res) > 0 and symbol_map['name'] == res[-1]['full_name']:
                continue

            res.append(
                {
                    'symbol': symbol_map['symbol'],
                    'full_name': symbol_map['name']
                }
            )
        return list(res)

    def get_candle_sticks(self, symbol: str, start: str, end: str, base: str = Settings.DEFAULT_BASE,
                          limit: int = Settings.API_LIMIT, interval: str = Settings.DEFAULT_INTERVAL
                          ):
        params = {
            'currency_pair': f'{symbol}_{base}',
            'from': start,
            'to': end,
            'interval': interval,
        }
        Logger.get_logger().debug(f'Requesting {symbol}_{base} from {start} to {end}')
        response = self.session.get(self.base_url + '/spot/candlesticks', params=params)
        response.raise_for_status()
        return response.json()

    # max_entries_k: 最多获取最近的多少条记录，主要用于数据过多的情况，一般来讲，2k条 8h已经为2年，足够分析
    def init_history_price(self, symbol: str, max_entries: int = 2000,  limit: int = Settings.API_LIMIT,
                           interval: str = Settings.DEFAULT_INTERVAL) -> Optional[DataFrame]:
        candle_sticks = []
        end = get_current_hour_timestamp_s()

        while True:
            start = end - limit * INTERVAL_S_MAP[interval]
            try:
                tmp = self.get_candle_sticks(symbol, start=start + INTERVAL_S_MAP[interval], end=end)
                n_entries = len(tmp)
                candle_sticks += tmp
                end = start
                if n_entries < limit:
                    break
                if len(candle_sticks) >= max_entries:
                    break
                time.sleep(0.01)
            except HTTPError as http_err:
                print(f"An error occurred: {http_err}")
                return None
            except Exception as e:
                print(f"An error occurred: {e}")
                time.sleep(0.5)

        if not candle_sticks:
            return None
        return gate_list2df_kline(candle_sticks)

    def get_history_price(self, symbol: str, last_time, end_time, limit: int = Settings.API_LIMIT,
                          interval: str = Settings.DEFAULT_INTERVAL):
        if end_time - last_time < INTERVAL_S_MAP[interval]:
            Logger.get_logger().info(f"{symbol} already the latest data")
            return None

        candle_sticks = []
        end = end_time

        while True:
            start = end - limit * INTERVAL_S_MAP[interval]
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
            return gate_list2df_kline(candle_sticks)
