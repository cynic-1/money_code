from typing import Optional, Dict

from config.settings import Settings
from pandas import DataFrame
from utils.timestamp import get_current_hour_timestamp_ms
from utils.transform import mexc_list2df_kline, pair2token, list2symbol_fullname
import time
from utils.logger import Logger
from requests.exceptions import HTTPError

from services.base_exchange_api import BaseExchangeAPI

INTERVAL_MS_MAP: dict[str, int] = {
    '8h': 8 * 3600 * 1000,
    '4h': 4 * 3600 * 1000,
    '1h': 1 * 3600 * 1000,
    '30m': 30 * 60 * 1000,
    '15m': 15 * 60 * 1000,
}


class MexcExchangeAPI(BaseExchangeAPI):
    def __init__(self, base_url, limit=Settings.API_LIMIT):
        super().__init__(base_url, limit)

    def get_local_time(self):
        return get_current_hour_timestamp_ms()

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

    # [from, to)
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

    def init_history_price(self, symbol: str, max_entries: int = 2000, limit: int = Settings.API_LIMIT, interval: str = Settings.DEFAULT_INTERVAL) -> Optional[DataFrame]:
        candle_sticks = []
        # 确保获取最新数据 [最早数据, end]
        end = get_current_hour_timestamp_ms()+INTERVAL_MS_MAP[interval]

        while True:
            start = end - limit * INTERVAL_MS_MAP[interval]
            try:
                tmp = self.get_candle_sticks(symbol, start=start, end=end, limit=limit, interval=interval)
                n_entries = len(tmp)
                candle_sticks += tmp
                end = start
                if n_entries < limit:
                    break
                if len(candle_sticks) >= max_entries:
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
        # 时间戳单位统一至s对外提供
        return mexc_list2df_kline(candle_sticks)

    def get_history_price(self, symbol: str, last_time, end_time, limit: int = Settings.API_LIMIT, interval: str = Settings.DEFAULT_INTERVAL):
        # 数据库统一至s，处理时先换为ms
        last_time = last_time*1000
        if end_time - last_time < INTERVAL_MS_MAP[interval]:
            Logger.get_logger().info(f"{symbol} already the latest data")
            return None

        candle_sticks = []
        # 加一个小的偏移量是为了避免获得重复数据。(last_time, end_time] => [last_time+interval, end_time+interval)
        end = end_time+INTERVAL_MS_MAP[interval]
        start = last_time+INTERVAL_MS_MAP[interval]

        while start < end:
            tmp_end = min(start+limit*INTERVAL_MS_MAP[interval], end)
            try:
                tmp = self.get_candle_sticks(symbol, start=start, end=tmp_end)
                candle_sticks += tmp
                start = tmp_end

                time.sleep(0.05)
            except HTTPError as http_err:
                print(f"An error occurred: {http_err}")
                return None
            except Exception as e:
                print(f"An error occurred: {e}")
                time.sleep(0.5)

        if len(candle_sticks) == 0:
            return None
        else:
            return mexc_list2df_kline(candle_sticks)
