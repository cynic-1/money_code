from typing import Optional, Dict

from config.settings import Settings
from pandas import DataFrame
from utils.timestamp import get_current_hour_timestamp_ms
from utils.transform import mexc_list2df_kline, pair2token, xeggex_list2symbol_fullname
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


class XeggexExchangeAPI(BaseExchangeAPI):
    def __init__(self, base_url, limit=Settings.API_LIMIT):
        super().__init__(base_url, limit)

    def get_local_time(self):
        return get_current_hour_timestamp_ms()

    def get_token_full_name(self):
        response = self.session.get(self.base_url + '/asset/getlist')
        response.raise_for_status()
        data = response.json()
        Logger.get_logger().info('Get token fullname.')
        return xeggex_list2symbol_fullname(data)

    # 有个很搞笑的事情，如果end-start>90 day，会报错；但是实际返回的数据只看end和countBack;
    # 好像也不看end。。。
    def get_candle_sticks(self, symbol: str, start: str, end: str, base: str = Settings.DEFAULT_BASE,
                          limit: int = Settings.API_LIMIT, interval: str = Settings.DEFAULT_INTERVAL
                          ):
        params = {
            'symbol': f'{symbol}/{base}',
            'resolution': interval,
            'from': start,
            'to': end,
            'countBack': limit
        }
        Logger.get_logger().debug(f'Requesting {symbol}{base} from {start} to {end}')
        response = self.session.get(self.base_url + '/market/candles', params=params)
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
