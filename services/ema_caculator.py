from pandas import DataFrame
from utils.logger import Logger

def _update_ema(last_ema, new_data, span):
    multiplier = 2 / (span + 1)
    ema = [last_ema]

    for price in new_data:
        ema.append((price - ema[-1]) * multiplier + ema[-1])

    return ema[1:]


class EmaCaculator:
    def __init__(self, exchangeAPI):
        self.exchangeAPI = exchangeAPI
        self.ema_list = [12, 144, 169, 576, 676]
        # 需要获取btc的历史数据，来计算相对价格
        self.btc = self.exchangeAPI.init_history_price('BTC')

    def calculate_all_ema(self, prices: DataFrame) -> DataFrame:
        prices['vbtc'] = prices['open'] / self.btc['open']

        for num in self.ema_list:
            prices[f'btc_ema_{num}'] = prices['vbtc'].ewm(span=num, adjust=False).mean()
            prices[f'usd_ema_{num}'] = prices['open'].ewm(span=num, adjust=False).mean()

        return prices

    def update_ema(self, prices: DataFrame, ema) -> DataFrame:
        ema_list = {}
        _, ema_list['usd_ema_12'], ema_list['usd_ema_144'], ema_list['usd_ema_169'], ema_list['usd_ema_576'], ema_list['usd_ema_676'], \
            ema_list['btc_ema_12'], ema_list['btc_ema_144'], ema_list['btc_ema_169'], ema_list['btc_ema_576'], ema_list['btc_ema_676'] = ema
        prices['vbtc'] = prices['open'] / self.btc['open']

        for num in self.ema_list:
            if ema_list[f'usd_ema_{num}'] is None:
                Logger.get_logger().info('No enough data to calculate, skip')
            else:
                prices[f'usd_ema_{num}'] = _update_ema(float(ema_list[f'usd_ema_{num}']), prices['open'], num)

            if ema_list[f'btc_ema_{num}'] is None:
                Logger.get_logger().info('No enough data to calculate, skip')
            else:
                prices[f'btc_ema_{num}'] = _update_ema(float(ema_list[f'btc_ema_{num}']), prices['vbtc'], num)

        return prices
