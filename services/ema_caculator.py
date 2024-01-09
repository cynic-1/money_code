from pandas import DataFrame


def _update_ema(last_ema, new_data, span):
    multiplier = 2 / (span + 1)
    ema = [last_ema]

    for price in new_data:
        ema.append((price - ema[-1]) * multiplier + ema[-1])

    return ema[1:]


class EmaCaculator:
    def __init__(self, exchangeAPI):
        self.exchangeAPI = exchangeAPI
        # 需要获取btc的历史数据，来计算相对价格
        self.btc = self.exchangeAPI.init_history_price('BTC')

    def calculate_all_ema(self, prices: DataFrame) -> DataFrame:
        prices['vbtc'] = prices['open'] / self.btc['open']
        prices['ema12'] = prices['vbtc'].ewm(span=12, adjust=False).mean()
        prices['ema144'] = prices['vbtc'].ewm(span=144, adjust=False).mean()
        prices['ema169'] = prices['vbtc'].ewm(span=169, adjust=False).mean()
        prices['ema576'] = prices['vbtc'].ewm(span=576, adjust=False).mean()
        prices['ema676'] = prices['vbtc'].ewm(span=676, adjust=False).mean()
        return prices

    def update_ema(self, prices: DataFrame, ema) -> DataFrame:
        _, ema12, ema144, ema169, ema576, ema676 = ema
        prices['vbtc'] = prices['open'] / self.btc['open']
        prices['ema12'] = _update_ema(float(ema12), prices['vbtc'], 12)
        prices['ema144'] = _update_ema(float(ema144), prices['vbtc'], 144)
        prices['ema169'] = _update_ema(float(ema169), prices['vbtc'], 169)
        prices['ema576'] = _update_ema(float(ema576), prices['vbtc'], 576)
        prices['ema676'] = _update_ema(float(ema676), prices['vbtc'], 676)
        return prices
