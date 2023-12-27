from pandas import DataFrame


class EmaCaculator:
    def __init__(self, exchangeAPI):
        self.exchangeAPI = exchangeAPI
        # 需要获取btc的历史数据，来计算相对价格
        self.btc = self.exchangeAPI.get_history_price('BTC')

    def calculate_all_ema(self, prices: DataFrame) -> DataFrame:
        prices['vbtc'] = prices['open'] / self.btc['open']
        prices['ema12'] = prices['vbtc'].ewm(span=12, adjust=False).mean()
        prices['ema144'] = prices['vbtc'].ewm(span=144, adjust=False).mean()
        prices['ema169'] = prices['vbtc'].ewm(span=169, adjust=False).mean()
        prices['ema576'] = prices['vbtc'].ewm(span=576, adjust=False).mean()
        prices['ema676'] = prices['vbtc'].ewm(span=676, adjust=False).mean()
        return prices
