from utils.logger import Logger
from services.exchange_api import ExchangeAPI
from services.ema_caculator import EmaCaculator
from db.database import Database
from config.settings import Settings
import json
from utils.timestamp import get_current_hour_timestamp


class MarketDataAnalyser:
    def __init__(self, base=Settings.DEFAULT_BASE):
        # with open(Settings.TOKEN_LIST, 'r') as f:
        #     self.token_list = json.load(f)
        self.base = base
        self.logger = Logger.get_logger()
        self.exchange_api = ExchangeAPI()
        self.ema_caculator = EmaCaculator(exchangeAPI=self.exchange_api)
        self.database = Database()
        self.token_list = self.exchange_api.get_token_list()

    def init_database(self):
        for token in self.token_list:
            prices = self.exchange_api.init_history_price(symbol=token)
            if prices is None:
                self.logger.error(f'Skip {token}')
                continue
            prices = self.ema_caculator.calculate_all_ema(prices)
            # prices.drop(['open', 'vbtc'], axis=1, inplace=True)

            self.database.store_data(prices, symbol=token)

            self.logger.info(f"Finish {token} initialization.")

    def update_database(self):
        cur = get_current_hour_timestamp()
        for token in self.token_list:
            last_ema = self.database.get_latest_data(token)
            if last_ema is None:
                continue
            self.logger.debug(f'Table {token} last record open_time: {last_ema[0]}')
            # 加一个小的偏移量是为了避免获得重复数据。
            time = last_ema[0] + 10000

            prices = self.exchange_api.get_history_price(symbol=token, last_time=time, end_time=cur)
            if prices is None:
                self.logger.error(f'Skip {token}')
                continue
            prices = self.ema_caculator.update_ema(prices, ema=last_ema)
            prices.drop(['open', 'vbtc'], axis=1, inplace=True)

            self.database.store_data(prices, symbol=token)

            self.logger.info(f"Finish {token} Update.")
        

def main():
    mda = MarketDataAnalyser()
    # mda.init_database()
    mda.update_database()


if __name__ == '__main__':
    main()

