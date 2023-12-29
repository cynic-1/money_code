from pandas import DataFrame, to_numeric
from utils.logger import Logger
from services.exchange_api import ExchangeAPI
from services.ema_caculator import EmaCaculator
from db.database import Database
from config.settings import Settings
import json


class MarketDataAnalyser:
    def __init__(self, base=Settings.DEFAULT_BASE):
        with open(Settings.TOKEN_LIST, 'r') as f:
            self.token_list = json.load(f)
        self.base = base
        self.logger = Logger.get_logger()
        self.exchange_api = ExchangeAPI()
        self.ema_caculator = EmaCaculator(exchangeAPI=self.exchange_api)
        self.database = Database()

    def init_database(self):
        for token in self.token_list:
            prices = self.exchange_api.get_history_price(symbol=token)
            if prices is None:
                self.logger.error(f'Skip {token}')
                continue
            prices = self.ema_caculator.calculate_all_ema(prices)
            prices.drop(['open', 'vbtc'], axis=1, inplace=True)

            self.database.store_data(prices, table_name=token)

            self.logger.info(f"Finish {token} initialization.")


def main():
    mda = MarketDataAnalyser()
    mda.init_database()


if __name__ == '__main__':
    main()
