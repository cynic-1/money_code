import pandas
import sys
from utils.logger import Logger
from services.mexc_exchange_api import MexcExchangeAPI
from services.gate_exchange_api import GateExchangeAPI
from services.ema_caculator import EmaCaculator
from db.database import Database
from config.settings import Settings
from utils.timestamp import get_current_hour_timestamp_s


class MarketDataAnalyser:
    def __init__(self, exchange_api, base=Settings.DEFAULT_BASE):
        self.base = base
        self.logger = Logger.get_logger()
        self.exchange_api = exchange_api
        self.ema_caculator = EmaCaculator(exchangeAPI=self.exchange_api)
        self.database = Database()
        self.token_info = self.exchange_api.get_token_full_name()
        # self.token_info = [
        #     {
        #         'symbol': 'NADA',
        #         'full_name': 'NADA Protocol Token'
        #     },
        #     {
        #         'symbol': 'AGIX',
        #         'full_name': 'SingularityNET'
        #     },
        # ]

    def update_token_info(self):
        def _get_latest_time(row, db):
            sb = row['symbol']
            res = db.get_latest_data_by_symbol(sb)
            if res is None or res[0] is None:
                return None
            else:
                return res[0]

        df = pandas.DataFrame(self.token_info)
        df['latest_timestamp'] = df.apply(_get_latest_time, args=(self.database,), axis=1)
        df['exchange'] = Settings.EXCHANGE
        self.database.write_to_token_info(df)

    def get_data(self):
        cur = get_current_hour_timestamp_s()
        for ti in self.token_info:
            token = ti['symbol']
            last_ema = self.database.get_latest_data_by_symbol(token, 1)
            # if fetchone() finds no matching row, it returns None
            if last_ema is None:  # initiation logic
                prices = self.exchange_api.init_history_price(symbol=token)
                if prices is None:
                    self.logger.error(f'Skip {token}')
                    continue
                prices = self.ema_caculator.calculate_all_ema(prices)

                prices['count'] = list(range(1, len(prices)+1))

                self.database.commit_to_write_cache(prices, symbol=token)

                self.logger.info(f"Finish {token} initialization.")
            else:
                self.logger.info(f'Symbol {token} last record: {last_ema}')

                time = last_ema[0]
                prices = self.exchange_api.get_history_price(symbol=token, last_time=time, end_time=cur)

                if prices is None:
                    self.logger.error(f'Skip {token}')
                    continue
                prices = self.ema_caculator.update_ema(prices, ema=last_ema)

                prices['count'] = list(range(last_ema[-1]+1, len(prices)+last_ema[-1]+1))

                self.database.commit_to_write_cache(prices, symbol=token)

                self.logger.info(f"Finish {token} Update.")
        self.database.write_data()


def main():
    global cex_api
    if len(sys.argv) <= 1:
        print("No argument provided.")
        return
    exchange = sys.argv[1]
    print(exchange)
    if exchange == 'gate':
        Settings.EXCHANGE = 'gate'
        cex_api = GateExchangeAPI("https://api.gateio.ws/api/v4", 1000)
    elif exchange == 'mexc':
        Settings.EXCHANGE = 'mexc'
        cex_api = MexcExchangeAPI("https://api.mexc.com/api/v3", 1000)
    mda = MarketDataAnalyser(cex_api)
    mda.get_data()
    mda.update_token_info()


if __name__ == '__main__':
    main()
