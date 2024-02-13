import pandas

from utils.logger import Logger
from services.exchange_api import ExchangeAPI
from services.ema_caculator import EmaCaculator
from db.database import Database
from config.settings import Settings
from utils.timestamp import get_current_hour_timestamp_ms


class MarketDataAnalyser:
    def __init__(self, base=Settings.DEFAULT_BASE):
        self.base = base
        self.logger = Logger.get_logger()
        self.exchange_api = ExchangeAPI()
        self.ema_caculator = EmaCaculator(exchangeAPI=self.exchange_api)
        self.database = Database()
        self.token_info = self.exchange_api.get_token_full_name()

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
        cur = get_current_hour_timestamp_ms()
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
                # 加一个小的偏移量是为了避免获得重复数据。
                time = last_ema[0] + 10000

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
    mda = MarketDataAnalyser()
    # mda.get_data()
    mda.update_token_info()


if __name__ == '__main__':
    main()
