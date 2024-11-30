import json

import psycopg2
import configparser
from io import StringIO
from utils.logger import Logger
from config.settings import Settings
import pandas as pd
import numpy as np


class Database:
    def __init__(self):
        config = configparser.ConfigParser()
        config.read('config/config.ini')
        self.connection_params = {
            'dbname': config['database']['dbname'],
            'user': config['database']['user'],
            'password': config['database']['password'],
            'host': config['database']['host'],
            'port': config['database']['port']
        }
        self.cache = []
        # self.create_table()
        # self.create_token_info_table()
        # self.create_cmc_table()

    def _connect(self):
        return psycopg2.connect(**self.connection_params)

    def create_table(self):
        create_table_query = f'''
            CREATE TABLE IF NOT EXISTS prices_1d
    (
        symbol                TEXT                            NOT NULL,
        exchange              TEXT                            NOT NULL,
        timestamp             BIGINT                          NOT NULL, 
        open                  NUMERIC                         NOT NULL,
        high                  NUMERIC                         NOT NULL,
        low                   NUMERIC                         NOT NULL,
        close                 NUMERIC                         NOT NULL,
        vbtc                  NUMERIC                         NOT NULL,    
        usd_ema_20            NUMERIC,
        usd_ema_200           NUMERIC,
        btc_ema_20            NUMERIC,
        btc_ema_200           NUMERIC,
        count                 INT                             NOT NULL
    );
        CREATE INDEX IF NOT EXISTS symbol_time_1d ON prices_1d (timestamp, symbol, exchange);
        CREATE INDEX IF NOT EXISTS ex_symbol_time_1d on prices_1d (exchange, symbol, timestamp);
        '''

        with self._connect() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(create_table_query)
                    conn.commit()
                    Logger.get_logger().info('Create Table.')
                except psycopg2.DatabaseError as e:
                    Logger.get_logger().error(f"CREATE TABLE: {e}")

    def commit_to_write_cache(self, data, symbol):
        data['symbol'] = symbol
        self.cache.append(data)

    def write_data(self):
        if self.cache == []:
            return
        data = pd.concat(self.cache)
        self.cache = []
        data['exchange'] = Settings.EXCHANGE
        data.reset_index(inplace=True)
        data.rename(columns={'index': 'timestamp'}, inplace=True)

        # 处理空值
        numeric_columns = ['open', 'high', 'low', 'close', 'vbtc',
                        'usd_ema_20', 'usd_ema_200',
                        'btc_ema_20', 'btc_ema_200', 'count']
        
        # 将空字符串和NaN替换为0或NULL
        for col in numeric_columns:
            data[col] = data[col].replace('', None)
            data[col] = pd.to_numeric(data[col], errors='coerce')  # 将无效数值转换为NaN

        data = data[['symbol', 'exchange', 'timestamp', 'open', 'high', 'low', 'close', 'vbtc',
                     'usd_ema_20', 'usd_ema_200',
                     'btc_ema_20', 'btc_ema_200', 'count']]

        # 使用NA表示来写入CSV，这样PostgreSQL会将其识别为NULL
        data.to_csv('out.csv', header=False, index=False, na_rep='\\N')

        with self._connect() as conn:
            with conn.cursor() as cur:
                try:
                    with open('out.csv', 'r') as f:
                        cur.copy_from(f, "prices_1d", sep=',', null='\\N')
                    conn.commit()
                    Logger.get_logger().info("Write to Database.")
                except psycopg2.DatabaseError as e:
                    Logger.get_logger().error(f"An error occurred: {e}")

    def get_latest_data_by_symbol(self, symbol, num=1):
        conn = self._connect()
        cur = conn.cursor()

        # 执行查询：假设你的表名是your_table，且有一个自增的ID列
        query = f'''
        SELECT 
        timestamp,
        usd_ema_20,
        usd_ema_200,
        btc_ema_20,
        btc_ema_200,
        count FROM prices_1d 
        WHERE SYMBOL = \'{symbol}\' AND EXCHANGE = \'{Settings.EXCHANGE}\' ORDER BY timestamp DESC LIMIT {num};
        '''

        try:
            cur.execute(query)
            latest_record = cur.fetchone()
            return latest_record
        except psycopg2.Error as e:
            Logger.get_logger().error("Unable to retrieve the latest record from the database.")
            Logger.get_logger().error(e)
            return None
        finally:
            cur.close()
            conn.close()

    def create_token_info_table(self):
        create_table_query = f'''
            CREATE TABLE IF NOT EXISTS token_info
    (
        symbol                TEXT                            NOT NULL,
        exchange              TEXT                            NOT NULL,
        full_name             TEXT                            NOT NULL, 
        latest_timestamp      BIGINT                          NOT NULL,
        CONSTRAINT unique_symbol_exchange UNIQUE (symbol, exchange)
    );
        CREATE INDEX IF NOT EXISTS symbol_ex ON token_info (symbol, exchange);
        CREATE INDEX IF NOT EXISTS ex_symbol on token_info (exchange, symbol);
        '''

        with self._connect() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(create_table_query)
                    conn.commit()
                    Logger.get_logger().info('Create Token Info Table.')
                except psycopg2.DatabaseError as e:
                    Logger.get_logger().error(f"CREATE TABLE: {e}")

    def write_to_token_info(self, data):
        columns = data.columns.tolist()
        columns_str = ', '.join(columns)

        insert_query = f"""
            INSERT INTO TOKEN_INFO ({columns_str})
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (symbol, exchange) DO UPDATE SET 
            latest_timestamp = EXCLUDED.latest_timestamp;
        """

        conn = self._connect()
        cur = conn.cursor()

        for index, row in data.iterrows():
            if pd.isnull(row['latest_timestamp']):
                continue
            try:
                cur.execute(insert_query, tuple(row))
            except psycopg2.Error as e:
                Logger.get_logger().error(f"Update token info: {e}, row: {row}")
                return
        conn.commit()
        Logger.get_logger().info("Update token info.")

        cur.close()
        conn.close()

    def create_cmc_table(self):
        create_table_query = f'''
            CREATE TABLE IF NOT EXISTS cmc
    (
        id                    BIGINT                          NOT NULL,
        name                  TEXT                            NOT NULL,
        symbol                TEXT                            PRIMARY KEY,
        slug                  TEXT                          NOT NULL,
        num_market_pairs      INT,
        date_added            TEXT,
        tags                  TEXT,
        max_supply            NUMERIC,
        circulating_supply    NUMERIC,
        total_supply          NUMERIC,
        platform              JSON,
        is_market_cap_included_in_calc  INT,
        infinite_supply       BOOLEAN,
        cmc_rank              INT,
        self_reported_circulating_supply    NUMERIC,
        self_reported_market_cap            NUMERIC,
        tvl_ratio                           NUMERIC,
        last_updated                        TIMESTAMP,
        quote                               JSON

    );
    CREATE INDEX IF NOT EXISTS symbol_index ON cmc (symbol);
        '''

        with self._connect() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(create_table_query)
                    conn.commit()
                    Logger.get_logger().info('Create cmc Table.')
                except psycopg2.DatabaseError as e:
                    Logger.get_logger().error(f"CREATE cmc TABLE: {e}")

    def write_to_cmc(self, data):
        columns = data.columns.tolist()
        columns_str = ', '.join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        update_columns = [f"{col} = EXCLUDED.{col}" for col in columns if col != 'symbol']
        update_columns_str = ", ".join(update_columns)  # 更新时的列赋值字符串

        data['platform'] = data['platform'].apply(json.dumps)
        data['quote'] = data['quote'].apply(json.dumps)

        insert_query = f"""
            INSERT INTO cmc ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT (id) DO UPDATE SET 
            {update_columns_str}
        """

        conn = self._connect()
        cur = conn.cursor()

        data = [tuple(x) for x in data.to_numpy()]

        cur.executemany(insert_query, data)
        conn.commit()
        Logger.get_logger().info("Update cmc table.")

        cur.close()
        conn.close()