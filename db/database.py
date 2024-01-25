import psycopg2
import configparser
from io import StringIO
from utils.logger import Logger
from config.settings import Settings
import pandas as pd


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
        self.create_table()

    def _connect(self):
        return psycopg2.connect(**self.connection_params)

    def create_table(self):
        create_table_query = f'''
            CREATE TABLE IF NOT EXISTS prices_8h
    (
        symbol                TEXT                            NOT NULL,
        exchange              TEXT                            NOT NULL,
        timestamp             BIGINT                          NOT NULL, 
        open                  NUMERIC                         NOT NULL,
        high                  NUMERIC                         NOT NULL,
        low                   NUMERIC                         NOT NULL,
        close                 NUMERIC                         NOT NULL,
        vbtc                  NUMERIC                         NOT NULL,    
        usd_ema_12            NUMERIC,
        usd_ema_144           NUMERIC,
        usd_ema_169           NUMERIC,
        usd_ema_576           NUMERIC,
        usd_ema_676           NUMERIC,
        btc_ema_12            NUMERIC,
        btc_ema_144           NUMERIC,
        btc_ema_169           NUMERIC,
        btc_ema_576           NUMERIC,
        btc_ema_676           NUMERIC,
        count                 INT                             NOT NULL
    );
        CREATE INDEX IF NOT EXISTS symbol_time ON prices_8h (timestamp, symbol, exchange);
        CREATE INDEX IF NOT EXISTS ex_symbol_time on prices_8h (exchange, symbol, timestamp);
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
        data = pd.concat(self.cache)
        data['exchange'] = Settings.EXCHANGE
        data.reset_index(inplace=True)
        data.rename(columns={'index': 'timestamp'}, inplace=True)
        data = data[['symbol', 'exchange', 'timestamp', 'open', 'high', 'low', 'close', 'vbtc',
                     'usd_ema_12', 'usd_ema_144', 'usd_ema_169', 'usd_ema_576', 'usd_ema_676',
                     'btc_ema_12', 'btc_ema_144', 'btc_ema_169', 'btc_ema_576', 'btc_ema_676', 'count']]
        with self._connect() as conn:
            with conn.cursor() as cur:
                # 准备一个内存文件对象
                output = StringIO()

                # 将DataFrame写入内存文件对象
                data.to_csv(output, sep='\t', header=False, index=False)
                data.to_csv('out.csv', header=False, index=False)  
                # 移动写指针到开始位置
                output.seek(0)

                # 将数据复制到数据库中
                # 提供目标表名和列名
                copy_sql = f'''
                    COPY prices_8h (
        symbol,
        exchange,
        timestamp,
        open,
        high,
        low,
        close,
        vbtc,    
        usd_ema_12,
        usd_ema_144,
        usd_ema_169,
        usd_ema_576,
        usd_ema_676,
        btc_ema_12,
        btc_ema_144,
        btc_ema_169,
        btc_ema_576,
        btc_ema_676,
        count               
                    )
                    FROM STDIN WITH (FORMAT CSV, DELIMITER '\t', HEADER FALSE);
                '''
                try:
                    cur.copy_expert(sql=copy_sql, file=output)
                except psycopg2.DatabaseError as e:
                    Logger.get_logger().error(f"An error occurred: {e}")

        self.cache = []

    def get_latest_data(self, token_name, num=1):
        conn = self._connect()
        cursor = conn.cursor()

        # 执行查询：假设你的表名是your_table，且有一个自增的ID列
        query = f'''
        SELECT 
        timestamp,
        usd_ema_12,
        usd_ema_144,
        usd_ema_169,
        usd_ema_576,
        usd_ema_676,
        btc_ema_12,
        btc_ema_144,
        btc_ema_169,
        btc_ema_576,
        btc_ema_676,
        count FROM prices_8h 
        WHERE SYMBOL = \'{token_name}\' AND EXCHANGE = \'{Settings.EXCHANGE}\' ORDER BY timestamp DESC LIMIT {num};
        '''

        try:
            cursor.execute(query)
            latest_record = cursor.fetchone()
            return latest_record
        except psycopg2.Error as e:
            Logger.get_logger().error("Unable to retrieve the latest record from the database.")
            Logger.get_logger().error(e)
            return None
        finally:
            cursor.close()
            conn.close()
