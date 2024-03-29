import psycopg2
import configparser
from io import StringIO
from utils.logger import Logger
from config.settings import Settings


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
        btc_ema_676           NUMERIC
    );
        CREATE INDEX IF NOT EXISTS symbol_time ON prices_8h (timestamp, symbol);
        '''

        with self._connect() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(create_table_query)
                    conn.commit()
                    Logger.get_logger().info('Create Table.')
                except psycopg2.DatabaseError as e:
                    Logger.get_logger().error(f"CREATE TABLE: {e}")

    def store_data(self, data, symbol):
        data['symbol'] = symbol
        data['exchange'] = Settings.EXCHANGE
        data.reset_index(inplace=True)
        data.rename(columns={'index': 'timestamp'}, inplace=True)
        data = data[['symbol', 'exchange', 'timestamp', 'open', 'high', 'low', 'close', 'vbtc',
                     'usd_ema_12', 'usd_ema_144', 'usd_ema_169', 'usd_ema_576', 'usd_ema_676',
                     'btc_ema_12', 'btc_ema_144', 'btc_ema_169', 'btc_ema_576', 'btc_ema_676']]
        with self._connect() as conn:
            with conn.cursor() as cur:
                # 准备一个内存文件对象
                output = StringIO()

                # 将DataFrame写入内存文件对象
                data.to_csv(output, sep='\t', header=False, index=False)
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
        btc_ema_676               
                    )
                    FROM STDIN WITH (FORMAT CSV, DELIMITER '\t', HEADER FALSE);
                '''
                try:
                    cur.copy_expert(sql=copy_sql, file=output)
                except psycopg2.DatabaseError as e:
                    Logger.get_logger().error(f"An error occurred: {e}")

    def get_latest_data(self, token_name):
        conn = self._connect()
        cursor = conn.cursor()

        # 执行查询：假设你的表名是your_table，且有一个自增的ID列
        query = f'SELECT * FROM "{token_name}" ORDER BY timestamp DESC LIMIT 1;'

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

    def delete_data(self, token_name, start, end):
        conn = self._connect()
        cursor = conn.cursor()

        query = f'DELETE FROM "{token_name}" WHERE timestamp BETWEEN {start} AND {end};'

        try:
            cursor.execute(query)
            # 提交事务
            conn.commit()
            Logger.get_logger().error(f"Deleted {cursor.rowcount} rows successfully from table {token_name}.")
        except psycopg2.Error as e:
            Logger.get_logger().error("Unable to delete the record.")
            Logger.get_logger().error(e)
        finally:
            cursor.close()
            conn.close()
