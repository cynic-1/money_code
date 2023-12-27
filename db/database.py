import psycopg2
import configparser
from io import StringIO
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

    def _connect(self):
        return psycopg2.connect(**self.connection_params)

    def store_data(self, data, table_name):
        with self._connect() as conn:
            with conn.cursor() as cur:
                # 创建表的 SQL 语句，这里需要根据 DataFrame 的结构来定义
                create_table_query = f'''
                CREATE TABLE IF NOT EXISTS "{table_name}" (
                    open_time BIGINT PRIMARY KEY,
                    ema12 NUMERIC,
                    ema144 NUMERIC,
                    ema169 NUMERIC,     
                    ema576 NUMERIC,
                    ema676 NUMERIC               
                );
                '''
                try:
                    cur.execute(create_table_query)
                    conn.commit()
                except psycopg2.DatabaseError as e:
                    print(f"An error occurred: {e}")

            with conn.cursor() as cur:
                # 准备一个内存文件对象
                output = StringIO()

                # 将DataFrame写入内存文件对象
                data.to_csv(output, sep='\t', header=False, index=True)
                # 移动写指针到开始位置
                output.seek(0)

                # 将数据复制到数据库中
                # 提供目标表名和列名
                copy_sql = f'''
                    COPY "{table_name}" (
                        open_time,
                        ema12,
                        ema144,
                        ema169,     
                        ema576,
                        ema676               
                    )
                    FROM STDIN WITH (FORMAT CSV, DELIMITER '\t', HEADER FALSE);
                '''
                try:
                    cur.copy_expert(sql=copy_sql, file=output)
                except psycopg2.DatabaseError as e:
                    print(f"An error occurred: {e}")
