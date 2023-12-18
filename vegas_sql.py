# coding: utf-8
import json
import requests
import time
import pandas as pd
import psycopg2

PATH = 'D:/All About Learning/Quant/mexc'

host = "https://api.mexc.com"
prefix = "/api/v3"
headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}


def get_current_timestamp():
    timestamp = int(time.time())
    hour = timestamp // 3600 * 3600 * 1000
    return hour


def get_candle_sticks(pair: str, after: str = '', before: str = '', interval: str = '8h'):
    url = '/klines'
    query_param = f'symbol={pair}&interval={interval}'
    if after != '':
        query_param += f'&startTime={after}'
    if before != '':
        query_param += f'&endTime={before}'

    r = requests.request('GET', host + prefix + url + "?" + query_param, headers=headers)
    return r.json()


def get_btc_candles():
    btc_list = get_candle_sticks('BTCUSDT')
    btc = pd.DataFrame(btc_list)
    btc.columns = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'turnover']
    btc.set_index('open_time', inplace=True)
    return btc


def update_EMA(span, pre, new):
    return new * 2 / (1 + span) + pre * (1 - 2 / (1 + span))


def update(token, btc_df, conn):
    print(f'Processing {token}')

    # 创建一个 cursor 对象用来执行 SQL 查询
    cur = conn.cursor()

    # 编写 SQL 查询，获取 open_time 最大的行
    query = f'SELECT * FROM "{token}" ORDER BY open_time DESC LIMIT 1;'

    try:
        # 执行 SQL 查询
        cur.execute(query)

        # 获取查询结果
        row = cur.fetchone()
        if not row:
            print("No rows found.")
            return
        print(row)
        start_time, MA12, MA144, MA169, MA576, MA676 = row
        now = get_current_timestamp()
        if start_time + 8 * 60 * 60 * 1000 > now:
            print('Already the latest data')
            return
        new_data = get_candle_sticks(f'{token}USDT', after=start_time, before=now)
        try:
            for open_time, open_price, *_ in new_data:
                open_price = float(open_price)
                btc_price = float(btc_df.loc[open_time]['open'])
                vbtc = open_price / btc_price
                MA12 = update_EMA(12, MA12, vbtc)
                MA144 = update_EMA(144, MA144, vbtc)
                MA169 = update_EMA(169, MA169, vbtc)
                MA576 = update_EMA(576, MA576, vbtc)
                MA676 = update_EMA(676, MA676, vbtc)

                cur.execute(
                    f'INSERT INTO "{token}" (open_time, "MA12", "MA144", "MA169", "MA576", "MA676") VALUES ({open_time}, {MA12}, {MA144}, {MA169}, {MA576}, {MA676});')
                print(f'Add timestamp {open_time} data')
            conn.commit()
            cur.close()
            # 可能没更新
        except KeyError as e:
            print(f"btc price not exist")

    except psycopg2.Error as e:
        # 打印数据库错误信息
        print("Database error:", e)
    finally:
        # 关闭 cursor 和连接
        cur.close()


def __main__():
    btc_df = get_btc_candles()

    with open(f'{PATH}/tokens.json', 'r') as f:
        tokens = json.load(f)

    # 数据库连接参数
    conn_params = {
        'dbname': 'mexc_vegas',
        'user': 'postgres',
        'password': 'postgres2785',
        'host': 'localhost',
        'port': '5432'
    }

    # 连接到数据库
    conn = psycopg2.connect(**conn_params)

    for tk in tokens:
        update(tk, btc_df=btc_df, conn=conn)


__main__()
