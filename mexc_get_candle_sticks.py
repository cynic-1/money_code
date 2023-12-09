# coding: utf-8
import json

import requests
import time
import pandas as pd
import matplotlib.pyplot as plt
import os

# 绘制滤波前后的时间序列图
plt.rcParams['font.sans-serif'] = ['SimHei']  # 设置中文字体为宋体或其他已安装的字体
plt.rcParams['axes.unicode_minus'] = False  # 解决保存图像是负号'-'显示为方块的问题

PATH = 'D:/All About Learning/Quant/mexc/'

host = "https://api.mexc.com"
prefix = "/api/v3"
headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}


def exist_file(file_name):
    return os.path.isfile(f'{PATH}{file_name}.csv')


def get_candle_sticks(pair: str, after: str = '', before: str = '', interval: str = '8h'):
    url = '/klines'
    query_param = f'symbol={pair}&interval={interval}'
    if after != '':
        query_param += f'&startTime={after}'
    if before != '':
        query_param += f'&endTime={before}'

    r = requests.request('GET', host + prefix + url + "?" + query_param, headers=headers)
    return r.json()


def get_current_timestamp():
    timestamp = int(time.time())
    hour = timestamp // 3600 * 3600 * 1000
    return hour


def get_all_candle_sticks(pair, latest, interval='8h'):
    candle_sticks = []
    end = latest
    while True:
        start = end-500*8*3600*1000
        try:
            tmp = get_candle_sticks(pair, after=start, before=end, interval=interval)
            print(tmp[0][0])
            n_entries = len(tmp)
            print(n_entries)
            candle_sticks += tmp
            end = start
            if n_entries < 500:
                break
            time.sleep(0.2)
        except Exception as e:
            time.sleep(1)
    return candle_sticks


with open(f'{PATH}pairs.json', 'r') as f:
    pairs = json.load(f)

cur_time = get_current_timestamp()
for p in pairs:
    if exist_file(p):
        print(f'Exist {p}')
        continue
    candles = get_all_candle_sticks(p, cur_time)
    df = pd.DataFrame(candles)
    df.columns = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'turnover']
    df.to_csv(f'{PATH}{p}.csv', index=False)
    print(f'Finish {p}!')