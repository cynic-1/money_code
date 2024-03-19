import requests
from utils.logger import Logger
import time


class GeckoAPI:
    def __init__(self):
        self.base_url = 'https://api.coingecko.com/api/v3'
        self.session = requests.Session()

    def __get_coin_list(self):
        response = self.session.get(self.base_url + '/coins/list')
        response.raise_for_status()
        data = response.json()
        Logger.get_logger().info('Get coingecko coin list.')
        return data

    def __get_coin_map(self):
        data = {}
        coin_list = self.__get_coin_list()
        print(coin_list)
        for coin in coin_list:
            cid = coin['id']
            symbol = coin['symbol'].upper()
            data[symbol] = cid
        return data

    def __get_coin_info(self, ids):
        params = {
            'vs_currency': 'usd',
            'ids': ids,
            'per_page': 50,
            'sparkline': 'false',
            'locale': 'en'
        }
        data = None
        while 1:
            try:
                response = self.session.get(self.base_url + '/coins/markets', params=params)
                response.raise_for_status()
                data = response.json()
                break
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    print("Too many requests. Waiting to retry...")
                    time.sleep(5)
                else:
                    print(f"HTTP error occurred: {e}")

        return data

    def __symbol_to_id_list(self, symbol_list):
        coin_map = self.__get_coin_map()
        data = []
        for symbol in symbol_list:
            try:
                # 尝试从字典中获取值
                coin_id = coin_map[symbol]
                data.append(coin_id)
            except KeyError:
                # 如果键不存在，打印错误消息并执行恢复操作
                print(f"KeyError: '{symbol}' does not exist in coingecko list.")
        return data

    def get_coin_info(self, symbol_list):
        id_list = self.__symbol_to_id_list(symbol_list)
        print(id_list)
        id_num = len(id_list)
        data = []
        for i in range(0, id_num, 50):
            tmp = id_list[i:i+50]
            ids = ','.join(tmp)
            response = self.__get_coin_info(ids)
            data.extend(response)
            time.sleep(1)
        return data
