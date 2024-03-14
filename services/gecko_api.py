import requests
from utils.logger import Logger


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
        for coin in coin_list:
            cid = coin['id']
            symbol = coin['symbol'].upper()
            data[symbol] = cid
        return data

    def __get_coin_info(self, ids, page):
        params = {
            'vs_currency': 'usd',
            'ids': ids,
            'per_page': 250,
            'page': page,
            'sparkline': 'false',
            'locale': 'en'
        }

        response = self.session.get(self.base_url + '/coins/markets', params=params)
        response.raise_for_status()
        data = response.json()
        Logger.get_logger().info(f'Get coingecko coin markets data page {page}.')
        return data

    def __symbol_to_id_list(self, symbol_list):
        coin_map = self.__get_coin_map()
        data = []
        for symbol in symbol_list:
            data.append(coin_map[symbol])

        return data

    def get_coin_info(self, symbol_list):
        id_list = self.__symbol_to_id_list(symbol_list)
        page_num = len(id_list) // 250
        ids = ','.join(id_list)
        data = []
        for i in range(1, page_num+1):
            response = self.__get_coin_info(ids, i)
            data.extend(response)
        return data
