import configparser


class Settings:
    API_URL = "https://api.mexc.com/api/v3/klines"
    API_LIMIT = 1000
    DEFAULT_INTERVAL = "8h"
    DEFAULT_BASE = "USDT"
    CONNECTION_PARAMS = None
    def __init__(self):
        config = configparser.ConfigParser()
        config.read('config.ini')
        self.CONNECTION_PARAMS = {
            'dbname': config['database']['dbname'],
            'user': config['database']['user'],
            'password': config['database']['password'],
            'host': config['database']['host'],
            'port': config['database']['port']
        }
