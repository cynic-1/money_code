from gecko_api import GeckoAPI
from mexc_exchange_api import MexcExchangeAPI

exchange = MexcExchangeAPI("https://api.mexc.com/api/v3", 1000)
token_list = exchange.get_token_list()
geckoAPI = GeckoAPI()
print(geckoAPI.get_coin_info(token_list))
