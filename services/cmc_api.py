import requests


def get_market_data():
    listing_url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    parameters = {
        'start': '1',
        'limit': '5000',
        'convert': 'USD',
        'aux': 'num_market_pairs,cmc_rank,date_added,tags,platform,max_supply,circulating_supply,total_supply,market_cap_by_total_supply,volume_24h_reported,volume_7d,volume_7d_reported,volume_30d,volume_30d_reported,is_market_cap_included_in_calc',
        'sort': 'market_cap_strict'
    }
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': 'fe572e03-173b-43a8-84ac-4a9b4b2b23b5',
    }
    coins = requests.get(listing_url, params=parameters, headers=headers).json()
    return coins
