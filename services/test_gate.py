import requests


listing_url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
parameters = {
  'start':'1',
  'limit':'5000',
  'convert':'USD',
  'aux': 'num_market_pairs,cmc_rank,date_added,tags,platform,max_supply,circulating_supply,total_supply,market_cap_by_total_supply,volume_24h_reported,volume_7d,volume_7d_reported,volume_30d,volume_30d_reported,is_market_cap_included_in_calc',
  'sort':'market_cap_strict'
}
headers = {
  'Accepts': 'application/json',
  'X-CMC_PRO_API_KEY': 'a854f954-9be5-4b24-8415-fa28f686cff1',
}
coins = requests.get(listing_url, params=parameters, headers=headers).json()

print(coins)