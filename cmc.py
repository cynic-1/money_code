from db.database import Database
from services.cmc_api import get_market_data
import pandas

db = Database()

market_data = get_market_data()
data = pandas.DataFrame(market_data['data'])
db.write_to_cmc(data)
