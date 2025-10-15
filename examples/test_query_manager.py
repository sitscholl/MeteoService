import logging
import logging.config
from datetime import datetime
import pytz

from webhandler.query_manager import QueryManager
from webhandler.config import load_config
from webhandler.database.db import MeteoDB
from webhandler.provider_manager import ProviderManager

config = load_config("config/config.yaml")

logging.config.dictConfig(config['logging'])
logger = logging.getLogger(__name__)

tz = pytz.timezone("Europe/Rome")
provider_query = 'province'
query_start = datetime(2025,8,25, tzinfo = tz)
query_end = datetime(2025,8,26, tzinfo = tz)
station = "56900MS" #103
variables = None

meteo_db = MeteoDB()
query_manager = QueryManager(config)
provider_manager = ProviderManager(provider_config = config['providers'])

meteo_db.initialize_provider_manager(provider_manager)
query_manager.initialize_provider_manager(provider_manager)

data_query = query_manager.get_data(
    db = meteo_db, 
    provider = provider_query, 
    station_id = station, 
    start_time = query_start, 
    end_time = query_end, 
    variables = variables
    )

print(data_query)
print(data_query.index.duplicated().any())
