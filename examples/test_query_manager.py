import logging
import logging.config
from datetime import datetime, timezone
import pytz

from webhandler.query_manager import QueryManager
from webhandler.config import load_config
from webhandler.db import MeteoDB

config = load_config("config/config.yaml")

logging.config.dictConfig(config['logging'])
logger = logging.getLogger(__name__)

tz = pytz.timezone("Europe/Rome")
provider_query = 'SBR'
query_start = datetime(2025,8,25, tzinfo = tz)
query_end = datetime(2025,8,26, tzinfo = tz)
station = 103
fields = None


query_manager = QueryManager(config)

with MeteoDB('db/db_test.csv') as db:
    data_query = query_manager.get_data(
        db = db, 
        provider = provider_query, 
        start_time = query_start, 
        end_time = query_end, 
        tags = {'station_id': station}, 
        fields = fields
    )

print(data_query)
print(data_query.index.duplicated().any())
