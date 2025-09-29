from tinyflux import TinyFlux, Point, MeasurementQuery, TagQuery, FieldQuery, TimeQuery
import pandas as pd
from pandas.api.types import is_numeric_dtype

from datetime import datetime, timezone
from numbers import Number
import logging
import pytz
from typing import Optional, Dict, List, Any

logger = logging.getLogger(__name__)

class MeteoDB:
