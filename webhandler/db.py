from tinyflux import TinyFlux, Point, MeasurementQuery, TagQuery, FieldQuery, TimeQuery
import pandas as pd

from datetime import datetime
from numbers import Number
import logging
import pytz

logger = logging.getLogger(__name__)

TIMEZONE = 'Europe/Rome'

class MeteoDB:

    def __init__(self, path: str):
        self.db = TinyFlux(path)

    def insert_data(self, data: pd.DataFrame, measurement: str, tags: dict, fields: list[str] | None = None, skip_existing: bool = False):

        if data is not None and len(data) > 0:
            for _, row in data.iterrows():

                if 'datetime' not in row or pd.isnull(row['datetime']):
                    raise ValueError("Missing or null 'time' field in the data row")

                if not isinstance(row['datetime'], datetime):
                    raise ValueError(f"'datetime' field must be a datetime object, got {type(row['datetime'])} instead")

                query = self._build_query(measurement, row['datetime'], row['datetime'], tags)
                if self.db.contains(query) and not skip_existing:
                    raise ValueError("Duplicate entry found in the database: Cannot append.")
                elif self.db.contains(query):
                    logger.info(f'Row with datetime {row['datetime']} and tags {tags} already exists. Skipping')
                    continue

                if fields is None:
                    fields = {col: row[col] for col in row.index if col not in ['datetime']}
                else:
                    fields = {col: row[col] for col in fields}

                for key, value in fields.copy().items():
                    if value is not None and not isinstance(value, Number):
                        logger.warning(f"Field '{key}' must be a numeric value or None, got {type(value)} instead. Will not be added to database")
                        fields.pop(key)

                point = Point(
                    time=row['datetime'],
                    measurement=measurement,
                    tags=tags,
                    fields=fields
                )
                self.db.insert(point)
        else:
            logger.warning('No data provided to insert_data method.')

    def _build_query(self, measurement: str, start_time: datetime, end_time: datetime, tags: dict = None):
        measurement_query = MeasurementQuery()
        measurement_query = measurement_query == measurement

        if start_time.tzinfo is None or end_time.tzinfo is None:
            raise ValueError("start_time and end_time must have timezone information.")

        time_query = TimeQuery()
        time_query = (time_query >= start_time) & (time_query <= end_time)

        tag_query = TagQuery()
        if tags:
            for i, (key, value) in enumerate(tags.items()):
                if i == 0:
                    tag_query_combined = (tag_query[key] == value)
                else:
                    tag_query_combined &= (tag_query[key] == value)

            return measurement_query & time_query & tag_query_combined

        else:
            return measurement_query & time_query

    def query_data(self, measurement: str, start_time: datetime, end_time: datetime, tags: dict = None) -> pd.DataFrame:
        query = self._build_query(measurement, start_time, end_time, tags)
        results = self.db.search(query)
        
        index, values = [], []
        for result in results:
            index.append(result.time)

            result_fields = result.fields
            result_fields['station_id'] = result.tags['station_id']
            values.append(result_fields)

        return pd.DataFrame(data = values, index = index)

if __name__ == '__main__':

    from webhandler.meteo.SBR import SBR
    import argparse

    db = MeteoDB('db/db.csv')

    parser = argparse.ArgumentParser()
    parser.add_argument('username', help = 'username')
    parser.add_argument('password', help = 'password')
    args = parser.parse_args()

    with SBR(args.username, args.password) as client:
        data = client.run(
            station_id="103",
            start=datetime(2025, 9, 1, 0, 0),
            end=datetime(2025, 9, 2, 0, 0),
            type = 'meteo',
            drop_columns = True
        )

    db.insert_data(data, measurement="SBR", tags={"station_id": "103", "type": "meteo", "source": "SBR"}, skip_existing = True)

    tz = pytz.timezone(TIMEZONE)
    start = datetime(2025, 9, 1, 0, 0, tzinfo = tz)
    end = datetime(2025, 9, 9, 14, 0, tzinfo=tz)
    query_result = db.query_data(measurement="SBR", start_time=start, end_time=end, tags = {'station_id': '103'})

    print(query_result.dtypes)
