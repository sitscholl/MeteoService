from tinyflux import TinyFlux, Point, MeasurementQuery, TagQuery, FieldQuery, TimeQuery
import pandas as pd
from pandas.api.types import is_numeric_dtype

from datetime import datetime
from numbers import Number
import logging
import pytz
from typing import Optional, Dict, List, Any

logger = logging.getLogger(__name__)

class MeteoDB:

    def __init__(self, path: str):
        self.db_path = path
        self.db = None

        logger.debug(f"Initialized MeteoDB with path: {self.db_path}")

    def __enter__(self):
        try:
            self._connect()
        except Exception as e:
            raise ValueError(f"Cannot connect to the database at {self.db_path}: {e}")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            self._disconnect()
        except Exception as e:
            logger.error(f"Error during disconnection from database at {self.db_path}: {e}")

    def _connect(self):
        self.db = TinyFlux(self.db_path)
        logger.debug(f"Connected to database at {self.db_path}")

    def _disconnect(self):
        if self.db is not None:
            self.db.close()
            self.db = None
        logger.debug(f"Disconnected from database at {self.db_path}")

    def insert_data(self,
                   data: pd.DataFrame,
                   provider: str,
                   tags: Dict[str, Any],
                   fields: Optional[List[str]] = None,
                   skip_existing: bool = False):
        """
        Insert DataFrame data into the database.

        Args:
            data: DataFrame with 'datetime' column and provider fields
            provider: provider name (e.g., 'SBR')
            tags: Tags to apply to all points (e.g., {'station_id': '103'})
            fields: Specific fields to include (None = all except datetime)
            skip_existing: Skip duplicate entries instead of raising error

        Returns:
            Dict with statistics: {'inserted': int, 'skipped': int, 'errors': int}
        """

        if self.db is None:
            raise ValueError("Database connection is not established")

        if tags is not None:
            tags = self._check_tags(tags)
        
        stats = {'inserted': 0, 'skipped': 0, 'errors': 0}

        if data is None or len(data) == 0:
            logger.warning('No data provided to insert_data method.')
            return stats

        # Validate required columns
        if 'datetime' not in data.columns:
            raise ValueError("DataFrame must contain 'datetime' column")

        # Ensure datetime column is timezone-aware
        data = self._ensure_timezone_aware(data.copy())

        # Determine fields to include
        field_columns = self._get_field_columns(data, fields)

        for idx, row in data.iterrows():
            try:
                if pd.isnull(row['datetime']):
                    logger.warning(f"Skipping row {idx}: missing datetime")
                    stats['errors'] += 1
                    continue
                ts = row['datetime']

                # Check for duplicates
                exists = False
                try:
                    exists = self._point_exists(provider, ts, tags)
                except Exception as e:
                    logger.error(f"Error checking existing point for row {idx}, datetime {ts}: {e}")
                    stats['errors'] += 1
                    if skip_existing:
                        # If we can't verify existence, skip to avoid accidental duplicates
                        logger.warning(f"Skipping row {idx} due to existence check failure")
                        continue
                    else:
                        raise

                if skip_existing and exists:
                    logger.debug(f'Row with datetime {ts} already exists. Skipping')
                    stats['skipped'] += 1
                    continue
                elif not skip_existing and exists:
                    raise ValueError(f"Duplicate entry found for datetime {ts}")

                # Prepare fields
                point_fields = self._prepare_fields(row, field_columns)

                if not point_fields:  # Skip if no valid fields
                    logger.warning(f"Skipping row {idx}: no valid numeric fields")
                    stats['errors'] += 1
                    continue

                point = Point(
                    time=row['datetime'],
                    measurement=provider,
                    tags=tags.copy(),
                    fields=point_fields
                )
                self.db.insert(point)
                stats['inserted'] += 1

            except Exception as e:
                logger.error(f"Error processing row {idx}: {e}")
                stats['errors'] += 1
                continue

        logger.info(f"Insert completed: {stats}")
        return stats

    def _ensure_timezone_aware(self, data: pd.DataFrame) -> pd.DataFrame:
        """Ensure datetime column is timezone-aware."""
        if data['datetime'].dt.tz is None:
            data['datetime'] = data['datetime'].dt.tz_localize("UTC")
            logger.warning("Localized naive datetimes to UTC timezone")
        return data

    def _get_field_columns(self, data: pd.DataFrame, fields: Optional[List[str]], exclude_fields = ['datetime', 'station_id']) -> List[str]:
        """Get list of field columns to include."""
        if fields is None:
            fields_cleaned = [col for col in data.columns if col not in exclude_fields]
        else:
            fields_cleaned = [col for col in fields if col not in exclude_fields]
            # Validate that specified fields exist
            missing_fields = set(fields) - set(data.columns)
            if missing_fields:
                raise ValueError(f"Specified fields not found in data: {missing_fields}")
        
        fields_dropped = []
        for f in fields_cleaned:
            if not is_numeric_dtype(data[f]):
                logger.warning(f"Column {f} is not numeric. Ignoring column.")
            else:
                fields_dropped.append(f)
        return fields_dropped

    def _prepare_fields(self, row: pd.Series, field_columns: List[str]) -> Dict[str, Any]:
        """Prepare and validate field values for a single row."""
        prepared_fields = {}

        for col in field_columns:
            value = row[col]

            # Validate numeric values
            if not isinstance(value, Number) and value is not None:
                logger.warning(f"Field '{col}' must be numeric or None, got {type(value)}. Skipping.")
                continue

            prepared_fields[col] = value

        return prepared_fields

    def _point_exists(self, provider: str, timestamp: datetime, tags: Dict[str, Any]) -> bool:
        """Check if a point already exists for the given provider, time, and tags."""
        query = self._build_query(provider, timestamp, timestamp, tags)
        return self.db.contains(query)

    def _check_tags(self, tags: Dict[str, Any]) -> None:
        """Validate and sanitize tags."""
        tags_new = tags.copy()
        for key, value in tags.items():
            if not isinstance(value, str):
                logger.warning(f"Tag {key} must be a string, got {value} (type: {type(value)})")
                tags_new[key] = str(value)
        return tags_new


    def _build_query(self, provider: str, start_time: datetime, end_time: datetime, tags: Optional[Dict[str, Any]] = None):
        """Build a TinyFlux query with improved error handling."""
        # Validate timezone awareness
        for time_val, name in [(start_time, 'start_time'), (end_time, 'end_time')]:
            if time_val.tzinfo is None:
                raise ValueError(f"{name} must have timezone information.")

        # Build provider query
        measurement_query = MeasurementQuery() == provider

        # Build time query
        time_query = (TimeQuery() >= start_time) & (TimeQuery() <= end_time)

        # Build tag query
        if tags:
            tag_query = TagQuery()
            tag_conditions = [tag_query[key] == value for key, value in tags.items()]

            # Combine tag conditions
            combined_tag_query = tag_conditions[0]
            for condition in tag_conditions[1:]:
                combined_tag_query &= condition

            return measurement_query & time_query & combined_tag_query
        else:
            return measurement_query & time_query

    def query_data(self,
                  provider: str,
                  start_time: datetime,
                  end_time: datetime,
                  tags: Optional[Dict[str, Any]] = None,
                  fields: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Query data with improved error handling and optional field filtering.

        Args:
            provider: provider name to query
            start_time: Start time (timezone-aware)
            end_time: End time (timezone-aware)
            tags: Optional tags to filter by
            fields: Optional list of fields to include in result

        Returns:
            DataFrame with datetime index and requested fields
        """
        if self.db is None:
            raise ValueError("Database connection is not established")

        if tags is not None:
            tags = self._check_tags(tags)

        try:
            query = self._build_query(provider, start_time, end_time, tags)
            results = self.db.search(query)

            if not results:
                logger.info(f"No data found for query: provider={provider}, "
                          f"time_range={start_time} to {end_time}, tags={tags}")
                return pd.DataFrame()

            # Process results
            data_rows = []
            timestamps = []

            for result in results:
                timestamps.append(result.time)

                # Combine fields and tags
                row_data = result.fields.copy()

                # Safely add tags to row data
                for tag_key, tag_value in result.tags.items():
                    row_data[tag_key] = tag_value

                # Filter fields if specified
                if fields:
                    row_data = {k: v for k, v in row_data.items() if k in fields or k in result.tags}

                data_rows.append(row_data)

            # Create DataFrame
            df = pd.DataFrame(data=data_rows, index=pd.DatetimeIndex(timestamps, name='datetime'))

            # Sort by datetime
            df = df.sort_index()

            logger.debug(f"Query returned {len(df)} rows")
            return df

        except Exception as e:
            logger.error(f"Query failed: {e}")
            raise

    def get_providers(self) -> List[str]:
        """Get list of all providers in the database."""
        if self.db is None:
            raise ValueError("Database connection is not established")

        try:
            all_points = self.db.all()
            providers = list(set(point.provider for point in all_points))
            return sorted(providers)
        except Exception as e:
            logger.error(f"Failed to get providers: {e}")
            return []

    def get_tags_for_provider(self, provider: str) -> Dict[str, List[Any]]:
        """Get all unique tag values for a provider (cached)."""
        if self.db is None:
            raise ValueError("Database connection is not established")

        try:
            query = MeasurementQuery() == provider
            points = self.db.search(query)

            tag_values = {}
            for point in points:
                for tag_key, tag_value in point.tags.items():
                    if tag_key not in tag_values:
                        tag_values[tag_key] = set()
                    tag_values[tag_key].add(tag_value)
            # Convert sets to sorted lists
            return {k: sorted(list(v)) for k, v in tag_values.items()}

        except Exception as e:
            logger.error(f"Failed to get tags for provider {provider}: {e}")
            return {}

if __name__ == '__main__':

    from webhandler.meteo.SBR import SBR
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('username', help = 'username')
    parser.add_argument('password', help = 'password')
    args = parser.parse_args()

    tz = pytz.timezone("Europe/Rome")

    with SBR(args.username, args.password) as client:
        data = client.run(
            station_id="103",
            start=datetime(2025, 9, 1, 0, 0),
            end=datetime(2025, 9, 2, 0, 0),
            type = 'meteo',
            drop_columns = True
        )

    with MeteoDB('db/db.csv', timezone = tz) as db:

        insert_stats = db.insert_data(
            data, 
            provider="SBR", 
            tags={"station_id": "103", "type": "meteo", "source": "SBR"}, 
            skip_existing = True
            )
            
        print(insert_stats)

        
        start = datetime(2025, 9, 1, 0, 0, tzinfo = tz)
        end = datetime(2025, 9, 9, 14, 0, tzinfo=tz)
        query_result = db.query_data(provider="SBR", start_time=start, end_time=end, tags = {'station_id': '103'})

        print(query_result.dtypes)
        print(query_result.head())
