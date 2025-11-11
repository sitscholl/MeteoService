import pandas as pd
from pandas.tseries.frequencies import to_offset

import logging
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple, Any

from webhandler.database.db import MeteoDB
from webhandler.provider_manager import ProviderManager
from webhandler.utils import derive_datetime_gaps

logger = logging.getLogger(__name__)

class QueryManager:
    """Orchestrates data fetching from database and external providers."""
    
    def __init__(self, config: dict[str, Any], provider_manager: ProviderManager = None):
        """Initialize DataManager with configuration."""
        self.config = config
        self.provider_manager = provider_manager

        
    def _find_data_gaps(self, existing_data: pd.DataFrame, start_time: datetime,
                        end_time: datetime, freq: str, inclusive = 'right', min_gap_duration: str = '30min') -> List[Tuple[datetime, datetime]]:
        """Find gaps in the database data for the requested time range."""
        try:
            min_gap_duration = pd.Timedelta(min_gap_duration)

            if existing_data.empty:
                # No data exists, entire range is a gap
                return [(start_time, end_time)]

            try:
                 # Ensure timezone consistency between input times and existing data
                if existing_data.index.tz is None:
                    existing_data_tz_converted = existing_data.copy()
                    existing_data_tz_converted.index = existing_data_tz_converted.index.tz_localize('UTC').tz_convert(start_time.tzinfo)
                elif existing_data.index.tz != start_time.tzinfo:
                    existing_data_tz_converted = existing_data.copy()
                    existing_data_tz_converted.index = existing_data_tz_converted.index.tz_convert(start_time.tzinfo)
                else:
                    existing_data_tz_converted = existing_data

                # Align start and end dates with timeseries
                start_time_aligned = pd.Timestamp(start_time).floor(freq)
                end_time_aligned = pd.Timestamp(end_time).ceil(freq)

                complete_ts = pd.date_range(start=start_time_aligned, end=end_time_aligned, freq=freq, inclusive = inclusive)
                missing_ts = [ts for ts in complete_ts if ts not in existing_data_tz_converted.index]

                gaps = derive_datetime_gaps(missing_ts, freq = freq)
                gaps = [(s, e) for s, e in gaps if (e - s) >= min_gap_duration]
                
                return gaps
            except Exception as e:
                logger.warning(f'Unable to determine timeseries gaps: {e}')
                return [(start_time, end_time)]

        except Exception as e:
            logger.error(f"Error finding data gaps: {e}")
            return [(start_time, end_time)]  # Return full range as gap on error
    
    def _fetch_missing_data(self, provider_name: str, station_id: str,
                           gaps: List[Tuple[datetime, datetime]]) -> pd.DataFrame:
        """Fetch missing data from the specified provider."""
        if not gaps:
            return pd.DataFrame()
        
        all_data = []

        if not hasattr(self, 'provider_manager'):
            logger.warning("No provider_manager initialized for query_manager. Cannot fetch missing data")
            return pd.DataFrame()

        # Find which provider to use
        if self.provider_manager.get_provider(provider_name.lower()) is None:
            logger.info(f"No provider available for provider: {provider_name}. Missing data cannot be requested.")
            return pd.DataFrame()
        
        try:
            with self.provider_manager.get_provider(provider_name.lower()) as provider:
                for start_gap, end_gap in gaps:
                    # Check if gap is within reasonable limits
                    gap_days = (end_gap - start_gap).days
                    max_gap_days = self.config.get('settings', {}).get('max_gap_days', 30)
                    
                    if max_gap_days > 0 and gap_days > max_gap_days:
                        logger.warning(f"Gap of {gap_days} days exceeds maximum of {max_gap_days} days. Skipping.")
                        continue
                    
                    logger.info(f"Fetching data from {provider_name} for {start_gap} to {end_gap}")
                    
                    # Fetch raw data
                    provider_data = provider.run(
                        start=start_gap,
                        end=end_gap,
                        data_type = 'meteo',
                        station_id = station_id
                    )
                    
                    if not provider_data.empty:
                        all_data.append(provider_data)
        
        except Exception as e:
            logger.error(f"Error fetching data from {provider_name}: {e}")
            return pd.DataFrame()
        
        if all_data:
            return pd.concat(all_data)
        return pd.DataFrame()
    
    def get_data(
            self, 
            db: MeteoDB, 
            provider: str, 
            station_id: str,
            start_time: datetime,
            end_time: datetime, 
            variables: Optional[List[str]] = None
        ) -> pd.DataFrame:
        """
        Get data from database and fetch missing data from providers if needed.

        Args:
            db: Database instance
            provider: provider name (corresponds to provider name in config.yaml --> providers)
            start_time: Start time for data query (must be timezone-aware)
            end_time: End time for data query (must be timezone-aware)
            variables: Optional list of variables to return

        Returns:
            Complete dataset combining database and newly fetched data
        """

        #Filtering by variables needs more sophisticated data-gap check
        #(i.e. needs to check data-gaps for each variable separately)
        if variables is not None:
            raise NotImplementedError("Filtering by variables in get_data not implemented yet.")

        # Validate timezone awareness
        if start_time.tzinfo is None:
            raise ValueError("start_time must be timezone-aware")
        if end_time.tzinfo is None:
            raise ValueError("end_time must be timezone-aware")

        if not isinstance(station_id, str):
            station_id = str(station_id)

        #Round to nearest hour to avoid inconsistencies between providers and data gaps
        orig_timezone = start_time.tzinfo
        provider_freq = self.provider_manager.get_provider(provider.lower()).freq
        start_time_utc = pd.Timestamp( start_time.astimezone(timezone.utc) ).floor(provider_freq)
        end_time_utc = pd.Timestamp( end_time.astimezone(timezone.utc) ).ceil(provider_freq)

        logger.debug(f"Querying data from {start_time_utc} to {end_time_utc} with frequency {provider_freq}")

        # First, get existing data from database
        existing_data = db.query_data(
            provider=provider,
            station_id=station_id,
            start_time=start_time_utc,
            end_time=end_time_utc,
            variables=variables
        )

        if not existing_data.empty:
            logger.debug(f"Found existing data ranging from {existing_data.index.min()} to {existing_data.index.max()}")
                        
        # Find gaps in the data
        gaps = self._find_data_gaps(existing_data, start_time_utc, end_time_utc, freq = provider_freq)
        
        if not gaps:
            logger.info("No data gaps found")
            if not existing_data.empty:
                existing_data.index = existing_data.index.tz_convert(orig_timezone)
            return existing_data
        else:
            for (start_gap, end_gap) in gaps:
                logger.debug(f"Data gap found: {start_gap} - {end_gap}")
        
        # Fetch missing data
        new_data = self._fetch_missing_data(provider, station_id, gaps)
        
        if not new_data.empty:
            try:
                # Save new data to database
                db.insert_data(new_data, provider)
                
                # Re-query database to get complete dataset
                complete_data = db.query_data(
                    provider=provider,
                    station_id=station_id,
                    start_time=start_time_utc,
                    end_time=end_time_utc,
                    variables=variables
                )
                complete_data.index = complete_data.index.tz_convert(orig_timezone)
                return complete_data
                
            except Exception as e:
                logger.error(f"Error saving new data to database: {e}")
                # Return combination of existing and new data
                if not existing_data.empty:
                    combined_data = pd.concat([existing_data, new_data])
                    combined_data.index = combined_data.index.tz_convert(orig_timezone)
                    return combined_data

                new_data.index = new_data.index.tz_convert(orig_timezone)
                return new_data
        
        if not existing_data.empty:
            existing_data.index = existing_data.index.tz_convert(orig_timezone)
        return existing_data