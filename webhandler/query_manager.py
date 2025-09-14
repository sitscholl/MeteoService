import pandas as pd

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple, Any
import importlib

from webhandler.db import MeteoDB
from webhandler.utils import derive_datetime_gaps

logger = logging.getLogger(__name__)

class QueryManager:
    """Orchestrates data fetching from database and external providers."""
    
    def __init__(self, config: dict[str, Any]):
        """Initialize DataManager with configuration."""
        self.config = config
        self.providers = self._initialize_providers()
    
    def _initialize_providers(self) -> Dict:
        """Initialize available data providers."""
        providers = {}
        for provider_name, provider_config in self.config.get('providers', {}).items():
            if provider_config.get('enabled', False):
                try:
                    module_path, class_name = provider_config['class'].rsplit('.', 1)
                    provider_module = importlib.import_module(module_path)
                    provider_class = getattr(provider_module, class_name)
                except Exception as e:
                    logger.error(f"Error initializing provider class {provider_config['class']}: {e}. Not using provider {provider_name}.")
                    continue

                providers[provider_name.lower()] = {
                    'class': provider_class,
                    'credentials': {
                        **{key: value for key, value in provider_config.items() if key != 'class'}
                    }
                }
        
        logger.info(f"Initialized providers: {list(providers.keys())}")
        return providers
    
    def _find_data_gaps(self, existing_data: pd.DataFrame, start_time: datetime,
                        end_time: datetime) -> List[Tuple[datetime, datetime]]:
        """Find gaps in the database data for the requested time range."""
        try:
            if existing_data.empty:
                # No data exists, entire range is a gap
                return [(start_time, end_time)]

            try:
                freq = pd.infer_freq(existing_data.index)
                if freq is None:
                    raise ValueError(f"Frequency cannot be determined for a series of len {len(existing_data)}")

                # Ensure timezone consistency between input times and existing data
                if existing_data.index.tz != start_time.tzinfo:
                    # Convert existing data index to match input timezone
                    existing_data_tz_converted = existing_data.copy()
                    existing_data_tz_converted.index = existing_data_tz_converted.index.tz_convert(start_time.tzinfo)
                else:
                    existing_data_tz_converted = existing_data

                complete_ts = pd.date_range(start=start_time, end=end_time, freq=freq)
                missing_ts = [ts for ts in complete_ts if ts not in existing_data_tz_converted.index]
                gaps = derive_datetime_gaps(missing_ts, freq = freq)
                return gaps
            except Exception as e:
                logger.warning(f'Unable to determine timeseries gaps: {e}')
                return [(start_time, end_time)]

        except Exception as e:
            logger.error(f"Error finding data gaps: {e}")
            return [(start_time, end_time)]  # Return full range as gap on error
    
    def _fetch_missing_data(self, provider_name: str, 
                           gaps: List[Tuple[datetime, datetime]], 
                           tags: Optional[Dict] = None) -> pd.DataFrame:
        """Fetch missing data from the specified provider."""
        if not gaps:
            return pd.DataFrame()
        
        provider_info = self.providers[provider_name.lower()]
        provider_class = provider_info['class']
        provider_kwargs = provider_info['credentials']
        
        all_data = []
        
        try:
            with provider_class(**provider_kwargs) as provider:
                for start_gap, end_gap in gaps:
                    # Check if gap is within reasonable limits
                    gap_days = (end_gap - start_gap).days
                    max_gap_days = self.config.get('settings', {}).get('max_gap_days', 30)
                    
                    if gap_days > max_gap_days:
                        logger.warning(f"Gap of {gap_days} days exceeds maximum of {max_gap_days} days. Skipping.")
                        continue
                    
                    logger.info(f"Fetching data from {provider_name} for {start_gap} to {end_gap}")
                    
                    # Fetch raw data
                    provider_data = provider.run(
                        start=start_gap,
                        end=end_gap,
                        data_type = 'meteo',
                        station_id = tags['station_id']
                    )
                    
                    if not provider_data.empty:
                        all_data.append(provider_data)
        
        except Exception as e:
            logger.error(f"Error fetching data from {provider_name}: {e}")
            return pd.DataFrame()
        
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()
    
    def get_data(self, db: MeteoDB, provider: str, start_time: datetime,
                 end_time: datetime, tags: Dict,
                 fields: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Get data from database and fetch missing data from providers if needed.

        Args:
            db: Database instance
            provider: provider name (corresponds to provider name in config.yaml --> providers)
            start_time: Start time for data query (must be timezone-aware)
            end_time: End time for data query (must be timezone-aware)
            tags: Optional tags for filtering
            fields: Optional list of fields to return

        Returns:
            Complete dataset combining database and newly fetched data
        """

        # Validate timezone awareness
        if start_time.tzinfo is None:
            raise ValueError("start_time must be timezone-aware")
        if end_time.tzinfo is None:
            raise ValueError("end_time must be timezone-aware")

        if not 'station_id' in tags.keys():
            raise ValueError("The 'station_id' key must be provided in the tags.")

        # First, get existing data from database
        existing_data = db.query_data(
            provider=provider,
            start_time=start_time,
            end_time=end_time,
            tags=tags,
            fields=fields
        )
        
        # Check if auto-fetch is enabled
        if not self.config.get('settings', {}).get('auto_fetch_missing_data', True):
            return existing_data
        
        # Find which provider to use
        if not provider.lower() in self.providers.keys():
            logger.info(f"No provider available for provider: {provider}")
            return existing_data
        
        # Find gaps in the data
        gaps = self._find_data_gaps(existing_data, start_time, end_time)
        
        if not gaps:
            logger.info("No data gaps found")
            return existing_data
        
        # Fetch missing data
        new_data = self._fetch_missing_data(provider, gaps, tags)
        
        if not new_data.empty:
            try:
                # Save new data to database
                db.insert_data(new_data, provider, tags)
                
                # Re-query database to get complete dataset
                complete_data = db.query_data(
                    provider=provider,
                    start_time=start_time,
                    end_time=end_time,
                    tags=tags,
                    fields=fields
                )
                return complete_data
                
            except Exception as e:
                logger.error(f"Error saving new data to database: {e}")
                # Return combination of existing and new data
                if not existing_data.empty:
                    return pd.concat([existing_data, new_data], ignore_index=True)
                return new_data
        
        return existing_data