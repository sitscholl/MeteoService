import pandas as pd
from pandas.tseries.frequencies import to_offset

import logging
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple, Any

from .database.db import MeteoDB
from .provider_manager import ProviderManager
from .utils import derive_datetime_gaps

logger = logging.getLogger(__name__)

class QueryManager:
    """Orchestrates data fetching from database and external providers."""
    
    def __init__(self, config: dict[str, Any], provider_manager: ProviderManager = None):
        """Initialize DataManager with configuration."""
        self.config = config
        self.provider_manager = provider_manager

        
    def _find_data_gaps(
        self,
        existing_data: pd.DataFrame,
        start_time: datetime,
        end_time: datetime,
        freq: str,
        inclusive: str = 'both',
        min_gap_duration: str = '30min'
    ) -> List[Tuple[datetime, datetime]]:
        """Find gaps in the database data for the requested time range."""
        try:
            freq_offset = to_offset(freq)
            min_gap_duration = pd.Timedelta(min_gap_duration)
            freq_delta = pd.Timedelta(freq_offset)

            start_time_aligned = pd.Timestamp(start_time).floor(freq)
            end_time_aligned = pd.Timestamp(end_time).ceil(freq)

            complete_ts = pd.date_range(
                start=start_time_aligned,
                end=end_time_aligned,
                freq=freq,
                inclusive=inclusive
            )

            if complete_ts.empty:
                return []

            if existing_data.empty:
                return [(complete_ts[0], complete_ts[-1])]

            existing_index = existing_data.index
            if existing_index.tz is None:
                existing_index = existing_index.tz_localize('UTC')

            target_tz = start_time.tzinfo or timezone.utc
            if existing_index.tz != target_tz:
                existing_index = existing_index.tz_convert(target_tz)

            existing_index = existing_index.sort_values().unique()
            missing_ts = complete_ts.difference(existing_index)

            if missing_ts.empty:
                return []

            gaps = []
            for gap_start, gap_end in derive_datetime_gaps(missing_ts.tolist(), freq=freq):
                coverage = (pd.Timestamp(gap_end) + freq_delta) - pd.Timestamp(gap_start)
                if coverage >= min_gap_duration:
                    gaps.append((gap_start, gap_end))

            return gaps

        except Exception as e:
            logger.error(f"Error finding data gaps: {e}")
            return [(start_time, end_time)]  # Return full range as gap on error
    
    def _fetch_missing_data(
        self,
        provider_name: str,
        station_id: str,
        gaps: List[Tuple[datetime, datetime]],
        freq: str,
        all_variables: List[str]
    ) -> pd.DataFrame:
        """Fetch and align missing data from the specified provider. Also makes sure that missing timestamps are included by filling with NA"""
        if not gaps:
            return pd.DataFrame()
        
        if not hasattr(self, 'provider_manager'):
            logger.warning("No provider_manager initialized for query_manager. Cannot fetch missing data")
            return pd.DataFrame()

        provider = self.provider_manager.get_provider(provider_name.lower())
        if provider is None:
            logger.info(f"No provider available for provider: {provider_name}. Missing data cannot be requested.")
            return pd.DataFrame()

        all_data: list[pd.DataFrame] = []
        n = len(gaps)

        try:
            with provider:
                for i, (start_gap, end_gap) in enumerate(gaps):
                    gap_days = (end_gap - start_gap).days
                    max_gap_days = self.config.get('settings', {}).get('max_gap_days', 30)
                    
                    if max_gap_days > 0 and gap_days > max_gap_days:
                        logger.warning(f"Gap of {gap_days} days exceeds maximum of {max_gap_days} days. Skipping.")
                        continue

                    gap_index = pd.date_range(
                        start=pd.Timestamp(start_gap),
                        end=pd.Timestamp(end_gap),
                        freq=freq,
                        inclusive='both'
                    )

                    if gap_index.empty:
                        continue
                    
                    logger.info(f"Fetching data from {provider_name} for {start_gap} to {end_gap}")

                    provider_inclusion = provider.inclusive
                    if provider_inclusion == 'left' and i == n - 1:
                        end_gap = end_gap + pd.Timedelta(freq)
                    if provider_inclusion == 'right' and i == 0:
                        start_gap = start_gap - pd.Timedelta(freq)
                    
                    provider_data = provider.run(
                        start=start_gap,
                        end=end_gap,
                        data_type='meteo',
                        station_id=station_id
                    )

                    if provider_data.empty:
                        logger.debug(f"No data returned for {start_gap} - {end_gap}")
                        if all_variables:
                            placeholder = pd.DataFrame({
                                'datetime': gap_index,
                                'station_id': station_id
                            })
                            for column in all_variables:
                                placeholder[column] = pd.NA
                            all_data.append(placeholder)
                        continue

                    #Add missing timestamps
                    provider_data.set_index('datetime', inplace=True)
                    provider_data = provider_data[~provider_data.index.duplicated(keep='last')]
                    provider_data = provider_data.reindex(gap_index) #maybe use a tolerance here that equals freq?

                    #Add missing variables
                    for column in all_variables:
                        if column not in provider_data.columns:
                            provider_data[column] = pd.NA

                    provider_data.reset_index(inplace=True)
                    provider_data.rename(columns={'index': 'datetime'}, inplace=True)

                    all_data.append(provider_data)
        
        except Exception as e:
            logger.error(f"Error fetching data from {provider_name}: {e}")
            return pd.DataFrame()
        
        if all_data:
            result = pd.concat(all_data, ignore_index=True)
            result.sort_values('datetime', inplace=True)
            # result.reset_index(drop=True, inplace=True)
            return result
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
        provider_handler = self.provider_manager.get_provider(provider.lower())
        if provider_handler is None:
            raise ValueError(f"No provider configured for '{provider}'. Available providers: {self.provider_manager.list_providers()}")

        provider_freq = provider_handler.freq
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
        new_data = self._fetch_missing_data(
            provider_name=provider,
            station_id=station_id,
            gaps=gaps,
            freq=provider_freq,
            all_variables = [] if existing_data.empty else [i for i in existing_data.columns if i not in {'station_id', 'datetime'}]
        )
        
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
                new_data_indexed = new_data.set_index('datetime').sort_index()
                new_data_indexed.index = new_data_indexed.index.tz_convert(orig_timezone)

                if not existing_data.empty:
                    combined_data = pd.concat([existing_data.sort_index(), new_data_indexed])
                    combined_data = combined_data[~combined_data.index.duplicated(keep='last')]
                    return combined_data

                return new_data_indexed
        
        if not existing_data.empty:
            existing_data.index = existing_data.index.tz_convert(orig_timezone)
        return existing_data
