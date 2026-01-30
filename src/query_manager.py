import pandas as pd

import logging
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from .database.db import MeteoDB
from .meteo.base import BaseMeteoHandler
from .gapfinder import Gapfinder

logger = logging.getLogger(__name__)

class QueryManager:
    """Orchestrates data fetching from database and external providers."""
    
    def __init__(self):
        self.gapfinder = Gapfinder()
            
    def _fetch_missing_data(
        self,
        provider_handler: BaseMeteoHandler,
        station_id: str,
        gaps: List[Tuple[datetime, datetime]] | None,
        all_variables: List[str]
    ) -> pd.DataFrame:
        """Fetch and align missing data from the specified provider. Also makes sure that missing timestamps are included by filling with NA"""
        
        if not gaps:
            return pd.DataFrame()
        
        all_data: list[pd.DataFrame] = []
        n = len(gaps)
        try:
            async with provider_handler as prv:
                for i, (start_gap, end_gap) in enumerate(gaps):

                    gap_index = pd.date_range(
                        start=pd.Timestamp(start_gap),
                        end=pd.Timestamp(end_gap),
                        freq=prv.freq,
                        inclusive='both'
                    )

                    if gap_index.empty:
                        continue
                    
                    logger.debug(f"Fetching data gap from {start_gap} to {end_gap} ")

                    provider_inclusion = prv.inclusive
                    if provider_inclusion == 'left' and i == n - 1:
                        end_gap = end_gap + pd.Timedelta(prv.freq)
                    if provider_inclusion == 'right' and i == 0:
                        start_gap = start_gap - pd.Timedelta(prv.freq)
                    
                    provider_data = prv.get_data(
                        start=start_gap,
                        end=end_gap,
                        data_type='meteo',
                        station_id=station_id
                    )

                    if provider_data.empty:
                        logger.warning(f"No data returned for {start_gap} - {end_gap}")
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
            logger.error(f"Error fetching data from {provider_handler.name}: {e}")
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
            provider_handler: BaseMeteoHandler, 
            station_id: str,
            start_time: datetime,
            end_time: datetime, 
            variables: Optional[List[str]] = None
        ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Get data from database and fetch missing data from providers if needed.

        Args:
            db: Database instance
            provider: provider name (corresponds to provider name in config.yaml --> providers)
            start_time: Start time for data query (must be timezone-aware)
            end_time: End time for data query (must be timezone-aware)
            variables: Optional list of variables to return
        """

        #Filtering by variables needs more sophisticated data-gap check
        #(i.e. needs to check data-gaps for each variable separately)
        if variables is not None:
            raise NotImplementedError("Filtering by variables in get_data not implemented yet.")
        self._validate_query_times(start_time, end_time)

        if not isinstance(station_id, str):
            station_id = str(station_id)

        # Convert to UTC
        orig_timezone = start_time.tzinfo
        start_time_utc = start_time.astimezone(timezone.utc)
        end_time_utc = end_time.astimezone(timezone.utc)

        start_time_round, end_time_round = self._round_range_to_freq(start_time_utc, end_time_utc, freq = provider_handler.freq)
        if start_time_round >= end_time_round:
            # Handle cases where the range is smaller than the frequency
            return pd.DataFrame(), pd.DataFrame()

        logger.info(
            f"Querying data from {start_time_round} (UTC) to {end_time_round} (UTC) with frequency {provider_handler.freq} and provider {provider_handler.name}"
            )

        # Get existing data from database
        existing_data = db.query_data(
            provider=provider_handler.name,
            station_id=station_id,
            start_time=start_time_round,
            end_time=end_time_round,
            variables=variables
        )

        if not existing_data.empty:
            logger.info(f"Found existing data ranging from {existing_data.index.min()} to {existing_data.index.max()}")
                        
        # Find gaps in the data
        dt_index = existing_data.index if isinstance(existing_data.index, pd.DatetimeIndex) else pd.DatetimeIndex([])
        gaps = self.gapfinder.find_data_gaps(dt_index, start_time_round, end_time_round, freq = provider_handler.freq)
        
        if not gaps:
            logger.info("No data gaps found")
            if not existing_data.empty:
                existing_data.index = existing_data.index.tz_convert(orig_timezone)
            return existing_data, pd.DataFrame()
        else:
            for (start_gap, end_gap) in gaps:
                logger.debug(f"Data gap found: {start_gap} - {end_gap}")
        
        # Fetch missing data
        new_data = self._fetch_missing_data(
            provider_handler=provider_handler,
            station_id=station_id,
            gaps=gaps,
            all_variables = [] if existing_data.empty else [i for i in existing_data.columns if i not in {'station_id', 'datetime'}]
        )

        if new_data.empty:
            if not existing_data.empty:
                existing_data.index = existing_data.index.tz_convert(orig_timezone)
            return existing_data, pd.DataFrame()

        combined = self._combine_existing_and_new(existing_data, new_data, orig_timezone)
        return combined, new_data
       
    @staticmethod
    def _validate_query_times(start_time, end_time):
        # Validate timezone awareness
        if start_time.tzinfo is None:
            raise ValueError("start_time must be timezone-aware")
        if end_time.tzinfo is None:
            raise ValueError("end_time must be timezone-aware")
        if start_time.tzinfo != end_time.tzinfo:
            raise ValueError("start_time and end_time must have the same timezone")
        if start_time > end_time:
            raise ValueError("start_time must be before end_time")
        if start_time > datetime.now(timezone.utc).astimezone(start_time.tzinfo):
            raise ValueError("start_time must be in the past")

    @staticmethod
    def _round_range_to_freq(start_time: datetime, end_time: datetime, freq: str):
        # Floor the start to ensure we cover the interval
        start_time_round = pd.Timestamp(start_time).floor(freq)
        
        # Floor the end to ensure we only query complete timestamps
        end_time_round = pd.Timestamp(end_time).floor(freq)
        
        # Ensure we don't query the future
        now_utc = datetime.now(timezone.utc)
        now_floor = pd.Timestamp(now_utc).floor(freq)
        if end_time_round > now_floor:
            logger.warning(f"Requested end time is in the future. Capping at {now_floor} (UTC)")
            end_time_round = now_floor

        return start_time_round, end_time_round

    @staticmethod
    def _combine_existing_and_new(existing_data: pd.DataFrame, new_data: pd.DataFrame, target_tz) -> pd.DataFrame:
        new_data_indexed = new_data.set_index('datetime').sort_index()
        if new_data_indexed.index.tz is None:
            new_data_indexed.index = new_data_indexed.index.tz_localize(timezone.utc)
        new_data_indexed.index = new_data_indexed.index.tz_convert(target_tz)

        if existing_data.empty:
            return new_data_indexed

        existing_sorted = existing_data.sort_index()
        combined = pd.concat([existing_sorted, new_data_indexed])
        combined = combined[~combined.index.duplicated(keep='last')]
        return combined
