import pandas as pd

import logging
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from .database.db import MeteoDB
from .provider_manager import ProviderManager
from .gapfinder import Gapfinder

logger = logging.getLogger(__name__)

class QueryManager:
    """Orchestrates data fetching from database and external providers."""
    
    def __init__(self):
        pass
            
    def _fetch_missing_data(
        self,
        provider_manager: ProviderManager,
        station_id: str,
        gaps: List[Tuple[datetime, datetime]],
        all_variables: List[str]
    ) -> pd.DataFrame:
        """Fetch and align missing data from the specified provider. Also makes sure that missing timestamps are included by filling with NA"""
        if not gaps:
            return pd.DataFrame()
        
        if not hasattr(self, 'provider_manager'):
            logger.warning("No provider_manager initialized for query_manager. Cannot fetch missing data")
            return pd.DataFrame()

        all_data: list[pd.DataFrame] = []
        n = len(gaps)

        try:
            async with provider_manager as prv:
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
            logger.error(f"Error fetching data from {provider_manager.name}: {e}")
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
            provider_manager: ProviderManager, 
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
        if start_time.tzinfo != end_time.tzinfo:
            raise ValueError("start_time and end_time must have the same timezone")
        if start_time > end_time:
            raise ValueError("start_time must be before end_time")
        if start_time > datetime.now(timezone.utc).astimezone(start_time.tzinfo):
            raise ValueError("start_time must be in the past")

        if not isinstance(station_id, str):
            station_id = str(station_id)

        # Convert to UTC
        orig_timezone = start_time.tzinfo
        start_time_utc = start_time.astimezone(timezone.utc)
        end_time_utc = end_time.astimezone(timezone.utc)
        now_utc = datetime.now(timezone.utc)

        # Round timestamps
        # Floor the start to ensure we cover the interval
        start_time_round = pd.Timestamp(start_time_utc).floor(provider_manager.freq)
        
        # Floor the end to ensure we only query complete timestamps
        end_time_round = pd.Timestamp(end_time_utc).floor(provider_manager.freq)
        
        # Ensure we don't query the future
        now_floor = pd.Timestamp(now_utc).floor(provider_manager.freq)
        if end_time_round > now_floor:
            logger.warning(f"Requested end time is in the future. Capping at {now_floor} (UTC)")
            end_time_round = now_floor

        if start_time_round >= end_time_round:
            # Handle cases where the range is smaller than the frequency
            return pd.DataFrame() 

        logger.info(f"Querying data from {start_time_round} (UTC) to {end_time_round} (UTC) with frequency {provider_manager.freq} and provider {provider_manager.name}")

        # First, get existing data from database
        existing_data = db.query_data(
            provider=provider_manager.name,
            station_id=station_id,
            start_time=start_time_round,
            end_time=end_time_round,
            variables=variables
        )

        if not existing_data.empty:
            logger.info(f"Found existing data ranging from {existing_data.index.min()} to {existing_data.index.max()}")
                        
        # Find gaps in the data
        ##TODO: Gapfinder for now directly called, wire in better
        gaps = Gapfinder().find_data_gaps(existing_data, start_time_round, end_time_round, freq = provider_manager.freq)
        
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
            provider_manager=provider_manager,
            station_id=station_id,
            gaps=gaps,
            all_variables = [] if existing_data.empty else [i for i in existing_data.columns if i not in {'station_id', 'datetime'}]
        )
        
        if not new_data.empty:
            try:
                # Save new data to database
                db.insert_data(new_data, provider_manager.name)
                
                # Re-query database to get complete dataset
                complete_data = db.query_data(
                    provider=provider_manager.name,
                    station_id=station_id,
                    start_time=start_time_round,
                    end_time=end_time_round,
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
