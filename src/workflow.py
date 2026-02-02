import pandas as pd
import pytz
from datetime import datetime, timedelta
from fastapi import HTTPException

from .runtime import RuntimeContext
from .validation import TimeseriesQuery, TimeseriesResponse

class QueryWorkflow:

    def __init__(self, runtime: RuntimeContext, latest_window_minutes: int = 60):
        self.runtime = runtime
        self.latest_window_minutes = latest_window_minutes

    async def run_timeseries_query(
        self,
        query: TimeseriesQuery,
    ):
        tz_name = query.timezone or self.runtime.default_timezone
        tz = pytz.timezone(tz_name)

        start_time = query.start_time
        end_time = query.end_time
        if start_time is None and end_time is None:
            latest = True
        else:
            latest = False

        if end_time is None:
            end_time = datetime.now(tz)
        elif end_time.tzinfo is None:
            end_time = tz.localize(end_time)

        if start_time is None:
            start_time = end_time - timedelta(minutes=self.latest_window_minutes)
        elif start_time.tzinfo is None:
            start_time = tz.localize(start_time)

        query.start_time = start_time
        query.end_time = end_time

        if query.start_time >= query.end_time:
            raise HTTPException(status_code=400, detail="start_time must be before end_time")

        provider_handler = self.runtime.provider_manager.get_provider(query.provider.lower())

        if provider_handler is None:
            raise ValueError(f"Unknow provider {query.provider}. Choose one of {self.runtime.provider_manager.list_providers()}")

        ## TODO: fix bug when query goes until now(), latest timestamps which are not included yet are filled with NAN and persisted in the DB
        df, pending = await self.runtime.query_manager.get_data(
            db=self.runtime.db,
            provider_handler=provider_handler,
            station_id=query.station_id,
            start_time=query.start_time,
            end_time=query.end_time,
            variables=query.variables,
        )

        station = self.runtime.db.query_station(provider=query.provider, external_id=query.station_id)
        if not station:
            station_info = {}
        else:
            station = station[0]
            station_info = {"elevation": station.elevation, 'latitude': station.latitude, 'longitude': station.longitude}

        query_metadata = {
            "provider": query.provider,
            "station": query.station_id,
            "elevation": station_info.get('elevation'),
            "latitude": station_info.get('latitude'),
            "longitude": station_info.get('longitude'),
            "variables": query.variables,
            "timezone_used": str(query.start_time.tzinfo),
            }

        if not df.empty:
            query_metadata['result_timezone'] = str(getattr(df.index, "tz", None))

        response = TimeseriesResponse.from_dataframe(df, latest = latest)
        response.metadata = query_metadata

        return (response, pending)
