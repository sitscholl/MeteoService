import pytz
from datetime import datetime, timedelta
from fastapi import HTTPException
import logging

from .runtime import RuntimeContext
from .validation import TimeseriesQuery, TimeseriesResponse

logger = logging.getLogger(__name__)

class QueryWorkflow:

    def __init__(self, runtime: RuntimeContext):
        self.runtime = runtime

    async def run_timeseries_query(
        self,
        query: TimeseriesQuery,
        latest: bool = False
    ):
        tz_name = query.timezone or self.runtime.default_timezone
        tz = pytz.timezone(tz_name)

        provider_handler = self.runtime.provider_manager.get_provider(query.provider.lower())

        if provider_handler is None:
            raise ValueError(f"Unknow provider {query.provider}. Choose one of {self.runtime.provider_manager.list_providers()}")

        start_time = query.start_time
        end_time = query.end_time
        now = datetime.now(tz = start_time.tzinfo)

        if start_time is not None and not provider_handler.can_forecast and start_time > now:
            raise ValueError("Start time must be in the past for non-forecast providers")

        if start_time is not None and start_time.tzinfo is None:
            start_time = tz.localize(start_time)

        window_minutes = (
            provider_handler.forecast_window_minutes
            if provider_handler.can_forecast
            else provider_handler.latest_window_minutes
        )

        if end_time is None:
            if start_time is not None and provider_handler.can_forecast and start_time > now:
                end_time = start_time + timedelta(minutes=window_minutes)
            else:
                end_time = now
        elif end_time.tzinfo is None:
            end_time = tz.localize(end_time)

        if start_time is None:
            start_time = end_time - timedelta(minutes=window_minutes)

        query.start_time = start_time
        query.end_time = end_time

        if query.start_time >= query.end_time:
            raise HTTPException(status_code=400, detail="start_time must be before end_time")

        df, pending = await self.runtime.query_manager.get_data(
            db=self.runtime.db,
            provider_handler=provider_handler,
            station_id=query.station_id,
            start_time=query.start_time,
            end_time=query.end_time,
            variables=query.variables,
            models=query.models
        )

        station = self.runtime.db.query_station(provider=provider_handler.provider_name, external_id=query.station_id)
        if not station:
            try:
                logger.debug(f"Fetching station info for station {query.station_id} from provider as station is not yet in database")
                async with provider_handler as prv:
                    station_info = await prv.get_station_info(query.station_id)
                station_info = station_info or {}
            except Exception as e:
                logger.exception(f"Error fetching station info for station {query.station_id}: {e}")
                station_info = {}
        else:
            station = station[0]
            station_info = {"elevation": station.elevation, 'latitude': station.latitude, 'longitude': station.longitude, "name": station.name}

        query_metadata = {
            "provider": query.provider,
            "station": query.station_id,
            "name": station_info.get('name'),
            "elevation": station_info.get('elevation'),
            "latitude": station_info.get('latitude'),
            "longitude": station_info.get('longitude'),
            "variables": query.variables,
            "query_timezone": str(query.start_time.tzinfo),
            }

        if not df.empty:
            query_metadata['result_timezone'] = str(getattr(df.datetime.dt, "tz", None))

        response = TimeseriesResponse.from_dataframe(df, latest = latest)
        response.metadata = query_metadata

        return (response, pending)
