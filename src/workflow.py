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
    ):
        tz_name = query.timezone or self.runtime.default_timezone
        tz = pytz.timezone(tz_name)

        provider_handler = self.runtime.provider_manager.get_provider(query.provider.lower())

        if provider_handler is None:
            raise ValueError(f"Unknow provider {query.provider}. Choose one of {self.runtime.provider_manager.list_providers()}")

        start_time = query.start_time
        end_time = query.end_time
        latest = start_time is None and end_time is None

        if latest:
            window_minutes = provider_handler.latest_window_minutes
            end_time = datetime.now(tz)
            start_time = end_time - timedelta(minutes=window_minutes)
            query.start_time = start_time
            query.end_time = end_time

            df = await self.runtime.query_manager.get_latest_from_provider(
                provider_handler=provider_handler,
                station_id=query.station_id,
                window_minutes=window_minutes,
                variables=query.variables,
            )
            pending = None
            if not df.empty:
                pending = df.reset_index()
                if "datetime" not in pending.columns:
                    pending = pending.rename(columns={"index": "datetime"})
        else:
            if end_time is None:
                end_time = datetime.now(tz)
            elif end_time.tzinfo is None:
                end_time = tz.localize(end_time)

            if start_time is None:
                start_time = end_time - timedelta(minutes=provider_handler.latest_window_minutes)
            elif start_time.tzinfo is None:
                start_time = tz.localize(start_time)

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
            "timezone_used": str(query.start_time.tzinfo),
            }

        if not df.empty:
            query_metadata['result_timezone'] = str(getattr(df.index, "tz", None))

        response = TimeseriesResponse.from_dataframe(df, latest = latest)
        response.metadata = query_metadata

        return (response, pending)
