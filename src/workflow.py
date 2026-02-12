import pytz
from datetime import datetime, timedelta
from fastapi import HTTPException
import logging

from .runtime import RuntimeContext
from .validation import TimeseriesQuery, TimeseriesResponse, ResponseMetadata

logger = logging.getLogger(__name__)

class QueryWorkflow:

    def __init__(self, runtime: RuntimeContext):
        self.runtime = runtime

    def _get_timezone_for_query(self, query: TimeseriesQuery) -> str:
        if query.start_time and query.start_time.tzinfo:
            tz = query.start_time.tzinfo
            tz= getattr(tz, "zone", str(tz))
        elif query.end_time and query.end_time.tzinfo:
            tz = query.end_time.tzinfo
            tz = getattr(tz, "zone", str(tz))
        else:
            tz = query.timezone or self.runtime.default_timezone
        return tz

    def resample_columns(self, df, agg: str, min_size: int | None = None):
        if df is None or df.empty:
            return df
        return self.runtime.column_resampler.apply_resampling(
            data=df,
            freq=agg,
            min_sample_size=min_size,
        )

    async def run_timeseries_query(
        self,
        query: TimeseriesQuery,
        latest: bool = False,
        agg: str | None = None,
        min_size: int | None = None,
    ):
        if latest and agg is not None:
            raise HTTPException(status_code=400, detail="Aggregation is not supported for latest queries.")

        tz_name = self._get_timezone_for_query(query)
        tz = pytz.timezone(tz_name)
        query.timezone = tz_name

        provider_handler = self.runtime.provider_manager.get_provider(query.provider.lower())

        if provider_handler is None:
            raise ValueError(f"Unknow provider {query.provider}. Choose one of {self.runtime.provider_manager.list_providers()}")

        start_time = query.start_time
        end_time = query.end_time
        now = datetime.now(tz)

        if start_time is not None and start_time.tzinfo is None:
            start_time = tz.localize(start_time)
        if end_time is not None and end_time.tzinfo is None:
            end_time = tz.localize(end_time)

        if start_time is not None and not provider_handler.can_forecast and start_time > now:
            raise ValueError("Start time must be in the past for non-forecast providers")

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

        if agg is not None and not df.empty:
            df = self.resample_columns(df, agg=agg, min_size=min_size)

        station = self.runtime.db.query_station(provider=provider_handler.provider_name, external_id=query.station_id)
        if not station or len(station) == 0:
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

        response_metadata = ResponseMetadata.from_query(query, station_info = station_info)
        response = TimeseriesResponse.from_dataframe(df, latest = latest, metadata = response_metadata)

        return (response, pending)
