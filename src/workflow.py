import pandas as pd
from fastapi import HTTPException

from .runtime import RuntimeContext
from .validation import TimeseriesQuery, TimeseriesResponse

class QueryWorkflow:

    def __init__(self, runtime: RuntimeContext):
        self.runtime = runtime

    async def run_timeseries_query(
        self,
        query: TimeseriesQuery,
    ):

        if query.start_time >= query.end_time:
            raise HTTPException(status_code=400, detail="start_time must be before end_time")

        provider_handler = self.runtime.provider_manager.get_provider(query.provider)
        df, pending = self.runtime.query_manager.get_data(
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

        if df.empty:
            response = TimeseriesResponse(
                data=[],
                count=0,
                time_range={"start": query.start_time, "end": query.end_time},
                metadata=query_metadata,
            )
            return (response, pending)

        data = []
        for timestamp, row in df.iterrows():
            try:
                ts_val = timestamp.isoformat()
            except Exception:
                ts_val = str(timestamp)
            row_clean = row.where(pd.notna(row), None).to_dict()
            record = {"datetime": ts_val, **row_clean}
            data.append(record)

        query_metadata['result_timezone'] = str(getattr(df.index, "tz", None))

        response = TimeseriesResponse(
            data=data,
            count=len(data),
            time_range={"start": df.index.min(), "end": df.index.max()},
            metadata=query_metadata,
        )
        return (response, pending)

