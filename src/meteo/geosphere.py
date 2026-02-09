import pandas as pd
import asyncio

import logging
from typing import Tuple, Dict, Any
import datetime

from .base import BaseMeteoHandler

_GEOSPHERE_HOURLY_RENAME = {
    "time": "datetime",
    "t2m": "temperature_2m",
    "rr": "precipitation"
}

_GEOSPHERE_MODEL_FREQ = {
    "nowcast-v1-15min-1km": "15min",
    "ensemble-v1-1h-2500m": "h",
    "nwp-v1-1h-2500m": "h",
}

logger = logging.getLogger(__name__)

class GeoSphere(BaseMeteoHandler):
    provider_name = 'geosphere'
    can_forecast = True

    base_url = "https://dataset.api.hub.geosphere.at/v1/timeseries"
    timeseries_url = base_url + "/forecast"

    def __init__(self, locations: Dict[str, Dict], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.locations = locations
        self.variables = [i for i in _GEOSPHERE_HOURLY_RENAME.keys() if i != 'time']
        self.models = list(_GEOSPHERE_MODEL_FREQ.keys())

    def get_freq(self, models: list[str] | None = None) -> str:
        if not models:
            return self.freq

        freqs = {_GEOSPHERE_MODEL_FREQ.get(m, self.freq) for m in models}
        if len(freqs) > 1:
            raise ValueError(
                f"GeoSphere models have mixed frequencies: {sorted(freqs)}. "
                "Query models with the same frequency together."
            )
        elif len(freqs) == 0:
            raise ValueError(f"No frequency for selected models found. Got {models}. Choose one from {list(_GEOSPHERE_MODEL_FREQ.keys())}")
        return next(iter(freqs))

    @property
    def inclusive(self):
        """Must be one of 'left', 'right' or 'both'."""
        return "both"

    async def get_sensors(self, station_id: str) -> list[str]:
        return list(self.variables)

    async def get_stations(self) -> list[str] | None:
        return list(self.locations.keys())

    async def get_models(self) -> list[str]:
        return list(self.models)

    async def get_station_info(self, station_id: str | None) -> Dict[str, Any]:
        if station_id is None:
            return self.locations
        return self.locations.get(station_id, {})

    async def get_station_coords(self, station_id: str) -> Tuple[float, float]:
        if station_id not in self.locations.keys():
            raise ValueError(f"Station {station_id} not found. Choose one of {self.locations.keys()}")
        info = self.locations[station_id]
        return info['lat'], info['lon']

    async def _create_request_task(
        self, station_id: str, model: str, sensors: list[str], start=None, end=None
        ):

        if self._client is None:
            raise ValueError("Initialize client before requesting data")
        
        try:
            lat, lon = await self.get_station_coords(station_id)
            data_params: Dict[str, Any] = {
                "lat_lon": f"{lat},{lon}",
                "parameters": ','.join(sensors),
                "timezone": self.timezone,
            }
            if start is not None:
                data_params["start"] = pd.Timestamp(start).strftime("%Y-%m-%d")
            if end is not None:
                data_params["end"] = pd.Timestamp(end).strftime("%Y-%m-%d")
            async with self._semaphore:
                response = await self._client.get(
                        self.timeseries_url + f"/{model}", params = data_params,
                        timeout=self.timeout
                    )
                response.raise_for_status()
                await asyncio.sleep(self.sleep_time)

            response_data = {}
            for param_name, param_data in response.json()['properties']['parameters'].items():
                response_data[param_name] = param_data
            response_data = pd.DataFrame.from_dict(response_data, index = response.json()['timestamps'])

            if len(response_data) == 0:
                logger.warning(f"No data found for {data_params}")
                return None

            # From kg m-2 to mm
            # if 'rr' in response_data.columns:
            #     response_data['rr'] = response_data['rr'] * 86400

            response_data['station_id'] = station_id
            response_data['model'] = model

            return response_data
        except Exception as e:
            logger.error(f"Error fetching data for {station_id}: {e}", exc_info = True)
            return None

    async def get_raw_data(            
            self,            
            station_id: str,
            start: datetime.datetime | None = None,
            end: datetime.datetime | None = None,
            sensor_codes: list[str] | None = None,
            models: str | list[str] | None = None,
            **kwargs
        ) -> Tuple[pd.DataFrame | None, Dict]:

        possible_stations = await self.get_stations()
        if station_id not in possible_stations:
            raise ValueError(f"Invalid station_id {station_id}. Choose one from {possible_stations}")

        if isinstance(models, str):
            models = [models]
        if models is None:
            models = ["nwp-v1-1h-2500m"]
        models = [m.lower() for m in models]

        possible_models = await self.get_models()
        invalid_models = [i for i in models if i not in possible_models]
        if invalid_models:
            raise ValueError(f"Invalid models {invalid_models}. Choose from {possible_models}")

        all_sensors = await self.get_sensors(station_id)
        if sensor_codes is None:
            sensor_codes = all_sensors
        else:
            if not isinstance(sensor_codes, list):
                raise ValueError(f"Sensor_codes must be of type list. Got {type(sensor_codes)}")
            for sensor in sensor_codes:
                if sensor not in all_sensors:
                    raise ValueError(f"Invalid sensor {sensor}. Choose from: {all_sensors}")

        if start is not None and end is not None and start > end:
            raise ValueError(f"start must be before end. Got {start} - {end}")

        tasks = [self._create_request_task(station_id, m, sensor_codes, start, end) for m in models]

        raw_response = []
        for task in asyncio.as_completed(tasks):
            _data = await task
            if _data is not None:
                raw_response.append(self._rename_and_localize(_data))

        if len(raw_response) > 0:
            raw_response = pd.concat(raw_response, ignore_index = True)
        else:
            logger.warning(f"No data could be fetched for station {station_id}")
        
        st_metadata = await self.get_station_info(station_id)
        return raw_response, st_metadata

    def _rename_and_localize(self, raw_data: pd.DataFrame):

        df_reanmed = raw_data.copy()
        df_reanmed.rename(columns =_GEOSPHERE_HOURLY_RENAME, inplace = True)

        try:
            df_reanmed['datetime'] = pd.to_datetime(df_reanmed['datetime']).dt.tz_localize(self.timezone)
            df_reanmed['datetime'] = df_reanmed['datetime'].dt.tz_convert('UTC')
            df_reanmed['datetime'] = df_reanmed['datetime'].dt.floor(self.freq)
        except Exception as e:
            logger.error(f"Error transforming datetime: {e}")

        return df_reanmed

    def transform(self, raw_data: pd.DataFrame | None) -> pd.DataFrame | None:

        if raw_data is None:
            return None

        df_prepared = raw_data.copy()

        if df_prepared[['datetime', 'station_id', 'model']].duplicated().any():
            logger.warning("Found duplicates for ['datetime', 'station_id', 'model']. They will be dropped")
            df_prepared.drop_duplicates(subset = ['datetime', 'station_id', 'model'], inplace = True)

        return df_prepared

if __name__ == '__main__':

    async def test_fn():
        locations = {'bozen': {'lat': 46.498, 'lon': 11.354}}
        geosphere = GeoSphere(timezone = 'Europe/Rome', locations = locations)
        async with geosphere as prv:
            data, _ = await prv.get_raw_data('bozen', models = ["nwp-v1-1h-2500m"])
        
        transformed_data = geosphere.transform(data)
        validated_data = geosphere.validate(transformed_data)
        return validated_data

    data = asyncio.run(test_fn())
    print(data)