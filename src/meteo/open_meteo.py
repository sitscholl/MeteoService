import pandas as pd
import asyncio

import logging
from typing import Tuple, Dict, Any
import datetime

from .base import BaseMeteoHandler

# a list of all the variables that are available
# Not used, insteal keys from _OPENMETEO_HOURLY_RENAME are used as possible variables
# _HOURLY_VARIABLES = ["temperature_2m","relative_humidity_2m","dew_point_2m","apparent_temperature","precipitation_probability","precipitation","rain","showers","snowfall","snow_depth",
#                     "soil_temperature_0cm","soil_temperature_6cm","soil_temperature_18cm","soil_temperature_54cm","soil_moisture_0_to_1cm","soil_moisture_1_to_3cm","soil_moisture_3_to_9cm","soil_moisture_9_to_27cm","soil_moisture_27_to_81cm",
#                     "weather_code","pressure_msl","surface_pressure","cloud_cover","cloud_cover_low","cloud_cover_mid","cloud_cover_high","visibility","evapotranspiration","et0_fao_evapotranspiration","vapour_pressure_deficit",
#                     "temperature_180m","temperature_120m","temperature_80m","wind_gusts_10m","wind_direction_180m","wind_direction_120m","wind_direction_80m","wind_direction_10m","wind_speed_180m","wind_speed_120m","wind_speed_80m","wind_speed_10m"]

_OPENMETEO_WEATHER_MODELS = ["best_match", "ecmwf_ifs", "ecmwf_ifs025", "ecmwf_aifs025_single", "cma_grapes_global", "bom_access_global", "icon_seamless", "icon_global", "icon_eu", "icon_d2", "metno_seamless", "metno_nordic", "gfs_seamless", "gfs_global", "gfs_hrrr", "ncep_nbm_conus", "ncep_nam_conus", "gfs_graphcast025", "ncep_aigfs025", "ncep_hgefs025_ensemble_mean", "gem_seamless", "gem_global", "gem_regional", "gem_hrdps_continental", "gem_hrdps_west", "knmi_seamless", "knmi_harmonie_arome_europe", "knmi_harmonie_arome_netherlands", "dmi_seamless", "dmi_harmonie_arome_europe", "jma_seamless", "jma_msm", "jma_gsm", "meteofrance_seamless", "meteofrance_arpege_world", "meteofrance_arpege_europe", "meteofrance_arome_france", "meteofrance_arome_france_hd", "ukmo_seamless", "ukmo_global_deterministic_10km", "ukmo_uk_deterministic_2km", "kma_seamless", "kma_ldps", "kma_gdps", "italia_meteo_arpae_icon_2i", "meteoswiss_icon_seamless", "meteoswiss_icon_ch1", "meteoswiss_icon_ch2"]

_OPENMETEO_HOURLY_RENAME = {
    "time": "datetime",
    "temperature_2m": "tair_2m",
    "relative_humidity_2m": "relative_humidity",
    "precipitation": "precipitation",
    "wind_speed_10m": "wind_speed",
    "wind_direction_10m": "wind_direction",
    "wind_gusts_10m": "wind_gust",
    "terrestrial_radiation_instant": "solar_radiation",
    "snow_depth": "snow_height",
    "cloud_cover": "cloud_cover"
}

logger = logging.getLogger(__name__)

class OpenMeteo(BaseMeteoHandler):
    
    provider_name = 'open-meteo'
    can_forecast = True

    base_url = "https://api.open-meteo.com/v1"
    timeseries_url = base_url + "/forecast"

    def __init__(self, locations: Dict[str, Dict], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.locations = locations
        self.variables = [i for i in _OPENMETEO_HOURLY_RENAME.keys() if i != 'time']
        self.models = list(_OPENMETEO_WEATHER_MODELS)
        self._last_queried_models = None

    @property
    def freq(self):
        return "h"

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
        self, station_id: str, models: list[str], sensors: list[str], start=None, end=None
        ):

        if self._client is None:
            raise ValueError("Initialize client before requesting data")
        
        try:
            lat, lon = await self.get_station_coords(station_id)
            data_params: Dict[str, Any] = {
                "latitude": lat,
                "longitude": lon,
                "hourly": ','.join(sensors),
                "timezone": self.timezone,
                "models": ','.join(models)
            }
            if start is not None:
                data_params["start_date"] = pd.Timestamp(start).strftime("%Y-%m-%d")
            if end is not None:
                data_params["end_date"] = pd.Timestamp(end).strftime("%Y-%m-%d")
            async with self._semaphore:
                response = await self._client.get(
                        self.timeseries_url, params = data_params,
                        timeout=self.timeout
                    )
                response.raise_for_status()

            response_data = pd.DataFrame(response.json()['hourly'])

            if len(response_data) == 0:
                logger.warning(f"No data found for {data_params}")
                return None

            response_data['station_id'] = station_id

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

        self._last_queried_models = None
        if isinstance(models, str):
            models = [models]
        if models is None:
            models = ["meteoswiss_icon_seamless"]
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

        # Create tasks
        raw_response = await self._create_request_task(
            station_id, models, sensor_codes, start=start, end=end
        )
        st_metadata = await self.get_station_info(station_id)
        if raw_response is None:
            logger.warning(f"No data could be fetched for station {station_id}")

        self._last_queried_models = models
        return raw_response, st_metadata

    def transform(self, raw_data: pd.DataFrame | None) -> pd.DataFrame | None:

        if raw_data is None:
            return None

        df_prepared = raw_data.copy()

        if df_prepared[['time', 'station_id']].duplicated().any():
            logger.warning("Found duplicates for ['time', 'station_id']. They will be dropped")
            df_prepared.drop_duplicates(subset = ['time', 'station_id'], inplace = True)

        # Extract model name from column and then stack model level into rows
        models = self._last_queried_models or []

        if len(models) == 1:
            df_prepared['model'] = models[0]
            df_prepared.rename(columns =_OPENMETEO_HOURLY_RENAME, inplace = True)
        else:
            new_columns = self._split_columns(df_prepared.columns, models)
            if new_columns:
                df_prepared.columns = pd.MultiIndex.from_tuples(new_columns, names=['parameter', 'model'])
                df_prepared = df_prepared.set_index([("time", ""), ("station_id", "")])
                df_prepared.index.names = ["datetime", "station_id"]
                df_prepared = df_prepared.stack(level="model").reset_index()
            
        for col in df_prepared.columns:
            if col in {"datetime", "station_id"}:
                continue
            if pd.api.types.is_integer_dtype(df_prepared[col]):
                df_prepared[col] = df_prepared[col].astype(float)

        try:
            df_prepared['datetime'] = pd.to_datetime(df_prepared['datetime']).dt.tz_localize(self.timezone)
            df_prepared['datetime'] = df_prepared['datetime'].dt.tz_convert('UTC')
            df_prepared['datetime'] = df_prepared['datetime'].dt.floor(self.freq)
        except Exception as e:
            logger.error(f"Error transforming datetime: {e}")

        return df_prepared

    def _split_columns(self, columns: list[str], models: list[str]):
        new_columns: list[Tuple[str, str]] = []

        for col in columns:
            match_found = False

            if col in {"time", "station_id"}:
                new_columns.append((col, ""))
                continue

            for m in sorted(models, key=len, reverse=True):
                if col.endswith(f"_{m}"):
                    base = col[: -len(m) - 1]
                    if base in _OPENMETEO_HOURLY_RENAME:
                        new_columns.append((_OPENMETEO_HOURLY_RENAME[base], m))
                        match_found = True
                        break
                    else:
                        logger.warning(
                            f"Unrecognized Open-Meteo parameter '{base}' for model '{m}'. Keeping original column '{col}'."
                        )
                        new_columns.append((base, m))
                        match_found = True
                        break
            
            if not match_found:
                if col in _OPENMETEO_HOURLY_RENAME:
                    new_columns.append((_OPENMETEO_HOURLY_RENAME[col], ''))
                else:
                    new_columns.append((col, ''))
        
        return new_columns

if __name__ == '__main__':

    async def test_fn():
        locations = {'bozen': {'lat': 46.498, 'lon': 11.354}}
        open_meteo = OpenMeteo(timezone = 'Europe/Rome', locations = locations)
        async with open_meteo as prv:
            data, _ = await prv.get_raw_data('bozen', models = ['meteoswiss_icon_seamless', 'best_match'])
        
        transformed_data = open_meteo.transform(data)
        validated_data = open_meteo.validate(transformed_data)
        return validated_data

    data = asyncio.run(test_fn())
    print(data)
