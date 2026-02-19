import pandas as pd
import asyncio
import httpx

import logging
from typing import Tuple, Dict, Any
import datetime
from dataclasses import dataclass

from .base import BaseMeteoHandler

_GEOSPHERE_MODELS = ["nowcast-v1-15min-1km", "ensemble-v1-1h-2500m", "nwp-v1-1h-2500m"]

_ACCUMULATED_PARAMS = {
    "nwp-v1-1h-2500m": ["rr_acc", "sundur_acc", "grad"]
}

_GEOSPHERE_MODEL_FREQS = {
    "nowcast-v1-15min-1km": "15min",
    "ensemble-v1-1h-2500m": "1h",
    "nwp-v1-1h-2500m": "1h",
}

_GEOSPHERE_RENAME = {
    "t2m": "tair_2m",
    "t2m_p10": "tair_2m_p10",
    "t2m_p50": "tair_2m_p50",
    "t2m_p90": "tair_2m_p90",

    "rr": "precipitation",
    "rr_p10": "precipitation_p10",
    "rr_p50": "precipitation_p50",
    "rr_p90": "precipitation_p90",

    "pt": "precipitation_type",
    "ff": "wind_speed",
    "fx": "wind_gust",
    "rh2m": "relative_humidity",

    "grad_p10": "solar_radiation_p10",
    "grad_p50": "solar_radiation_p50",
    "grad_p90": "solar_radiation_p90",

    "sundur_p10": "sun_duration_p10",
    "sundur_p50": "sun_duration_p50",
    "sundur_p90": "sun_duration_p90",

    "tcc_p10": "cloud_cover_p10",
    "tcc_p50": "cloud_cover_p50",
    "tcc_p90": "cloud_cover_p90",

    # from nwp-v1-1h-2500m
    'tcc': "cloud_cover",
    'rr_acc': "precipitation",
    'sundur_acc': 'sun_duration',
    'grad': "solar_radiation",
    "sy": "weather_code"
}

logger = logging.getLogger(__name__)

@dataclass
class ModelInfo:
    title: str
    freq: str
    mode: str
    parameters: Dict[str, str]
    spatial_resolution: int | None = None
    crs: str | None = None

    @classmethod
    def from_json(cls, json):

        params = {}
        for p_info in json['parameters']:
            params[p_info['name']] = p_info['unit']

        if json['frequency'] == "1H":
            freq = "1h"
        else:
            freq = json['frequency']

        return cls(
            title = json['title'],
            freq = freq,
            mode = json['mode'],
            parameters = params,
            spatial_resolution = json.get('spatial_resolution_m'),
            crs = json.get('crs')
        )

    @property
    def sensors(self):
        return list(self.parameters.keys())

class GeoSphere(BaseMeteoHandler):
    provider_name = 'geosphere'
    can_forecast = True

    base_url = "https://dataset.api.hub.geosphere.at/v1/timeseries"
    timeseries_url = base_url + "/forecast"

    def __init__(self, locations: Dict[str, Dict], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.locations = locations
        self.variables = ["t2m", "rr"]
        self.models = list(_GEOSPHERE_MODELS)
        self.model_info = None

    async def _get_model_info(self):

        if self._client is None:
            raise ValueError("Initialize client before requesting model info")

        model_info_dict = {}
        for m in self.models:
            try:
                logger.debug(f"Loading metadata for model {m}")
                response = await self._client.get(self.timeseries_url + f"/{m}/metadata", timeout=self.timeout)
                response.raise_for_status()

                model_info_dict[m] = ModelInfo.from_json(response.json())
            except Exception as e:
                logger.exception(f"Failed to load model info for model {m}: {e}")
        
        self.model_info = model_info_dict
        self.models = list(self.model_info.keys()) #remove models that failed and have no info

    async def __aenter__(self):
        """Start httpx client that is reused across requests"""
        logger.info("Opening API session...")
        self._client = httpx.AsyncClient(timeout = self.timeout)
        await self._authenticate()
        if self.model_info is None:
            await self._get_model_info()
        return self

    def get_freq(self, models: list[str] | None = None) -> str:
        
        if not models:
            return "1h"

        if self.model_info is None:
            logger.warning("Using fixed model frequency from _GEOSPHERE_MODEL_FREQS as self.model_info has not been initialized.")
            freqs = { _GEOSPHERE_MODEL_FREQS.get(m) for m in models }
            if None in freqs:
                raise ValueError(f"No frequency for selected models found. Got {models}")
            if len(freqs) > 1:
                raise ValueError(
                    f"GeoSphere models have mixed frequencies: {sorted(freqs)}. "
                    "Query models with the same frequency together."
                )
            return next(iter(freqs))

        freqs = {self.model_info[m].freq for m in models}
        if len(freqs) > 1:
            raise ValueError(
                f"GeoSphere models have mixed frequencies: {sorted(freqs)}. "
                "Query models with the same frequency together."
            )
        elif len(freqs) == 0:
            raise ValueError(f"No frequency for selected models found. Got {models}. Choose one from {self.get_models()}")
        return next(iter(freqs))

    @property
    def inclusive(self):
        """Must be one of 'left', 'right' or 'both'."""
        return "both"

    async def get_sensors(self, station_id: str) -> list[str]:
        if self.model_info is None:
            raise ValueError("Run _get_model_info() first before querying sensors")

        all_sensors = set()
        for info in self.model_info.values():
            all_sensors.update(info.sensors)

        normalized = set()
        for sensor in all_sensors:
            if "_p" in sensor:
                normalized.add(sensor.split("_p", 1)[0])
            else:
                normalized.add(sensor)

        return sorted(normalized)

    def get_model_sensors(self, model: str) -> list[str]:
        if self.model_info is None:
            raise ValueError("Run _get_model_info() first before querying model sensors")
        if model not in self.model_info:
            raise ValueError(f"Invalid model {model}. Choose one from {self.get_models()}")
        model_sensors_all = list(self.model_info[model].sensors)
        return [i for i in model_sensors_all if i in _GEOSPHERE_RENAME.keys()]

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
        self, station_id: str, model: str, sensors: list[str], end=None
        ):

        if self._client is None:
            raise ValueError("Initialize client before requesting data")

        try:
            async with self._semaphore:
                response = await self._client.get(self.timeseries_url + f"/{model}/metadata", timeout=self.timeout)
                response.raise_for_status()
                reference_start = pd.Timestamp(response.json()["last_forecast_reftime"])
        except Exception as e:
            logger.exception(f"Fetching last forecast reference time failed with error {e}")
            #pick conservative default. Forecasts start usually a few hours before now
            reference_start = datetime.datetime.now() - datetime.timedelta(days = 3)

        reference_start = pd.Timestamp(reference_start)
        if reference_start.tz is None:
            reference_start = reference_start.tz_localize("UTC")
        else:
            reference_start = reference_start.tz_convert("UTC")

        end_ts = None
        if end is not None:
            end_ts = pd.Timestamp(end)
            if end_ts.tz is None:
                end_ts = end_ts.tz_localize("UTC")
            else:
                end_ts = end_ts.tz_convert("UTC")
            if end_ts < reference_start:
                logger.warning(
                    "End is before the forecast reference start. "
                    f"end={end_ts}, reference_start={reference_start}"
                )
                return None
        
        try:
            lat, lon = await self.get_station_coords(station_id)
            data_params: Dict[str, Any] = {
                "lat_lon": f"{lat},{lon}",
                "start": reference_start.strftime("%Y-%m-%d %H:%M:%S"),
                "parameters": ','.join(sensors),
                "timezone": self.timezone,
            }
                        
            if end_ts is not None:
                data_params["end"] = end_ts.strftime("%Y-%m-%d %H:%M:%S")

            async with self._semaphore:
                response = await self._client.get(
                        self.timeseries_url + f"/{model}", params = data_params,
                        timeout=self.timeout
                    )
                response.raise_for_status()
                await asyncio.sleep(self.sleep_time)

            response_data = {}
            for feature in response.json()['features']:
                for param_name, param_data in feature['properties']['parameters'].items():
                    response_data[param_name] = param_data['data']
            response_data = pd.DataFrame(response_data)
            response_data['datetime'] = response.json()['timestamps']

            if len(response_data) == 0:
                logger.warning(f"No data found for {data_params}")
                return None

            # Drop the first row if it only contains null parameter values (common at reference start)
            param_cols = [c for c in response_data.columns if c != 'datetime']
            if param_cols and response_data[param_cols].iloc[0].isna().all():
                response_data = response_data.iloc[1:].reset_index(drop=True)
                if len(response_data) == 0:
                    logger.warning(f"No data left after dropping leading nulls for {data_params}")
                    return None

            # Fix accumulated parameters
            accumulated = [i for i in sensors if i in _ACCUMULATED_PARAMS.get(model, [])]
            if len(accumulated) > 0:
                for acc_col in accumulated:
                    response_data[acc_col] = self._cumulative_to_instantaneous(response_data[acc_col])

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

        model_freq = self.get_freq(models)

        if sensor_codes is not None and not isinstance(sensor_codes, list):
            raise ValueError(f"Sensor_codes must be of type list. Got {type(sensor_codes)}")

        if start is not None and end is not None and start > end:
            raise ValueError(f"start must be before end. Got {start} - {end}")

        tasks = []
        for model in models:
            if sensor_codes is None:
                available_sensors = self.get_model_sensors(model)
            else:
                available_sensors = sensor_codes

            expanded_sensors = []
            for sensor in available_sensors:
                expanded_sensors.extend(self._expand_model_sensor(model, sensor))
            
            if not expanded_sensors:
                logger.warning(f"No valid sensors for model {model}. Skipping.")
                continue
            tasks.append(self._create_request_task(station_id, model, expanded_sensors, end))

        raw_response = []
        for task in asyncio.as_completed(tasks):
            _data = await task
            if _data is not None:
                raw_response.append(self._rename_and_localize(_data, freq = model_freq))

        if len(raw_response) > 0:
            renamed_response = pd.concat(raw_response, ignore_index = True)
        else:
            logger.warning(f"No data could be fetched for station {station_id}")
            renamed_response = None

        st_metadata = await self.get_station_info(station_id)
        return renamed_response, st_metadata

    def _rename_and_localize(self, raw_data: pd.DataFrame, freq: str):

        df_renamed = raw_data.copy()
        df_renamed.rename(columns =_GEOSPHERE_RENAME, inplace = True)

        try:
            df_renamed['datetime'] = pd.to_datetime(df_renamed['datetime'])
            if df_renamed['datetime'].dt.tz is None:
                df_renamed['datetime'] = df_renamed['datetime'].dt.tz_localize('UTC')
            df_renamed['datetime'] = df_renamed['datetime'].dt.floor(freq)
        except Exception as e:
            logger.error(f"Error transforming datetime: {e}")

        # Convert solar radiation from Ws m-2 to W m-2 using the interval length
        try:
            seconds = pd.Timedelta(freq).total_seconds()
            if seconds > 0:
                for col in df_renamed.columns:
                    if col.startswith("solar_radiation"):
                        df_renamed[col] = df_renamed[col] / seconds
        except Exception as e:
            logger.error(f"Error converting solar radiation units: {e}")

        return df_renamed

    def transform(self, raw_data: pd.DataFrame | None) -> pd.DataFrame | None:

        if raw_data is None:
            return None

        df_prepared = raw_data.copy()

        if df_prepared[['datetime', 'station_id', 'model']].duplicated().any():
            logger.warning("Found duplicates for ['datetime', 'station_id', 'model']. They will be dropped")
            df_prepared.drop_duplicates(subset = ['datetime', 'station_id', 'model'], inplace = True)

        for col in df_prepared.columns:
            if col.startswith('cloud_cover'):
                df_prepared[col] = df_prepared[col] * 100

        return df_prepared

    def _expand_model_sensor(self, model: str, sensor: str) -> list[str]:

        if self.model_info is None:
            raise ValueError("Run _get_model_info() first before trying to expand model sensors")

        if model not in self.model_info:
            raise ValueError(f"Cannot expand sensors for model {model}. Choose one of {self.get_models()}")
        
        model_sensors = self.model_info[model].sensors
        if sensor in model_sensors:
            return [sensor]

        expanded = [i for i in model_sensors if i.startswith(f"{sensor}_")]
        return list(dict.fromkeys(expanded))

    def _cumulative_to_instantaneous(self, data: pd.Series):
        prev = data.shift(1)
        delta = data - prev

        # clamp tiny negatives to 0
        delta = delta.clip(lower=0)

        # keep NaNs where either current or prev is NaN
        mask_null = data.isna() | prev.isna()
        delta[mask_null] = pd.NA

        return delta

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
