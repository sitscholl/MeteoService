import pandas as pd
import asyncio

import datetime
import pytz
from typing import Dict, Any, Tuple
import logging

from .base import BaseMeteoHandler
from ..utils import split_dates

logger = logging.getLogger(__name__)

PROVINCE_RENAME = {
    "DATE": "datetime",
    "LT": "tair_2m",
    "LF": "relative_humidity",
    "N": "precipitation",
    "WG": "wind_speed",
    "WR": "wind_direction",
    "WG.BOE": "wind_gust",
    "LD.RED": "air_pressure",
    "SD": "sun_duration",
    "GS": "solar_radiation",
    "HS": "snow_height",
    "W": "water_level",
    "Q": "discharge"
}

class ProvinceMeteo(BaseMeteoHandler):

    provider_name = 'province'

    base_url = "http://daten.buergernetz.bz.it/services/meteo/v1"
    sensors_url = base_url + "/sensors"
    stations_url = base_url + "/stations"
    timeseries_url = base_url + "/timeseries"

    @property
    def freq(self):
        return "10min"

    @property
    def inclusive(self):
        return "both"

    async def get_sensors(self, station_code: str):
        if self.station_sensors.get(station_code) is not None:
            return self.station_sensors.get(station_code)

        lock = self._station_sensors_locks.get(station_code)
        if lock is None:
            lock = asyncio.Lock()
            self._station_sensors_locks[station_code] = lock

        async with lock:
            if self.station_sensors.get(station_code) is not None:
                return self.station_sensors.get(station_code)

            if self._client is None:
                raise ValueError("Initialize client before querying sensors")

            response = await self._client.get(
                    self.sensors_url, params = {"station_code": station_code},
                    timeout=self.timeout
                )
            response.raise_for_status()

            sensors_list = [i['TYPE'] for i in response.json()]
            sensors_list = list(dict.fromkeys(sensors_list))
            self.station_sensors[station_code] = sensors_list #remove duplicates

            return sensors_list

    async def get_stations(self):
        if self.station_info is not None:
            return list(self.station_info.keys())
        else:
            info = await self.get_station_info()
            return list(info.keys())

    async def get_station_info(self, station_id: str | None = None) -> Dict[str, Any]:

        if self.station_info is not None:
            if station_id is not None:
                return self.station_info.get(station_id, {})
            return self.station_info

        async with self._station_info_lock:

            if self.station_info is not None:
                return self.station_info

            if self._client is None:
                raise ValueError("Initialize client before querying station info")

            response = await self._client.get(
                    self.stations_url, 
                    timeout=self.timeout
                )
            response.raise_for_status()

            response_data = response.json()

            if len(response_data['features']) == 0:
                raise ValueError("Error retrieving station info. Response data contains no features")

            info_dict = {}
            for i in response_data['features']:
                station_props = i['properties']
                station_info = {
                    'latitude': station_props.get('LAT'),
                    'longitude': station_props.get('LONG'),
                    'elevation': station_props.get('ALT'),
                    'name': station_props.get('NAME_D'),
                    'id': station_props['SCODE']
                }
                info_dict[station_props['SCODE']] = station_info

            self.station_info = info_dict

            if station_id is not None:
                return self.station_info.get(station_id, {})
            return self.station_info

    async def _create_request_task(
        self, station_id: str, date_range: Tuple[datetime, datetime], sensor: str
        ):

        if self._client is None:
            raise ValueError("Initialize client before requesting data")
        
        try:
            query_start, query_end = date_range
            data_params = {
                "station_code": station_id,
                "sensor_code": sensor,
                "date_from": query_start.strftime("%Y%m%d%H%M"),
                "date_to": query_end.strftime("%Y%m%d%H%M")
            }
            async with self._semaphore:
                response = await self._client.get(
                        self.timeseries_url, params = data_params,
                        timeout=self.timeout
                    )
                response.raise_for_status()
                await asyncio.sleep(self.sleep_time)

            response_data = pd.DataFrame(response.json())

            if len(response_data) == 0:
                logger.warning(f"No data found for {data_params}")
                return None

            response_data['sensor'] = sensor
            response_data['station_id'] = station_id

            return response_data
        except Exception as e:
            logger.error(f"Error fetching data for {sensor} for {query_start} - {query_end}: {e}", exc_info = True)
            return None

    async def get_raw_data(
            self,            
            station_id: str,
            start: datetime.datetime,
            end: datetime.datetime,
            split_on_year = True,
            sensor_codes: list[str] | None = None,
            **kwargs
        ):

        possible_stations = await self.get_stations()
        if station_id not in possible_stations:
            raise ValueError(f"Invalid station_id {station_id}. Choose one from {possible_stations}")

        start = start.astimezone(pytz.timezone(self.timezone))
        end = end.astimezone(pytz.timezone(self.timezone))
        
        dates_split = split_dates(start, end, freq = self.freq, n_days = self.chunk_size_days, split_on_year=split_on_year)
        
        all_sensors = await self.get_sensors(station_id)
        if sensor_codes is None:
            sensor_codes = all_sensors
        else:
            if not isinstance(sensor_codes, list):
                raise ValueError(f"Sensor_codes must be of type list. Got {type(sensor_codes)}")
            for sensor in sensor_codes:
                if sensor not in all_sensors:
                    raise ValueError(f"Invalid sensor {sensor}. Choose from: {all_sensors}")

        # Create tasks
        raw_responses = []
        for date_range in dates_split:
            for sensor in sensor_codes:
                raw_responses.append(self._create_request_task(station_id, date_range, sensor))

        # Make sure all workers finish
        raw_responses = await asyncio.gather(*raw_responses)
        raw_responses = [i for i in raw_responses if i is not None]

        st_metadata = await self.get_station_info(station_id)
        if len(raw_responses) > 0:
            return pd.concat(raw_responses, ignore_index = True), st_metadata
        else:
            logger.warning(f"No data could be fetched for station {station_id} and sensors {sensor_codes}")
            return None, st_metadata

    def transform(self, raw_data: pd.DataFrame | None):

        if raw_data is None:
            return None

        if raw_data[['DATE', 'station_id', 'sensor']].duplicated().any():
            logger.warning("Found duplicates for ['DATE', 'station_id', 'sensor']. They will be dropped")
            raw_data.drop_duplicates(subset = ['DATE', 'station_id', 'sensor'], inplace = True)
        
        df_pivot = raw_data.pivot(columns = "sensor", values = "VALUE", index = ["DATE", "station_id"]).reset_index()
        df_pivot.rename(columns = PROVINCE_RENAME, inplace = True)

        try:
            # 1. Capture whether it was Summer Time (CEST) before stripping
            is_dst = df_pivot['datetime'].str.contains('CEST')

            # 2. Strip the strings
            df_pivot['datetime'] =  df_pivot['datetime'].str.replace('CEST', '', regex=False).str.replace('CET', '', regex=False)

            # 3. Convert to naive datetime
            df_pivot['datetime'] = pd.to_datetime(df_pivot['datetime'], format="%Y-%m-%dT%H:%M:%S")

            # 4. Localize using the mask to resolve ambiguity
            # ambiguous=is_dst tells pandas: "If this hour repeats, use the DST version if is_dst is True"
            df_pivot['datetime'] = df_pivot['datetime'].dt.tz_localize(
                self.timezone, 
                ambiguous=is_dst,
                nonexistent='shift_forward' # handle the spring "gap" too
            ).dt.tz_convert('UTC')

            df_pivot['datetime'] = df_pivot['datetime'].dt.floor(self.freq)
        except Exception as e:
            logger.error(f"Error transforming datetime: {e}")

        # Precipitation is available in 5min freq while all others are in 10min freq. Drop additional timestamps for precipitation
        df_pivot = df_pivot.dropna(subset = [i for i in df_pivot.columns if i not in ['datetime', 'station_id', 'precipitation']], how = 'all')

        return df_pivot

if __name__ == '__main__':

    import logging

    logging.basicConfig(level = logging.DEBUG, force = True)

    async def run_test():
        start = datetime.datetime(2025, 1, 14)
        end = datetime.datetime(2025, 10, 21)

        pr_handler = ProvinceMeteo(timezone = 'Europe/Rome')
        async with pr_handler as meteo_handler:

            st_info = await meteo_handler.get_station_info("86900MS")
            print(st_info)

            data = await meteo_handler.run(
                station_id = '86900MS',
                sensor_codes = ["LT", 'N', 'LF'],
                start = start,
                end = end,
            )
        print(data)
        return data

    data = asyncio.run(run_test())