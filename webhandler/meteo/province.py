import pandas as pd

import datetime
import requests
import pytz
import time
from typing import Dict, Any
import logging

from webhandler.meteo.base import BaseMeteoHandler
from webhandler.utils import split_dates

logger = logging.getLogger(__name__)

PROVINCE_RENAME = {
    "DATE": "datetime",
    "LT": "tair_2m",
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

    def __init__(self, timezone: str, **kwargs):
        self.timezone = timezone
        self.station_codes = None
        self.station_sensors = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def get_sensors_for_station(self, station_code: str):
        if self.station_sensors.get(station_code) is not None:
            return self.station_sensors.get(station_code)
        response = requests.get(self.sensors_url, params = {"station_code": station_code})
        sensors_list = set([i['TYPE'] for i in response.json()])
        self.station_sensors[station_code] = sensors_list
        return sensors_list

    def get_station_codes(self):
        if self.station_codes is not None:
            return self.station_codes
        response = requests.get(self.stations_url)
        response.raise_for_status()
        stations_list = set([i['properties']['SCODE'] for i in response.json()['features']])
        self.station_codes = stations_list
        return stations_list

    def get_station_info(self, station_id: str) -> Dict[str, Any]:

        response = requests.get(self.stations_url)
        response.raise_for_status()

        response_data = response.json()
        station_info = [i for i in response_data['features'] if i['properties']['SCODE'] == station_id]

        if len(station_info) == 0 :
            logger.warning(f"No metadata found for {station_id}")
            return {}
        
        station_props = station_info[0]['properties']
        return {
            'latitude': station_props.get('LAT'),
            'longitude': station_props.get('LONG'),
            'elevation': station_props.get('ALT'),
            'name': station_props.get('NAME_D')
        }

    def get_data(
            self,            
            station_id: str,
            start: datetime.datetime,
            end: datetime.datetime,
            sleep_time: int = 1,
            request_batch_size = 365,
            split_on_year = True,
            sensor_codes: list[str] | None = None,
            **kwargs
        ):

        if station_id not in self.get_station_codes():
            raise ValueError(f"Invalid station_id {station_id}. Choose one from {self.get_station_codes()}")

        start = start.astimezone(pytz.timezone(self.timezone))
        end = end.astimezone(pytz.timezone(self.timezone))
        
        dates_split = split_dates(start, end, n_days = request_batch_size, split_on_year=split_on_year)
        if sensor_codes is None:
            sensor_codes = self.get_sensors_for_station(station_id)
        else:
            if not isinstance(sensor_codes, list):
                raise ValueError(f"Sensor_codes must be of type list. Got {type(sensor_codes)}")
            for sensor in sensor_codes:
                if sensor not in self.get_sensors_for_station(station_id):
                    raise ValueError(f"Invalid sensor {sensor}. Choose from: {self.get_sensors_for_station(station_id)}")

        raw_responses = []
        for query_start, query_end in dates_split:
            for sensor in sensor_codes:
                data_params = {
                    "station_code": station_id,
                    "sensor_code": sensor,
                    "date_from": query_start.strftime("%Y%m%d%H%M"),
                    "date_to": query_end.strftime("%Y%m%d%H%M")
                }
                response = requests.get(self.timeseries_url, params = data_params)
                response.raise_for_status()

                response_data = pd.DataFrame(response.json())

                if len(response_data) == 0:
                    logger.warning(f"No data found for {data_params}")
                    continue

                response_data['sensor'] = sensor
                response_data['station_id'] = station_id

                raw_responses.append(response_data)

                time.sleep(sleep_time)

        return raw_responses

    def transform(self, raw_data: list):
        df_raw = pd.concat(raw_data, ignore_index = True)
        df_pivot = df_raw.pivot(columns = "sensor", values = "VALUE", index = ["DATE", "station_id"]).reset_index()
        df_pivot.rename(columns = PROVINCE_RENAME, inplace = True)

        df_pivot['datetime'] = df_pivot['datetime'].map(lambda x: x.replace('CEST', '').replace('CET', ''))
        df_pivot['datetime'] = pd.to_datetime(df_pivot['datetime'], format = "%Y-%m-%dT%H:%M:%S")
        df_pivot['datetime'] = df_pivot['datetime'].dt.tz_localize(self.timezone).dt.tz_convert('UTC')

        # Sensors are available in different frequencies. Resample to 15min to have a single frequency for all
        df_pivot = df_pivot.groupby('station_id').resample("15min", on = 'datetime').mean().reset_index()

        return df_pivot

if __name__ == '__main__':

    start = datetime.datetime(2025, 1, 14)
    end = datetime.datetime(2025, 1, 16)

    pr_handler = ProvinceMeteo(timezone = 'CET')
    print(pr_handler.get_station_info("86900MS"))
    data = pr_handler.run(
        station_id = '86900MS',
        sensor_codes = ["LT"],
        start = start,
        end = end
    )

    print(data)