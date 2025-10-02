import pandas as pd
import json

import requests
import logging
import datetime
import numpy as np
import re
import time
from typing import Any, Dict, List
import pytz
from pathlib import Path

from ..config import sbr_colmap
from ..utils import split_dates
from .base import BaseMeteoHandler

logger = logging.getLogger(__name__)

# ['datetime', 'create_time', 'tair_2m', 'tsoil_25cm', 'precipitation',
#        'wind_speed', 'wind_gust', 'relative_humidity', 'irrigation',
#        'tdry_60cm', 'twet_60cm', 'mg22', 'mg23', 'station_id', 'Ausf.']
SBR_RENAME = {
        "Datum": "datetime",
        "T2m": "tair_2m",
        "TB -25cm": "tsoil_25cm",
        "Tt": "tdry_60cm",
        "Tf": "twet_60cm",
        "RL": "relative_humidity",
        "Wind": "wind_speed",
        "Wg  max": "wind_gust",
        "Nied.": "precipitation",
        "Ber.": "irrigation",
        "Bn": "leaf_wetness",
        "BMp": "millsperiode_start",
        "rainStart": "rain_start"
}

class SBRMeteo(BaseMeteoHandler):
    """
    A class to interact with the SBR (Beratungsring) website for retrieving weather station data.
    Handles login, session management, and data extraction.
    """

    provider_name = 'sbr'

    base_url = "https://www3.beratungsring.org"
    login_url = base_url + "/mein-sbr/login"
    timeseries_url = base_url + "/wetterstationen-custom"
    stations_url = 'data/sbr.geojson'
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"

    def __init__(self, username: str, password: str, timezone: str, **kwargs):
        self.username = username
        self.password = password
        self.timezone = timezone
        self._session = None

    def __enter__(self):
        """
        Allows the object to be used in a 'with' block.
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Ensure the session is closed when exiting the 'with' block.
        """
        self.close_session()

    @property
    def session(self):
        """
        Manages the session with the SBR website, handling login if necessary.
        """
        if self._session is None:
            self._session = requests.Session()
            login_payload = {
                "ajaxform": "84",
                "login": "84",
                "path": "",
                "username": self.username,
                "password": self.password,
                "remember": "1"
            }

            # Headers (some websites require headers to mimic a browser request)
            login_headers = {
                "User-Agent": self.user_agent,
                "Referer": self.login_url,
                "Origin": "https://www3.beratungsring.org"
            }

            # Send the POST request with form data
            login_response = self._session.post(self.login_url, data=login_payload, headers=login_headers)
            login_response.raise_for_status()

            # Check if login was successful
            if "Logindaten vergessen" in login_response.text:
                raise ValueError("Login failed! Check your credentials.")
            else:
                logger.debug('Login successful!')

        return self._session

    def close_session(self):
        """
        Closes the current session if it exists.
        """
        if self._session is not None:
            self._session.close()
            self._session = None

    def get_station_info(self, station_id: str) -> dict[str, Any]:
        """
        Get station information from local file, as no API available
        """

        station_info_file = Path(self.stations_url)
        if not station_info_file.exists():
            logger.warning(f"File {station_info_file} does not exist. Cannot fetch SBR station info.")
            return {}

        if isinstance(station_id, int):
            station_id = str(station_id)

        with station_info_file.open("r", encoding="utf-8") as file:
            response_data = json.load(file)

        station_info = [i for i in response_data['features'] if i['properties']['st_id'] == station_id]

        if len(station_info) == 0 :
            logger.warning(f"No metadata found for {station_id}")
            return {}
        
        station_props = station_info[0]['properties']
        return {
            'lat': station_props.get('lat'),
            'lon': station_props.get('lon'),
            'elevation': None,
            'name': station_props.get('st_name')
        }

    def get_data(
            self,
            station_id: str | int,
            start: datetime.datetime,
            end: datetime.datetime,
            data_type: str,
            sleep_time: int = 1,
            request_batch_size = 7,
            **kwargs
        ) -> List[str]:
        """
        Query the raw data from the SBR website.

        Args:
            **kwargs: Parameters for data retrieval including:
                - station_id (int): The ID of the weather station
                - start (datetime.datetime): The start date and time (must be timezone-aware)
                - end (datetime.datetime): The end date and time (must be timezone-aware)
                - type (str): The type of data to retrieve (default: 'meteo')
                - sleep (int): The time to sleep between requests (default: 1)

        Returns:
            List[str]: List of raw HTML response texts from the website
        """

        # Validate timezone awareness
        if start.tzinfo is None:
            raise ValueError("start datetime must be timezone-aware")
        if end.tzinfo is None:
            raise ValueError("end datetime must be timezone-aware")

        start = start.astimezone(pytz.timezone(self.timezone))
        end = end.astimezone(pytz.timezone(self.timezone))
        
        if isinstance(station_id, str):
            station_id = int(station_id)
        if end < start:
            raise ValueError(f'End date must be after start date. Got {start} - {end}')

        data_headers = {
            "User-Agent": self.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-US,en;q=0.9"
        }

        dates_split = split_dates(start, end, n_days=request_batch_size)
        raw_responses = []
        
        for start_date, end_date in dates_split:
            # Make the GET request
            data_params = {
                "web_page": f"user-stations/{station_id}",
                "graphType": data_type,
                "skippath": "1",
                "id": "/wetterstationen-custom",
                "LANG": "",
                "datefrom": f"{start_date:%Y.%m.%d %H:%M}",
                "dateto": f"{end_date:%Y.%m.%d %H:%M}",
            }

            response = self.session.get(self.timeseries_url, params=data_params, headers=data_headers)
            response.raise_for_status()
            
            logger.debug(f"Response url: {response.request.url}")
            raw_responses.append(response.text)
            
            time.sleep(sleep_time)  # avoid too many requests in short time
            
        return raw_responses

    def transform(self, raw_data: List[str]) -> pd.DataFrame:
        """
        Transform the raw HTML responses into a standardized DataFrame format.
        
        Args:
            raw_data: List of raw HTML response texts
            
        Returns:
            pd.DataFrame: Transformed data in standardized format
        """
        if not raw_data:
            return pd.DataFrame()
            
        dataframes = []
        for response_text in raw_data:
            rows = self._extract_data_from_response(response_text)
            if rows:  # Only process if we have data
                df = self._get_formatted_tbl(rows)
                dataframes.append(df)
        
        if not dataframes:
            return pd.DataFrame()
            
        return pd.concat(dataframes, ignore_index=True).rename(columns = SBR_RENAME)

    def _extract_data_from_response(
        self,
        text: str,
        pattern: str = r"let\s+dataSetOnLoad\s*=\s*prepareDataset\(\[\[(\{.*?\})\]\]",
        n_group: int = 1,
    ) -> List[Dict[str, str]]:
        """
        Extracts data from the HTML response text based on a given pattern.

        Args:
            text (str): The HTML response text.
            pattern (str): The regex pattern to search for.
            n_group (int): The group number to extract from the regex match.
            
        Returns:
            List[Dict[str, str]]: A list of dictionaries, where each dictionary represents a row of data.
        """
        p_match = re.search(pattern, text, re.DOTALL)
        if not p_match:
            logger.warning(f"Could not find pattern {pattern} in the text.")
            return []

        text = p_match.group(n_group)
        data = [i.strip('}').strip('{').split(',') for i in text.split('},{')]

        rows = []
        for n in data:
            try:
                row = {j.split(':')[0]: j.split(':')[1] for j in n if ':' in j}
                if row:  # Only add non-empty rows
                    rows.append(row)
            except (IndexError, ValueError) as e:
                logger.warning(f"Error parsing row data: {e}")
                continue
                
        return rows

    def _assign_dtype(self, tbl: pd.DataFrame) -> pd.DataFrame:
        """
        Assigns appropriate data types to DataFrame columns based on sbr_colmap.
        
        Args:
            tbl: Input DataFrame
            
        Returns:
            pd.DataFrame: DataFrame with corrected data types
        """
        tbl_re = tbl.copy()
        for col in tbl_re.columns:
            unit = sbr_colmap.get(col, {'einheit': ''})['einheit']
            try:
                if unit in ['mm', 'degC', '%', 'm*s-1']:
                    tbl_re[col] = tbl_re[col].astype(np.float64)
                elif unit == 'Ein/Aus':
                    tbl_re[col] = tbl_re[col].astype(bool).astype(int)
                else:
                    tbl_re[col] = tbl_re[col].astype(str)
            except (ValueError, TypeError) as e:
                logger.warning(f"Error converting column {col} to appropriate dtype: {e}")
                tbl_re[col] = tbl_re[col].astype(str)
                
        return tbl_re

    def _get_formatted_tbl(self, rows: List[Dict[str, str]]) -> pd.DataFrame:
        """
        Formats the extracted data into a pandas DataFrame.

        Args:
            rows: A list of dictionaries, where each dictionary represents a row of data.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the formatted data.
        """
        if not rows:
            return pd.DataFrame()
            
        tbl = pd.DataFrame.from_dict(rows)
        tbl.rename(columns=lambda x: x.strip('"'), inplace=True)
        tbl = self._assign_dtype(tbl)
        tbl.rename(columns=lambda x: sbr_colmap[x]['kuerzel_de'] if x in sbr_colmap.keys() else x, inplace=True)
        tbl.rename(columns={'x': 'Datum'}, inplace=True)

        # Handle timestamp conversions with error handling
        try:
            if 'create_time' in tbl.columns:
                tbl['create_time'] = tbl['create_time'].apply(
                    lambda x: datetime.datetime.fromtimestamp(int(x), tz=pytz.timezone(self.timezone)) if pd.notna(x) else pd.NaT
                )
                # Convert to UTC for consistency
                tbl['create_time'] = tbl['create_time'].dt.tz_convert('UTC')
        except (ValueError, TypeError) as e:
            logger.warning(f"Error converting create_time: {e}")

        try:
            if 'rainStart' in tbl.columns:
                tbl['rainStart'] = tbl['rainStart'].apply(lambda x: x.strip('"') if pd.notna(x) else x)
                tbl['rainStart'] = pd.to_datetime(tbl['rainStart'], format='%Y-%m-%d %H', errors='coerce')
                tbl['rainStart'] = tbl['rainStart'].dt.tz_localize(tz = self.timezone).dt.tz_convert('UTC')
        except (ValueError, TypeError) as e:
            logger.warning(f"Error converting rainStart: {e}")

        try:
            if 'station_id' in tbl.columns:
                tbl['station_id'] = tbl['station_id'].apply(lambda x: x.strip('"') if pd.notna(x) else x)
        except (ValueError, TypeError) as e:
            logger.warning(f"Error processing station_id: {e}")

        try:
            if 'Datum' in tbl.columns:
                tbl['Datum'] = tbl['Datum'].apply(
                    lambda x: datetime.datetime.fromtimestamp(int(x)) if pd.notna(x) else pd.NaT
                )
                tbl['Datum'] = tbl['Datum'].dt.tz_localize(tz=self.timezone).dt.tz_convert('UTC')
        except (ValueError, TypeError) as e:
            logger.warning(f"Error converting Datum: {e}")

        return tbl


if __name__ == "__main__":

    sbr_handler = SBRMeteo('a', 'b', timezone = 'CET')
    print(sbr_handler.get_station_info('-3'))