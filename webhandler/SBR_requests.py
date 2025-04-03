import requests
import logging
import datetime
import pandas as pd
import re
import time
from webhandler.config import sbr_colmap
from webhandler.utils import split_dates

logger = logging.getLogger(__name__)

class SBR:
    """
    A class to interact with the SBR (Beratungsring) website for retrieving weather station data.
    Handles login, session management, and data extraction.
    """
    base_url = "https://www3.beratungsring.org"
    login_url = base_url + "/mein-sbr/login"
    stationdata_url = base_url + "/wetterstationen-custom"
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"

    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
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

    def _extract_data_from_response(
        self,
        text: str,
        pattern: str = r"let\s+dataSetOnLoad\s*=\s*prepareDataset\(\[\[(\{.*?\})\]\]\);",
        n_group: int = 1,
    ) -> list[dict]:
        """
        Extracts data from the HTML response text based on a given pattern.

        Args:
            text (str): The HTML response text.
            pattern (str): The regex pattern to search for.
            n_group (int): The group number to extract from the regex match.
        Returns:
            list: A list of dictionaries, where each dictionary represents a row of data.
        """
        p_match = re.search(pattern, text, re.DOTALL)
        if not p_match:
            raise ValueError(f"Could not find pattern {pattern} in the text.")

        text = p_match.group(n_group)
        data = [i.strip('}').strip('{').split(',') for i in text.split('},{')]

        rows = []
        for n in data:
            row = {j.split(':')[0]: j.split(':')[1] for j in n}
            rows.append(row)
        return rows

    def _get_formatted_tbl(self, rows: list[dict]) -> pd.DataFrame:
        """
        Formats the extracted data into a pandas DataFrame.

        Args:
            rows (list): A list of dictionaries, where each dictionary represents a row of data.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the formatted data.
        """
        tbl = pd.DataFrame.from_dict(rows)
        tbl.rename(columns = lambda x: x.strip('"'), inplace = True)
        tbl.rename(columns = lambda x: sbr_colmap[x]['kuerzel_de'] if x in sbr_colmap.keys() else x, inplace = True)
        tbl.rename(columns = {'x': 'Datum'}, inplace = True)

        if 'create_time' in tbl.columns:
            tbl['create_time'] = tbl['create_time'].map(lambda x: datetime.datetime.fromtimestamp(int(x)))
        if 'rainStart' in tbl.columns:
            tbl['rainStart'] = tbl['rainStart'].map(lambda x: x.strip('"'))
            tbl['rainStart'] = pd.to_datetime(tbl['rainStart'], format = '%Y-%m-%d %H')
        tbl['station_id'] = tbl['station_id'].map(lambda x: x.strip('"')) 

        tbl['Datum'] = tbl['Datum'].map(lambda x: datetime.datetime.fromtimestamp(int(x)))
        return tbl

    def get_stationdata(self, station_id: int, start: datetime.datetime, end: datetime.datetime, type: str = 'meteo', sleep: int = 1) -> pd.DataFrame:
        """
        Retrieves weather station data for a given station ID and date range.

        Args:
            station_id (int): The ID of the weather station.
            start (datetime.datetime): The start date and time.
            end (datetime.datetime): The end date and time.
            type (str): The type of data to retrieve (default: 'meteo'). Can be one of 'meteo' or 'schorf'
            sleep (int): The time to sleep between requests (in seconds).

        Returns:
            pd.DataFrame: A pandas DataFrame containing the weather station data.

        """

        if isinstance(station_id, str):
            station_id = int(station_id)
        if end < start:
            raise ValueError(f'End date must be smaller than start date. Got {start} - {end}')

        data_headers = {
            "User-Agent": self.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-US,en;q=0.9"
        }

        dates_split = split_dates(start, end, n_days = 7)

        tbl = []
        for start_date, end_date in dates_split:
            # Make the GET request
            data_params = {
                "web_page": f"user-stations/{station_id}",
                "graphType": type,
                "skippath": "1",
                "id": "/wetterstationen-custom",
                "LANG": "",
                "datefrom": f"{start_date:%Y.%m.%d %H:%M}",
                "dateto": f"{end_date:%Y.%m.%d %H:%M}",
            }

            response = self.session.get(self.stationdata_url, params=data_params, headers=data_headers) 

            logger.debug(f"Response url: {response.request.url}")

            rows = self._extract_data_from_response(response.text)
            tbl.append(self._get_formatted_tbl(rows))

            time.sleep(sleep) #avoid too many requests in short time

        return(pd.concat(tbl))
