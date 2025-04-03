import requests
import logging
import datetime
import pandas as pd
import re
from webhandler.config import sbr_colmap

logger = logging.getLogger(__name__)

class SBR:
    base_url = "https://www3.beratungsring.org"
    login_url = base_url + "/mein-sbr/login"
    stationdata_url = base_url + "/wetterstationen-custom"
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"

    def __init__(self, username, password):
        self.username = username
        self.password = password
        self._session = None

    def __enter__(self):
        # Allows the object to be used in a 'with' block
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Ensure the session is closed when exiting the block
        self.close_session()   

    @property
    def session(self):

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
        if self._session is not None:
            self._session.close()
            self._session = None

    def _extract_data_from_response(self, text, pattern = r'let\s+dataSetOnLoad\s*=\s*prepareDataset\(\[\[(\{.*?\})\]\]\);', n_group = 1):
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

    def _get_formatted_tbl(self, rows):
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
                        
    def get_stationdata(self, station_id: int, start: datetime.datetime, end: datetime.datetime, type = 'meteo'):

        if isinstance(station_id, str):
            station_id = int(station_id)
        if end < start:
            raise ValueError(f'End date must be smaller than start date. Got {start} - {end}')

        # Make the GET request
        data_params = {
            "web_page": f"user-stations/{station_id}",
            "graphType": type,
            "skippath": "1",
            "id": "/wetterstationen-custom",
            "LANG": "",
            "datefrom": f"{start:%Y.%m.%d %H:%M}",
            "dateto": f"{end:%Y.%m.%d %H:%M}",
        }

        data_headers = {
            "User-Agent": self.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-US,en;q=0.9"
        }

        response = self.session.get(self.stationdata_url, params=data_params, headers=data_headers) 

        logger.debug(f"Response url: {response.request.url}")

        rows = self._extract_data_from_response(response.text)
        return self._get_formatted_tbl(rows) 