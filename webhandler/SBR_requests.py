import requests
import logging
import datetime

logger = logging.getLogger(__name__)

class SBR:
    login_url = "https://www3.beratungsring.org/mein-sbr/login"
    stationdata_url = "https://www3.beratungsring.org/wetterstationen-custom"
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
            
    def get_stationdata(self, station_id: int, start: datetime.datetime, end: datetime.datetime):

        if isinstance(station_id, str):
            station_id = int(station_id)
        if end < start:
            raise ValueError(f'End date must be smaller than start date. Got {start} - {end}')

        # Make the GET request
        data_params = {
            "web_page": f"user-stations/{station_id}",
            "graphType": "meteo",
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

        return self.session.get(self.stationdata_url, params=data_params, headers=data_headers)       