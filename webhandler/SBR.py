import pandas as pd
import datetime
from pathlib import Path
import io
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from .utils import wait_for_download, validate_date, split_dates, scroll_element
from .data import open_sbr_export
import logging
import warnings

logger = logging.getLogger(__name__)

class SBRBase:
    # Registry to hold all page classes
    registry = {}

    def __init_subclass__(cls, page_name=None, **kwargs):
        
        if page_name is None:
            page_name = cls.__name__.lower()
        cls.page_name = page_name

        SBRBase.registry[page_name] = cls

    def load(self):
        """Each page must implement its own load method."""
        raise NotImplementedError("Subclasses must implement this method.")

# Central navigator that uses the registry:
class SBR:
    def __init__(self, driver):
        self.driver = driver
        self.pages = SBRBase.registry  # All pages are registered here

    @property
    def is_logged_in(self):
        if self.driver.current_url != self.pages.get('home').address:
            self.go_to_page('home')
        if self.driver.find_elements(By.CSS_SELECTOR, "a.login-link")[0].text == 'personLOGIN':
            return False
        elif self.driver.find_elements(By.CSS_SELECTOR, "a.login-link")[0].text == 'personMEIN SBR':
            return True
        else:
            raise ValueError(
                f'Logged in status text could not be matched. Got {self.driver.find_elements(By.CSS_SELECTOR, "a.login-link")[0].text}'
            )

    def login(self, user, pwd):
        self.driver.get("https://www3.beratungsring.org/mein-sbr/login")

        self.driver.find_element(By.CSS_SELECTOR, "a.login-link").click()
        self.driver.find_element(By.ID, "s_username").send_keys(user)
        self.driver.find_element(By.ID, "s_password").send_keys(pwd)
        self.driver.find_element(By.XPATH, '//button[@type="submit"]').click()

        logger.info('SBR Anmeldung erfolgreich.')
    # else:
    #     logger.info('Bereits bei SBR angemeldet.')

    def go_to_page(self, page_name: str):
        page_class = self.pages.get(page_name)

        if page_class is None:
            raise ValueError(f"Page '{page_name}' not found. Choose one of {list(self.pages.keys())}")

        page_instance = page_class()
        page_instance.load(driver = self.driver)
        return page_instance

    def export_stationdata(self, station_id: str, start: datetime.datetime, end: datetime.datetime, driver):
        """
        Downloads station data by opening website in selenium, progressively scrolling down the data table and extract the data as it loads.
        """

        logger.info('Exporting SBR Stationsdaten.')
        meteodata_url = "https://www3.beratungsring.org/wetterstationen-custom?web_page=user-stations/{station_id}&graphType=meteo&skippath=1&id=%2Fwetterstationen-custom&LANG=&datefrom={start}&dateto={end}#meteo-graphs"
        datefmt = '%Y-%m-%d+%H:%M'

        if not isinstance(start, datetime.datetime) or not isinstance(end, datetime.datetime):
            raise ValueError(f"Start and end dates must be datetime objects. Got {type(start)} and {type(end)}")

        dates_split = split_dates(start, end, n_days = 3)

        if isinstance(station_id, str) or isinstance(station_id, int):
            station_id = [station_id]

        exported_stations = []
        for sid in station_id:
            for start_date, end_date in dates_split:
                driver.get(
                    meteodata_url.format(
                        station_id=sid,
                        start=start_date.strftime(datefmt),
                        end=end_date.strftime(datefmt),
                    )
                )

                scrollable_path = "//div[contains(@class, 'scrollarea clusterize-scroll')]"
                WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.XPATH, scrollable_path)),
                )
                scrollable_table = driver.find_element(
                    By.XPATH, scrollable_path
                )

                table_data = []
                for tbl_scrolled in scroll_element(
                    scrollable_table, driver, scroll_factor=6
                ):
                    table_element = driver.find_element(
                        By.XPATH, "//table[contains(@class, 'clusterize-content')]"
                    )
                    table_html = table_element.get_attribute("outerHTML")
                    table_data.append(pd.read_html(io.StringIO(table_html))[0].dropna(how = 'all'))
                table_data = pd.concat(table_data).drop_duplicates()

                table_data["Zeit"] = pd.to_datetime(
                    table_data["Zeit"], format="%d.%m.%y - %H:%M"
                )
                num_cols = table_data.select_dtypes("number").columns
                table_data[num_cols] = table_data[num_cols] / 10
                table_data["st_id"] = sid

                exported_stations.append(table_data)

                logger.info(f'Wetterdaten für station {sid} und Zeitraum {start_date} - {end_date} heruntergeladen.')

        return(pd.concat(exported_stations).sort_values(['sid', 'Zeit']).reset_index(drop = True))


# Page definitions:
class Home(SBRBase, page_name="home"):
    address = 'https://www3.beratungsring.org/'
    def load(self, driver):
        logger.info("Loading SBR Home Page")
        driver.get(self.address)

        return self

class MySBR(SBRBase, page_name='mysbr'):
    def load(self, driver):
        logger.info("Loading MySBR Page")
        login_element = driver.find_element(By.CSS_SELECTOR, "a.login-link")

        if login_element.text == 'personLOGIN':
            raise ValueError('Need to log in before going to MySBR')

        driver.find_element(By.CSS_SELECTOR, "a.login-link").click()
        driver.find_element(By.XPATH, "//a[text()='Beratungsbestätigungen']").click()
        driver.switch_to.window(driver.window_handles[-1])

        return self
