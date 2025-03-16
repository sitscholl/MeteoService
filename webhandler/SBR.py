import pandas as pd
import datetime
from pathlib import Path
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from .utils import wait_for_download, validate_date, split_dates
from .data import open_sbr_export
import logging

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

# Example page definitions:
class Home(SBRBase, page_name="home"):
    address = 'https://www3.beratungsring.org/'
    def load(self, driver):
        logger.info("Loading SBR Home Page")
        driver.get(self.address)

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

    def export_stationdata(self, station_name: str, start: str, end: str, driver, download_dir):
        logger.info('Exporting SBR Stationsdaten.')

        validate_date(start)
        validate_date(end)
        dates_split = split_dates(
            datetime.datetime.strptime(start, "%d.%m.%Y"),
            datetime.datetime.strptime(end, "%d.%m.%Y"),
        )

        if isinstance(station_name, str):
            station_name = [station_name]

        driver.find_element(By.XPATH, "//a[text()='Dienste']").click()
        driver.find_element(By.XPATH, "//a[text()='Wetterdaten']").click()
        driver.find_element(By.XPATH, "//a[text()='Wetterdaten exportieren']").click()

        exported_stations = []
        for start_date, end_date in dates_split:
            ##End date
            driver.find_element(By.ID, "datepicker_to").clear()
            driver.find_element(By.ID, "datepicker_to").send_keys(end_date)

            ##Start date
            driver.find_element(By.ID, "datepicker_from").clear()
            driver.find_element(By.ID, "datepicker_from").send_keys(start_date)

            ##Select Format
            st_select = Select(driver.find_element(By.NAME, 'ExportFormat'))
            st_select.select_by_visible_text('CSV-Datei')

            st_select = Select(driver.find_element(By.NAME, 'st_id'))

            for snam in station_name:
                ##Select station
                st_select.select_by_visible_text(snam)

                ##Export
                driver.find_element(By.NAME, "submit").click()

                # Make sure download finishes
                dfile = wait_for_download(download_dir, f"{snam.replace(' ', '_')}*.csv")
                exported_stations.append(open_sbr_export(dfile))
                Path(dfile).unlink()
                logger.info(f'Wetterdaten für station {snam} und Zeitraum {start_date} - {end_date} heruntergeladen.')

        return(pd.concat(exported_stations).sort_values(['wst_codice', 'datetime']))


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
        if not self.is_logged_in:
            self.go_to_page('home')

            self.driver.find_element(By.CSS_SELECTOR, "a.login-link").click()
            self.driver.find_element(By.ID, "s_username").send_keys(user)
            self.driver.find_element(By.ID, "s_password").send_keys(pwd)
            self.driver.find_element(By.XPATH, '//button[@type="submit"]').click()

            logger.info('SBR Anmeldung erfolgreich.')
        else:
            logger.info('Bereits bei SBR angemeldet.')

    def go_to_page(self, page_name: str):
        page_class = self.pages.get(page_name)

        if page_class is None:
            raise ValueError(f"Page '{page_name}' not found. Choose one of {list(self.pages.keys())}")

        page_instance = page_class()
        page_instance.load(driver = self.driver)
        return page_instance