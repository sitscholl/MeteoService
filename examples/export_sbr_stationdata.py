##Warning: This example currently does not work. Use the request_sbr_stationdata example instead.

from webhandler.SBR import SBR
from tempfile import TemporaryDirectory
from pathlib import Path
from selenium import webdriver
import datetime
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('username', help = 'username')
parser.add_argument('password', help = 'password')
args = parser.parse_args()

download_dir = Path(TemporaryDirectory().name)
download_dir.mkdir(exist_ok = True, parents = True)
driver = webdriver.Chrome()

SBR = SBR(driver)
SBR.login(user = args.username, pwd = args.password)
sbr_files = SBR.export_stationdata(
    station_id="103",
    start=datetime.datetime(2025, 3, 20, 0, 0),
    end=datetime.datetime(2025, 3, 30, 14, 0),
    driver=driver
)

