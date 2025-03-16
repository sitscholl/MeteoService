# WebHandler
WebScraping classes for different websites


# Examples

## SBR

```
import os
from dotenv import load_dotenv
import time
from tempfile import TemporaryDirectory
from pathlib import Path
from selenium import webdriver

load_dotenv()

download_dir = Path(TemporaryDirectory().name)
download_dir.mkdir(exist_ok = True, parents = True)
driver = webdriver.Chrome(options=options)

SBR = SBR(driver)
SBR.login(user = os.environ.get('SBR_USERNAME'), pwd = os.environ.get('SBR_PASSWORD'))
time.sleep(3)
mySBR = SBR.go_to_page('mysbr')
sbr_files = mySBR.export_stationdata(
    station_name="Latsch 1",
    start="01.12.2024",
    end="16.03.2025",
    driver=driver,
    download_dir=download_dir,
)
```