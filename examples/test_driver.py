from pathlib import Path
import time
import shutil

from src.driver import Driver

download_dir = 'downloads'
Path(download_dir).mkdir(parents = True, exist_ok=True)
ddir_abs = str( Path.cwd() / download_dir ) #Make sure to use absolute path, otherwise it might fail 

try:
    with Driver(download_dir=ddir_abs) as driver:
        filename = "sample4.csv"
        driver.get(f"https://filesamples.com/samples/document/csv/{filename}")

        # Wait for the file to download, adjust if needed
        time.sleep(1)
finally:
    shutil.rmtree(download_dir)
