import time
import datetime
import time
import sys
from pathlib import Path
from pytz import timezone
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)

def wait_for_download(download_dir, pattern, timeout=60, stability_interval=3):
    """
    Waits for a file download to complete in the specified directory.
    
      - If expected_filename is provided, it uses that as the search pattern.
      - Otherwise, it uses the provided extension (e.g., ".xlsx") as the pattern.
    
    It then waits until a matching file appears and its size remains stable for the
    given stability_interval, indicating that the file is no longer being written to.
    
    Parameters:
        download_dir (str or Path): Directory where downloads are saved.
        expected_filename (str, optional): Exact filename to wait for.
        extension (str, optional): File extension (include the dot, e.g. ".xlsx") to search for.
        timeout (int, optional): Maximum number of seconds to wait (default is 60).
        stability_interval (int, optional): Duration in seconds the file size must remain unchanged (default is 2).
    
    Returns:
        Path: The Path object of the downloaded file once detected and verified as complete.
    
    Raises:
        ValueError: If neither expected_filename nor extension is provided.
        SystemExit: Exits the program if the file isn’t detected or stabilized within the timeout.
    """

    download_dir = Path(download_dir)
    start_time = datetime.datetime.now(tz=timezone('Europe/Berlin'))

    while (datetime.datetime.now(tz=timezone('Europe/Berlin')) - start_time).total_seconds() < timeout:
        files = list(download_dir.glob(pattern))
        # filter out typical temporary files used during download.
        files = [f for f in files if not f.name.endswith(('.crdownload', '.part'))]
        logger.debug(f"Found the following files in download folder that match the pattern: {files}")

        if files:
            # Sort files by creation time (oldest first) and choose the most recent candidate.
            files.sort(key=lambda x: x.stat().st_ctime)
            candidate = files[-1]
            logger.debug(f"Candidate file: {candidate}")
            # Check if the candidate file's size remains stable.
            size1 = candidate.stat().st_size
            time.sleep(stability_interval)
            size2 = candidate.stat().st_size
            if size1 == size2:
                logger.debug(f"{size1}b equals {size2}b, returning {candidate}")
                return candidate
            logger.debug(f"{size1}b != {size2}b, waiting...")
        time.sleep(2)

    logger.warning("Download might not have completed within the expected time.")
    sys.exit(1)


def wait_for_page_stability(driver, check_interval=1, timeout=30):
    """
    Wait until the page's HTML stabilizes (i.e., doesn't change)
    for at least one check interval.
    """
    start = time.time()
    last_source = driver.execute_script("return document.documentElement.outerHTML")
    while time.time() - start < timeout:
        time.sleep(check_interval)
        current_source = driver.execute_script("return document.documentElement.outerHTML")
        if current_source == last_source:
            return True
        last_source = current_source
        logger.debug(f"Page still loading, waiting...")
    return False

def validate_date(date, target_format = "%d.%m.%Y"):
    ##Validate input dates
    try:
        if date != datetime.datetime.strptime(date, target_format).strftime(target_format):
            raise ValueError
    except ValueError:
        raise ValueError(f'Start date needs to be in {target_format} format. Got {date}')

def split_dates(start: datetime.datetime, end: datetime.datetime, target_format: str = "%d.%m.%Y"):

    if end < start:
        raise ValueError(f"Start date cannot be smaller than end date. Got {start} and {end}")

    if start.year != end.year:
        return(
            [start.strftime(target_format), datetime.datetime(start.year, 12, 31).strftime(target_format)],
            [datetime.datetime(end.year, 1, 1).strftime(target_format), end.strftime(target_format)]
        )
    else:
        return([start.strftime(target_format), end.strftime(target_format)])

@contextmanager
def temporary_implicit_wait(driver, wait_time):
    # Set the implicit wait to the new value
    driver.implicitly_wait(wait_time)
    try:
        yield
    finally:
        # Restore the original implicit wait value
        driver.implicitly_wait(30)
