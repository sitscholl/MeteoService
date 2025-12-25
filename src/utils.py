import pandas as pd

import time
import datetime
from datetime import timedelta
import time
import sys
from pathlib import Path
from pytz import timezone
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)

def derive_datetime_gaps(timestamps, freq):
    """
    Groups a list of timestamp objects into consecutive gaps based on the given frequency.

    Parameters:
        timestamps (list): A list of datetime objects representing the missing timestamps in a series.
        freq (str): The frequency string (e.g., 'D', 'H', 'T') of the original timeseries.

    Returns:
        list: A list of tuples, where each tuple represents the start and end of a gap.
              Each tuple contains Python datetime.datetime objects.
    """

    if not timestamps:
        return []

    # Normalize timestamps to pandas.Timestamp for reliable arithmetic/comparison
    pd_timestamps = [pd.Timestamp(ts) for ts in timestamps]
    pd_timestamps.sort()

    # Convert frequency string to a Timedelta/offset
    try:
        offset = pd.tseries.frequencies.to_offset(freq)
    except Exception as e:
        raise ValueError(f"Invalid frequency '{freq}': {e}")
    freq_delta = pd.Timedelta(offset)

    gaps = []
    gap_start = pd_timestamps[0]
    gap_end = pd_timestamps[0]

    for current_ts in pd_timestamps[1:]:
        expected_next = gap_end + freq_delta
        if current_ts == expected_next:
            # consecutive, extend current gap
            gap_end = current_ts
        else:
            # non-consecutive, close current gap and start a new one
            gaps.append((gap_start.to_pydatetime(), gap_end.to_pydatetime()))
            gap_start = current_ts
            gap_end = current_ts

    # append the final gap
    gaps.append((gap_start.to_pydatetime(), gap_end.to_pydatetime()))
    return gaps

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

def split_dates(start_date, end_date, n_days = 7, split_on_year = False):
    """
    Create a list of (start, end) date tuples that each span n_days, 
    covering the period from start_date to end_date.
    
    Parameters:
    - start_date: A date object or a string in "YYYY-MM-DD" format.
    - end_date: A date object or a string in "YYYY-MM-DD" format.
    - n_days: Number of days in each interval.
    
    Returns:
    - List of tuples, where each tuple is (start_date, end_date) for that interval.
    """

    if end_date < start_date:
        raise ValueError(f"Start date cannot be smaller than end date. Got {start_date} and {end_date}")

    date_pairs = []
    current_start = start_date
    
    while current_start < end_date:
        potential_end = current_start + timedelta(days=n_days)
        current_end = min(potential_end, end_date)

        if split_on_year and (current_end.year != current_start.year):
            current_end = datetime.datetime(current_start.year, 12, 31, tzinfo = current_start.tzinfo)

        date_pairs.append((current_start, current_end))
        current_start = current_end
    
    return date_pairs

@contextmanager
def temporary_implicit_wait(driver, wait_time):
    # Set the implicit wait to the new value
    driver.implicitly_wait(wait_time)
    try:
        yield
    finally:
        # Restore the original implicit wait value
        driver.implicitly_wait(30)
