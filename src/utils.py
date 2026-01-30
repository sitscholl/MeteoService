import pandas as pd

import datetime
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

def validate_date(date, target_format = "%d.%m.%Y"):
    ##Validate input dates
    try:
        if date != datetime.datetime.strptime(date, target_format).strftime(target_format):
            raise ValueError
    except ValueError:
        raise ValueError(f'Start date needs to be in {target_format} format. Got {date}')

def split_dates(start_date, end_date, freq, n_days=7, split_on_year=False):
    """
    freq: string (e.g., '1h', '15min') or pd.Timedelta
    """
    if end_date < start_date:
        raise ValueError(f"Start date cannot be smaller than end date. Got {start_date} and {end_date}")

    # Convert freq to a Timedelta for easy math
    freq_delta = pd.Timedelta(freq)
    date_pairs = []
    current_start = start_date
    
    while current_start <= end_date:
        # 1. Calculate the normal step (e.g., 7 days)
        # Note: We subtract one frequency unit from the potential end 
        # so that the chunk spans n_days TOTAL including the start and end.
        potential_end = current_start + datetime.timedelta(days=n_days) - freq_delta
        
        if split_on_year:
            # 2. Calculate the very last possible timestamp of the current year
            # (December 31st, 23:59:59... or whatever the last 'freq' step is)
            next_year_start = datetime.datetime(current_start.year + 1, 1, 1, tzinfo=current_start.tzinfo)
            last_of_year = next_year_start - freq_delta
            
            # 3. Pick the earliest of the three boundaries
            current_end = min(potential_end, end_date, last_of_year)
        else:
            current_end = min(potential_end, end_date)

        # Safety: check we didn't go backwards
        if current_end < current_start:
            # This can happen if start_date is already the last timestamp of the year
            # Force the end to be the start so we at least get one record
            current_end = current_start

        date_pairs.append((current_start, current_end))
        
        # 4. MOVE TO THE NEXT TIMESTAMP
        # The next start is exactly one frequency step after the current end
        current_start = current_end + freq_delta
    
    return date_pairs
