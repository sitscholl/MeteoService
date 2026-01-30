import pandas as pd
from pandas.tseries.frequencies import to_offset
from pandas.api.types import is_datetime64_any_dtype

import logging
from datetime import datetime, timezone, tzinfo

from typing import List, Tuple

logger = logging.getLogger(__name__)

class Gapfinder:

    def __init__(self):
        pass

    def _build_daterange(self, start: datetime, end: datetime, freq: str, inclusive: str):

        start_time_aligned = pd.Timestamp(start).floor(freq)
        end_time_aligned = pd.Timestamp(end).floor(freq)

        return pd.date_range(
            start=start_time_aligned,
            end=end_time_aligned,
            freq=freq,
            inclusive=inclusive
        )

    def _delta_from_freq(self, freq: str):
        try:
            offset = to_offset(freq)
        except Exception as e:
            raise ValueError(f"Invalid frequency '{freq}': {e}")
        return pd.Timedelta(offset)

    def find_data_gaps(
        self,
        existing_dates: pd.DatetimeIndex,
        start: datetime,
        end: datetime,
        freq: str,
        inclusive: str = 'both',
        min_gap_duration: str = '30min',
        tz: str | tzinfo | None = None
    ) -> List[Tuple[datetime, datetime]]:

        """Find gaps in existing data for the requested time range."""

        if not is_datetime64_any_dtype(existing_dates):
            raise ValueError("Existing dates must be datetime-like")

        if not isinstance(existing_dates, pd.DatetimeIndex):
            raise ValueError(f"Existing dates must be a pandas DatetimeIndex. Got {type(existing_dates)}")

        if start.tzinfo is None or end.tzinfo is None:
            if tz is None:
                raise ValueError("Naive start/end datetimes are not allowed without an explicit timezone")
            start = pd.Timestamp(start).tz_localize(tz).to_pydatetime()
            end = pd.Timestamp(end).tz_localize(tz).to_pydatetime()

        if start.tzinfo != end.tzinfo:
            raise ValueError(f"start and end must be in the same timezone. Got {start.tzinfo} vs {end.tzinfo}")

        if end < start:
            raise ValueError(f"end must be >= start. Got {end} < {start}")

        try:
            min_gap_duration = pd.Timedelta(min_gap_duration)
            freq_delta = self._delta_from_freq(freq)

            complete_ts = self._build_daterange(start, end, freq, inclusive)

            if complete_ts.empty:
                return []

            if existing_dates.empty:
                return [(complete_ts[0], complete_ts[-1])]

            if existing_dates.tz is None:
                if tz is None:
                    raise ValueError("Naive existing_dates are not allowed without an explicit timezone")
                existing_dates = existing_dates.tz_localize(tz)

            target_tz = start.tzinfo or timezone.utc
            if existing_dates.tz != target_tz:
                existing_dates = existing_dates.tz_convert(target_tz)

            existing_dates = existing_dates.sort_values().unique()
            missing_ts = complete_ts.difference(existing_dates)

            if missing_ts.empty:
                return []

            gaps = []
            for gap_start, gap_end in self.derive_datetime_gaps(missing_ts.tolist(), freq=freq):

                coverage = (pd.Timestamp(gap_end) + freq_delta) - pd.Timestamp(gap_start)

                if coverage >= min_gap_duration:
                    gaps.append((gap_start, gap_end))

            return gaps

        except Exception:
            logger.exception("Error finding data gaps")
            return [(start, end)]  # Return full range as gap on error

    def derive_datetime_gaps(self, timestamps: list[datetime] | None, freq: str):
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
        freq_delta = self._delta_from_freq(freq)

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

    # def validate_date(self, date, target_format = "%d.%m.%Y"):
    #     ##Validate input dates
    #     try:
    #         if date != datetime.datetime.strptime(date, target_format).strftime(target_format):
    #             raise ValueError
    #     except ValueError:
    #         raise ValueError(f'Start date needs to be in {target_format} format. Got {date}')
