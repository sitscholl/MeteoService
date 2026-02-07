from unittest.mock import AsyncMock, patch
from datetime import datetime, timezone

import pandas as pd
import pytest

from src.query_manager import QueryManager


def dt_utc(year, month, day, hour=0, minute=0, second=0):
    return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)


def make_existing_df(index, station_id="STATION_1", model=""):
    df = pd.DataFrame(
        {
            "station_id": station_id,
            "model": model,
            "temp": range(len(index)),
        },
        index=pd.DatetimeIndex(index),
    )
    return df


class FakeDB:
    def __init__(self, existing_df=None):
        self._existing_df = existing_df if existing_df is not None else pd.DataFrame()

    def query_data(self, provider, station_id, start_time, end_time, variables=None, weather_models=None):
        if self._existing_df.empty:
            return pd.DataFrame()
        mask = (self._existing_df.index >= start_time) & (self._existing_df.index <= end_time)
        return self._existing_df.loc[mask].copy()


class FakeProvider:
    def __init__(self, provider_name="province", freq="1h", inclusive="both"):
        self.provider_name = provider_name
        self.freq = freq
        self.inclusive = inclusive
        self.latest_window_minutes = 60
        self.run = AsyncMock(side_effect=self._run)

    async def _run(self, start, end, data_type, station_id, models=None):
        if start >= end:
            return pd.DataFrame()
        if models is None:
            models = [""]
        model = models[0]
        rng = pd.date_range(start=pd.Timestamp(start), end=pd.Timestamp(end), freq=self.freq, inclusive="both")
        if rng.empty:
            return pd.DataFrame()
        return pd.DataFrame(
            {
                "datetime": rng,
                "station_id": station_id,
                "model": model,
                "temp": range(len(rng)),
            }
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


@pytest.fixture()
def manager():
    return QueryManager()


@pytest.fixture()
def provider():
    return FakeProvider()


@pytest.mark.asyncio
async def test_get_data_returns_existing_when_no_gaps(manager, provider):
    start_time = dt_utc(2025, 1, 1, 0)
    end_time = dt_utc(2025, 1, 1, 5)
    full_range = pd.date_range(start=start_time, end=end_time, freq=provider.freq, inclusive="both")
    existing_df = make_existing_df(full_range)
    db = FakeDB(existing_df)

    combined, pending = await manager.get_data(
        db=db,
        provider_handler=provider,
        station_id="STATION_1",
        start_time=start_time,
        end_time=end_time,
    )

    assert pending.empty
    assert len(combined) == len(full_range)
    provider.run.assert_not_called()


@pytest.mark.asyncio
async def test_get_data_fetches_only_missing_gaps(manager, provider):
    start_time = dt_utc(2025, 1, 1, 0)
    end_time = dt_utc(2025, 1, 1, 5)
    full_range = pd.date_range(start=start_time, end=end_time, freq=provider.freq, inclusive="both")
    existing_range = full_range[:4]  # missing the last two timestamps
    existing_df = make_existing_df(existing_range)
    db = FakeDB(existing_df)

    combined, pending = await manager.get_data(
        db=db,
        provider_handler=provider,
        station_id="STATION_1",
        start_time=start_time,
        end_time=end_time,
    )

    assert len(combined) == len(full_range)
    assert len(pending) == 2
    provider.run.assert_called_once()
    called_start = provider.run.call_args.kwargs["start"]
    called_end = provider.run.call_args.kwargs["end"]
    assert pd.Timestamp(called_start) == full_range[4]
    assert pd.Timestamp(called_end) == full_range[5]


@pytest.mark.asyncio
async def test_get_data_fetches_full_range_when_cache_empty(manager, provider):
    db = FakeDB(pd.DataFrame())

    start_time = dt_utc(2025, 2, 1, 0)
    end_time = dt_utc(2025, 2, 1, 3)
    full_range = pd.date_range(start=start_time, end=end_time, freq=provider.freq, inclusive="both")

    combined, pending = await manager.get_data(
        db=db,
        provider_handler=provider,
        station_id="STATION_1",
        start_time=start_time,
        end_time=end_time,
    )

    assert len(combined) == len(full_range)
    assert len(pending) == len(full_range)
    provider.run.assert_called_once()


def test_round_range_caps_future_end(manager):
    fixed_now = dt_utc(2025, 1, 2, 12)
    start_time = dt_utc(2025, 1, 2, 10)
    end_time = dt_utc(2025, 1, 3, 10)

    with patch("src.query_manager.datetime", wraps=datetime) as mock_datetime:
        mock_datetime.now.return_value = fixed_now
        start_round, end_round = manager._round_range_to_freq(start_time, end_time, freq="1h")

    assert start_round == pd.Timestamp(start_time).floor("1h")
    assert end_round == pd.Timestamp(fixed_now).floor("1h")
