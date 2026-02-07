"""
A small harness that executes multiple QueryManager calls covering:
    * overlapping date ranges
    * repeated queries on the same range (idempotency)
    * multiple stations
    * short ranges to keep API usage low

Use this script to quickly sanity check that inserts remain unique and that
gaps/placeholder rows behave as expected before deploying to production.
"""

import pytest
import asyncio
from datetime import datetime

import pytz
import pandas as pd

from src.runtime import RuntimeContext
from src.workflow import QueryWorkflow
from src.validation import TimeseriesQuery

tz = pytz.timezone("utc")

def dt(year: int, month: int, day: int, hour: int = 0, minute: int = 0, second: int = 0):
    """Shortcut for building UTC-aware datetimes."""
    return datetime(year, month, day, hour, minute, second, tzinfo=tz)

def _normalize_response(response, coerce_to_utc = False):
    if not response.data:
        return pd.DataFrame()
    df = pd.DataFrame(response.data)
    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], utc=coerce_to_utc, errors="coerce")
    df = df.sort_values("datetime").reset_index(drop=True)
    return df

async def _run_query(workflow, provider_handler, station_id, start_time, end_time, db = None):
    query = TimeseriesQuery(
        provider=provider_handler.provider_name,
        start_time=start_time,
        end_time=end_time,
        station_id=station_id,
    )
    response, pending = await workflow.run_timeseries_query(query)
    if db is not None and pending is not None and not pending.empty:
        await db.insert_data(pending, provider_handler)
    return response, pending

_PROVINCE_TEST_STATIONS = ["01110MS", "23200MS", "89190MS", "65350MS", "41000MS", "31410MS"]
_SBR_TEST_STATIONS = ["103", "113", "96", "137", "140", "12"]
_QUERY_COMBINATIONS = {
    "province": {
        "q1": {"start_time": dt(2025, 1, 1), "end_time": dt(2026, 1, 1), "station_ids": _PROVINCE_TEST_STATIONS},
        "q2": {"start_time": dt(2025, 2, 1), "end_time": dt(2025, 2, 10), "station_ids": _PROVINCE_TEST_STATIONS},
        "q3": {"start_time": dt(2025, 10, 1), "end_time": dt(2025, 10, 10), "station_ids": _PROVINCE_TEST_STATIONS},
        "q4": {"start_time": dt(2025, 12, 1), "end_time": dt(2026, 2, 1), "station_ids": _PROVINCE_TEST_STATIONS}
    },
    "sbr": {
        "q1": {"start_time": dt(2026, 1, 1), "end_time": dt(2026, 1, 10), "station_ids": _SBR_TEST_STATIONS},
        "q2": {"start_time": dt(2025, 12, 25), "end_time": dt(2026, 1, 2), "station_ids": _SBR_TEST_STATIONS},
        "q3": {"start_time": dt(2026, 1, 7), "end_time": dt(2026, 1, 14), "station_ids": _SBR_TEST_STATIONS}
    }
}

@pytest.fixture
def runtime():
    return RuntimeContext.from_config_file('config.yaml')

@pytest.fixture
def workflow():
    return QueryWorkflow(runtime())

@pytest.mark.asyncio
async def test_provider_full_timeseries(provider, station_id, timezone, runtime, workflow):
    tz = pytz(timezone)
    start_time = dt(2025, 1, 1, tzinfo = tz)
    end_time = dt(2026, 1, 1, tzinfo = tz)

    provider_handler = runtime.provider_manager.get_provider(provider)
    range = pd.date_range(start=start_time, end=end_time, freq=provider_handler.freq, inclusive="both")

    response, pending = await _run_query(workflow, provider_handler, station_id, start_time, end_time, db = runtime.db)
    
    response = _normalize_response(response)
    pending = _normalize_response(pending)

    assert len(response) == range
    assert len(pending) == range
    assert response.equals(pending)
    assert response.datetime.min() == start_time
    assert response.datetime.max() == end_time
    assert response.datetime.tz == start_time.tz

@pytest.mark.asyncio
async def test_provider_with_gaps(provider, station_id, timezone, runtime, workflow):
    tz = pytz(timezone)
    start_time = dt(2025, 1, 1, tzinfo = tz)
    end_time = dt(2026, 1, 1, tzinfo = tz)

    provider_handler = runtime.provider_manager.get_provider(provider)
    range = pd.date_range(start=start_time, end=end_time, freq=provider_handler.freq, inclusive="both")

    response, pending = await _run_query(workflow, provider_handler, station_id, start_time, end_time, db = runtime.db)

    response = _normalize_response(response)
    pending = _normalize_response(pending)

    assert len(response) == len(range)

    start_time2 = dt(2024, 12, 20, tzinfo = tz)
    end_time2 = dt(2025, 1, 10, tzinfo = tz)
    range2_1 = pd.date_range(start=start_time2, end=end_time2, freq=provider_handler.freq, inclusive="both")
    range2_2 = pd.date_range(start=start_time2, end=start_time, freq=provider_handler.freq, inclusive="both")

    response, pending = await _run_query(workflow, provider_handler, station_id, start_time2, end_time2, db = runtime.db)

    response = _normalize_response(response)
    pending = _normalize_response(pending)

    assert len(response) == len(range2_1)
    assert len(pending) == len(range2_2)
    assert response.datetime.min() == start_time2
    assert response.datetime.max() == end_time2
    assert response.datetime.tz == start_time2.tz

    start_time3 = dt(2025, 12, 20, tzinfo = tz)
    end_time3 = dt(2026, 1, 10, tzinfo = tz)
    range3_1 = pd.date_range(start=start_time3, end=end_time3, freq=provider_handler.freq, inclusive="both")
    range3_2 = pd.date_range(start=end_time, end=end_time3, freq=provider_handler.freq, inclusive="both")

    response, pending = await _run_query(workflow, provider_handler, station_id, start_time3, end_time3, db = runtime.db)

    response = _normalize_response(response)
    pending = _normalize_response(pending)

    assert len(response) == len(range3_1)
    assert len(pending) == len(range3_2)
    assert response.datetime.min() == start_time3
    assert response.datetime.max() == end_time3
    assert response.datetime.tz == start_time3.tz

@pytest.mark.asyncio
async def test_provider_all_cached(provider, station_id, timezone, runtime, workflow):
    tz = pytz(timezone)
    start_time = dt(2025, 1, 1, tzinfo = tz)
    end_time = dt(2026, 1, 1, tzinfo = tz)

    provider_handler = runtime.provider_manager.get_provider(provider)
    range = pd.date_range(start=start_time, end=end_time, freq=provider_handler.freq, inclusive="both")

    response, pending = await _run_query(workflow, provider_handler, station_id, start_time, end_time, db = runtime.db)

    response = _normalize_response(response)
    pending = _normalize_response(pending)

    assert len(response) == len(range)

    start_time2 = dt(2025, 3, 1)
    end_time2 = dt(2025, 9, 1)
    range2 = pd.date_range(start=start_time2, end=end_time2, freq=provider_handler.freq, inclusive="both")

    response, pending = await _run_query(workflow, provider_handler, station_id, start_time2, end_time2, db = runtime.db)

    response = _normalize_response(response)
    pending = _normalize_response(pending)

    assert len(response) == len(range2)
    assert pending.empty

@pytest.mark.asyncio
async def test_latest_query(provider, station_id, timezone, workflow):

    latest_query = TimeseriesQuery(
        provider=provider,
        station_id=station_id,
        timezone=timezone,
    )

    latest_response, _ = await workflow.run_timeseries_query(latest_query, latest=True)
    data = pd.DataFrame(latest_response.data)
    
    assert len(data) > 1

    obs_cols = [c for c in data.columns if c not in {"station_id", "datetime", "model"}]
    assert not data[obs_cols].isna().all(axis=None)

@pytest.mark.asyncio
async def test_invalid_station(provider, runtime):
    provider_handler = runtime.provider_manager.get_provider(provider)
    start_time = dt(2026,1,1)
    end_time = dt(2026,1,10)
    
    with pytest.raises(ValueError, match="invalid station"):
        response, pending = await _run_query(workflow, provider_handler, "INVALID_STATION", start_time, end_time)

@pytest.mark.asyncio
async def test_timezone_equivalence(station_id, provider, workflow, runtime):
    utc_start = dt(2025, 10, 1, 0, 0, 0)
    utc_end = dt(2025, 10, 2, 0, 0, 0)

    rome_tz = pytz.timezone("Europe/Rome")
    local_start = utc_start.astimezone(rome_tz)
    local_end = utc_end.astimezone(rome_tz)

    resp_utc, _ = await _run_query(
        workflow,
        runtime.provider_manager.get_provider(provider),
        station_id,
        utc_start,
        utc_end,
    )
    resp_local, _ = await _run_query(
        workflow,
        runtime.provider_manager.get_provider(provider),
        station_id,
        local_start,
        local_end,
    )

    df_utc = _normalize_response(resp_utc, coerce_to_utc=True)
    df_local = _normalize_response(resp_local, coerce_to_utc=True)

    assert df_utc.shape == df_local.shape
    assert df_utc.equals(df_local)
