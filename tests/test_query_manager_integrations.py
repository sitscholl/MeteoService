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
import pytest_asyncio
from datetime import datetime
import numpy as np

import pytz
import pandas as pd

from src.runtime import RuntimeContext, load_config_file
from src.workflow import QueryWorkflow
from src.validation import TimeseriesQuery

tz = pytz.timezone("utc")

def dt(year: int, month: int, day: int, hour: int = 0, minute: int = 0, second: int = 0, tzinfo=tz):
    """Shortcut for building tz-aware datetimes."""
    naive = datetime(year, month, day, hour, minute, second)
    if hasattr(tzinfo, "localize"):
        return tzinfo.localize(naive)
    return naive.replace(tzinfo=tzinfo)

def _normalize_response(response, coerce_to_utc = False):
    if not response.data:
        return pd.DataFrame()
    df = pd.DataFrame(response.data)
    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
        
        if df['datetime'].dt.tz is None:
            df['datetime'] = df['datetime'].dt.tz_localize(response.metadata.get('result_timezone', 'UTC'))
        else:
            df['datetime'] = df['datetime'].dt.tz_convert(response.metadata.get('result_timezone', 'UTC'))
        
        if coerce_to_utc:
            df['datetime'] = df['datetime'].dt.tz_localize("UTC")
    df = df.sort_values("datetime").reset_index(drop=True)
    return df


def _expected_missing_count(start_time, end_time, freq, cache_start_utc, cache_end_utc):
    start_utc = start_time.astimezone(pytz.UTC)
    end_utc = end_time.astimezone(pytz.UTC)
    full_range_utc = pd.date_range(start=start_utc, end=end_utc, freq=freq, inclusive="both")
    missing = full_range_utc[(full_range_utc < cache_start_utc) | (full_range_utc > cache_end_utc)]
    return len(missing)

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

_PROVINCE_TEST_STATIONS = ["01110MS"] #, "23200MS", "89190MS"] #, "65350MS", "41000MS", "31410MS"]
_SBR_TEST_STATIONS = ["103"] #, "113", "96"] #, "137", "140", "12"]

@pytest.fixture(scope="session")
def runtime(tmp_path_factory):
    config = load_config_file("config/config.yaml")
    db_path = tmp_path_factory.mktemp("db") / "test.db"
    config.setdefault("database", {})["path"] = f"sqlite:///{db_path}"
    runtime_ctx = RuntimeContext(config=config, config_file="config/config.yaml")
    yield runtime_ctx
    runtime_ctx.db.close()

@pytest.fixture
def workflow(runtime):
    return QueryWorkflow(runtime)

@pytest.fixture(
    scope="session",
    params=[
        *[("province", station_id) for station_id in _PROVINCE_TEST_STATIONS],
        *[("sbr", station_id) for station_id in _SBR_TEST_STATIONS],
    ],
)
def provider_station(request):
    return request.param


@pytest.fixture(params=["Europe/Rome"])
def timezone_name(request):
    return request.param


@pytest_asyncio.fixture(scope="session")
async def primed_cache(runtime, provider_station):
    provider, station_id = provider_station
    workflow = QueryWorkflow(runtime)
    tz_utc = pytz.timezone("UTC")
    start_time = dt(2025, 6, 1, tzinfo=tz_utc)
    end_time = dt(2025, 7, 1, tzinfo=tz_utc)
    provider_handler = runtime.provider_manager.get_provider(provider)
    await _run_query(workflow, provider_handler, station_id, start_time, end_time, db=runtime.db)
    return provider, station_id


@pytest.mark.asyncio
async def test_provider_full_timeseries(primed_cache, timezone_name, runtime, workflow):
    provider, station_id = primed_cache
    tz = pytz.timezone(timezone_name)
    start_time = dt(2025, 6, 1, tzinfo = tz)
    end_time = dt(2025, 7, 1, tzinfo = tz)
    cache_start_utc = dt(2025, 6, 1, tzinfo=pytz.UTC)
    cache_end_utc = dt(2025, 7, 1, tzinfo=pytz.UTC)

    provider_handler = runtime.provider_manager.get_provider(provider)
    full_range = pd.date_range(start=start_time, end=end_time, freq=provider_handler.freq, inclusive="both")

    response, pending = await _run_query(workflow, provider_handler, station_id, start_time, end_time, db = runtime.db)
    
    response = _normalize_response(response)
    pending = pending if isinstance(pending, pd.DataFrame) else pd.DataFrame()

    assert len(response) == len(full_range)
    expected_missing = _expected_missing_count(
        start_time, end_time, provider_handler.freq, cache_start_utc, cache_end_utc
    )
    assert len(pending) == expected_missing
    assert response.datetime.min() == start_time
    assert response.datetime.max() == end_time
    assert str(response.datetime.dt.tz) == str(start_time.tzinfo)

@pytest.mark.asyncio
async def test_provider_with_gaps(primed_cache, timezone_name, runtime, workflow):
    provider, station_id = primed_cache
    tz = pytz.timezone(timezone_name)
    start_time = dt(2025, 6, 1, tzinfo = tz)
    end_time = dt(2025, 7, 1, tzinfo = tz)
    # cache_start_utc = dt(2025, 6, 1, tzinfo=pytz.UTC)
    # cache_end_utc = dt(2025, 7, 1, tzinfo=pytz.UTC)

    provider_handler = runtime.provider_manager.get_provider(provider)
    full_range = pd.date_range(start=start_time, end=end_time, freq=provider_handler.freq, inclusive="both")

    response, pending = await _run_query(workflow, provider_handler, station_id, start_time, end_time, db = runtime.db)

    response = _normalize_response(response)
    pending = pending if isinstance(pending, pd.DataFrame) else pd.DataFrame()

    assert len(response) == len(full_range)

    start_time2 = dt(2025, 5, 20, tzinfo = tz)
    end_time2 = dt(2025, 6, 10, tzinfo = tz)
    range2_1 = pd.date_range(start=start_time2, end=end_time2, freq=provider_handler.freq, inclusive="both")
    range2_2 = pd.date_range(start=start_time2, end=start_time, freq=provider_handler.freq, inclusive="both")

    response, pending = await _run_query(workflow, provider_handler, station_id, start_time2, end_time2, db = runtime.db)

    response = _normalize_response(response)
    pending = pending if isinstance(pending, pd.DataFrame) else pd.DataFrame()

    assert len(response) == len(range2_1)
    assert len(pending) == len(range2_2)-1 #overlap gets removed
    assert response.datetime.min() == start_time2
    assert response.datetime.max() == end_time2
    assert str(response.datetime.dt.tz) == str(start_time2.tzinfo)

    start_time3 = dt(2025, 6, 20, tzinfo = tz)
    end_time3 = dt(2025, 7, 10, tzinfo = tz)
    range3_1 = pd.date_range(start=start_time3, end=end_time3, freq=provider_handler.freq, inclusive="both")
    range3_2 = pd.date_range(start=start_time, end=end_time3, freq=provider_handler.freq, inclusive="both")

    response, pending = await _run_query(workflow, provider_handler, station_id, start_time3, end_time3, db = runtime.db)

    response = _normalize_response(response)
    pending = pending if isinstance(pending, pd.DataFrame) else pd.DataFrame()

    assert len(response) == len(range3_1)
    assert len(pending) == len(range3_2)-1
    assert response.datetime.min() == start_time3
    assert response.datetime.max() == end_time3
    assert str(response.datetime.dt.tz) == str(start_time3.tzinfo)

@pytest.mark.asyncio
async def test_provider_all_cached(primed_cache, timezone_name, runtime, workflow):
    provider, station_id = primed_cache
    tz = pytz.timezone(timezone_name)
    start_time = dt(2025, 6, 1, tzinfo = tz)
    end_time = dt(2025, 7, 1, tzinfo = tz)

    provider_handler = runtime.provider_manager.get_provider(provider)
    full_range = pd.date_range(start=start_time, end=end_time, freq=provider_handler.freq, inclusive="both")

    response, pending = await _run_query(workflow, provider_handler, station_id, start_time, end_time, db = runtime.db)

    response = _normalize_response(response)
    pending = pending if isinstance(pending, pd.DataFrame) else pd.DataFrame()

    assert len(response) == len(full_range)

    start_time2 = dt(2025, 6, 1, tzinfo=tz)
    end_time2 = dt(2025, 6, 10, tzinfo=tz)
    range2 = pd.date_range(start=start_time2, end=end_time2, freq=provider_handler.freq, inclusive="both")

    response, pending = await _run_query(workflow, provider_handler, station_id, start_time2, end_time2, db = runtime.db)

    response = _normalize_response(response)
    pending = pending if isinstance(pending, pd.DataFrame) else pd.DataFrame()

    assert len(response) == len(range2)
    assert pending.empty

@pytest.mark.asyncio
async def test_province_dst_changes(runtime, workflow):
    
    provider, station_id = "province", "01110MS"
    tz = pytz.timezone("Europe/Rome")
    start_time = dt(2025, 1, 1, tzinfo = tz)
    end_time = dt(2025, 12, 31, tzinfo = tz)

    provider_handler = runtime.provider_manager.get_provider(provider)
    full_range = pd.date_range(start=start_time, end=end_time, freq=provider_handler.freq, inclusive="both")

    response, pending = await _run_query(workflow, provider_handler, station_id, start_time, end_time)

    response_raw = pd.DataFrame(response.data)
    dt_offsets = response_raw.datetime.str.split('+', expand = True)[1].unique()
    assert np.array_equal(dt_offsets, ['01:00', '02:00'])
    
    response = _normalize_response(response)
    pending = pending if isinstance(pending, pd.DataFrame) else pd.DataFrame()

    assert len(response) == len(full_range)
    assert len(response) == len(pending)
    assert response.datetime.min() == start_time
    assert response.datetime.max() == end_time
    assert str(response.datetime.dt.tz) == str(start_time.tzinfo)

@pytest.mark.asyncio
async def test_latest_query(primed_cache, timezone_name, workflow, runtime):
    provider, station_id = primed_cache
    timezone = timezone_name

    latest_query = TimeseriesQuery(
        provider=provider,
        station_id=station_id,
        timezone=timezone,
    )

    latest_response, _ = await workflow.run_timeseries_query(latest_query, latest=True)
    data = pd.DataFrame(latest_response.data)
    
    assert len(data) > 0

    obs_cols = [c for c in data.columns if c not in {"station_id", "datetime", "model"}]
    if obs_cols:
        assert not data[obs_cols].isna().all(axis=None)

@pytest.mark.asyncio
async def test_invalid_station(primed_cache, runtime, workflow):
    provider, _ = primed_cache
    provider_handler = runtime.provider_manager.get_provider(provider)
    start_time = dt(2026,1,1)
    end_time = dt(2026,1,10)
    
    with pytest.raises(Exception):
        await _run_query(workflow, provider_handler, "INVALID_STATION", start_time, end_time)

@pytest.mark.asyncio
async def test_timezone_equivalence(primed_cache, workflow, runtime):
    provider, station_id = primed_cache
    utc_start = dt(2025, 10, 1, 0, 0, 0)
    utc_end = dt(2025, 10, 10, 0, 0, 0)

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

##Add tests using models argument (e.g. test raise when multiple models are fetched, test that open-meteo provider works for different models)
