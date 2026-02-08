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
from datetime import datetime, timedelta
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
        df["datetime"] = pd.to_datetime(df["datetime"], utc = True)
        
        df['datetime'] = df['datetime'].dt.tz_convert(response.metadata.get('result_timezone', 'UTC'))
        
        if coerce_to_utc and df['datetime'].dt.tz != pytz.timezone('UTC'):
            df['datetime'] = df['datetime'].dt.tz_convert("UTC")

    df = df.sort_values("datetime").reset_index(drop=True)
    return df

def _round_start_end_to_freq(start, end, freq):
    start_round = pd.Timestamp(start).floor(freq)
    end_round = pd.Timestamp(end).floor(freq)
    return start_round, end_round

async def _run_query(workflow, provider_handler, station_id, start_time, end_time, db = None):
    query = TimeseriesQuery(
        provider=provider_handler.provider_name,
        start_time=start_time,
        end_time=end_time,
        station_id=station_id,
    )
    response, pending = await workflow.run_timeseries_query(query)
    if db is not None and pending is not None and not pending.empty:
        if provider_handler.cache_data:
            await db.insert_data(pending, provider_handler)
    return response, pending

_PROVINCE_TEST_STATIONS = ["01110MS"] #, "23200MS", "89190MS"] #, "65350MS", "41000MS", "31410MS"]
_SBR_TEST_STATIONS = ["103"] #, "113", "96"] #, "137", "140", "12"]
_OPENMETEO_TEST_STATIONS = ['latsch', 'mals', 'bozen']

@pytest.fixture
def runtime(tmp_path):
    config = load_config_file("config/config.yaml")
    db_path = tmp_path / "test.db"
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

@pytest.fixture(
    scope="session",
    params=[
        *[("open-meteo", station_id) for station_id in _OPENMETEO_TEST_STATIONS],
    ],
)
def forecast_provider_station(request):
    return request.param


@pytest.fixture(params=["UTC", "Europe/Rome"])
def timezone_name(request):
    return request.param

@pytest.mark.asyncio
async def test_provider_fetching(provider_station, timezone_name, runtime, workflow):
    ## Query full timeseries and test result
    provider, station_id = provider_station
    tz = pytz.timezone(timezone_name)
    start_time = dt(2025, 6, 1, tzinfo = tz)
    end_time = dt(2025, 7, 1, tzinfo = tz)

    provider_handler = runtime.provider_manager.get_provider(provider)

    start_round, end_round = _round_start_end_to_freq(start_time, end_time, freq = provider_handler.freq)
    full_range = pd.date_range(start=start_round, end=end_round, freq=provider_handler.freq, inclusive="both")

    response, pending = await _run_query(workflow, provider_handler, station_id, start_time, end_time, db = runtime.db)

    response = _normalize_response(response)
    pending = pending if isinstance(pending, pd.DataFrame) else pd.DataFrame()

    assert len(response) == len(full_range)
    assert len(pending) == len(full_range)
    assert np.array_equal(response.datetime.values, full_range.values)
    assert str(response.datetime.dt.tz) == str(start_time.tzinfo)

    ## Test gap at start of existing data
    start_time2 = dt(2025, 5, 20, tzinfo = tz)
    end_time2 = dt(2025, 6, 10, tzinfo = tz)

    start2_round, end2_round = _round_start_end_to_freq(start_time2, end_time2, freq = provider_handler.freq)
    range2 = pd.date_range(start=start2_round, end=end2_round, freq=provider_handler.freq, inclusive="both")
    range2_before = pd.date_range(start=start2_round, end=start_round, freq=provider_handler.freq, inclusive="both")

    response, pending = await _run_query(workflow, provider_handler, station_id, start_time2, end_time2, db = runtime.db)

    response = _normalize_response(response)
    pending = pending if isinstance(pending, pd.DataFrame) else pd.DataFrame()

    assert len(response) == len(range2)
    assert len(pending) == len(range2_before)-1 #overlap gets removed
    assert np.array_equal(response.datetime.values, range2.values)
    assert str(response.datetime.dt.tz) == str(start_time2.tzinfo)

    ## Test gap at end of existing data
    start_time3 = dt(2025, 6, 20, tzinfo = tz)
    end_time3 = dt(2025, 7, 10, tzinfo = tz)

    start3_round, end3_round = _round_start_end_to_freq(start_time3, end_time3, freq = provider_handler.freq)
    range3 = pd.date_range(start=start3_round, end=end3_round, freq=provider_handler.freq, inclusive="both")
    range3_after = pd.date_range(start=end_round, end=end3_round, freq=provider_handler.freq, inclusive="both")
    
    response, pending = await _run_query(workflow, provider_handler, station_id, start_time3, end_time3, db = runtime.db)

    response = _normalize_response(response)
    pending = pending if isinstance(pending, pd.DataFrame) else pd.DataFrame()

    assert len(response) == len(range3)
    assert len(pending) == len(range3_after)-1
    assert np.array_equal(response.datetime.values, range3.values)
    assert str(response.datetime.dt.tz) == str(start_time3.tzinfo)

    ## Test with now gap, i.e. all data already cached

    start_time4 = dt(2025, 6, 1, tzinfo=tz)
    end_time4 = dt(2025, 6, 10, tzinfo=tz)

    start4_round, end4_round = _round_start_end_to_freq(start_time4, end_time4, freq = provider_handler.freq)
    range4 = pd.date_range(start=start4_round, end=end4_round, freq=provider_handler.freq, inclusive="both")

    response, pending = await _run_query(workflow, provider_handler, station_id, start_time4, end_time4, db = runtime.db)

    response = _normalize_response(response)
    pending = pending if isinstance(pending, pd.DataFrame) else pd.DataFrame()

    assert len(response) == len(range4)
    assert pending.empty
    assert np.array_equal(response.datetime.values, range4.values)
    assert str(response.datetime.dt.tz) == str(start_time4.tzinfo)

@pytest.mark.asyncio
async def test_province_dst_changes(runtime, workflow):
    
    provider, station_id = "province", "01110MS"
    tz = pytz.timezone("Europe/Rome")
    start_time = dt(2025, 1, 1, tzinfo = tz)
    end_time = dt(2025, 12, 31, tzinfo = tz)

    provider_handler = runtime.provider_manager.get_provider(provider)

    start_round, end_round = _round_start_end_to_freq(start_time, end_time, freq = provider_handler.freq)
    full_range = pd.date_range(start=start_round, end=end_round, freq=provider_handler.freq, inclusive="both")

    response, pending = await _run_query(workflow, provider_handler, station_id, start_time, end_time)

    response_raw = pd.DataFrame(response.data)
    dt_offsets = response_raw.datetime.str.split('+', expand = True)[1].unique()
    assert np.array_equal(dt_offsets, ['01:00', '02:00'])
    
    response = _normalize_response(response)
    pending = pending if isinstance(pending, pd.DataFrame) else pd.DataFrame()

    assert len(response) == len(full_range)
    assert len(response) == len(pending)
    assert np.array_equal(response.datetime.values, full_range.values)
    assert str(response.datetime.dt.tz) == str(start_time.tzinfo)

@pytest.mark.asyncio
async def test_latest_query(provider_station, timezone_name, workflow):
    provider, station_id = provider_station
    timezone = timezone_name

    latest_query = TimeseriesQuery(
        provider=provider,
        station_id=station_id,
        timezone=timezone,
    )

    latest_response, _ = await workflow.run_timeseries_query(latest_query, latest=True)
    data = pd.DataFrame(latest_response.data)
    
    assert len(data) == 1

    obs_cols = [c for c in data.columns if c not in {"station_id", "datetime", "model"}]
    if obs_cols:
        assert not data[obs_cols].isna().all(axis=None)

@pytest.mark.asyncio
async def test_invalid_station(provider_station, runtime, workflow):
    provider, _ = provider_station
    start_time = dt(2026,1,1)
    end_time = dt(2026,1,10)

    q = TimeseriesQuery(
        provider=provider,
        station_id="INVALID_STATION",
        start_time = start_time,
        end_time = end_time
    )
    
    response, pending = await workflow.run_timeseries_query(q)
    return_data = pd.DataFrame(response.data)

    assert return_data.empty
    assert pending.empty

## test invalid provider
@pytest.mark.asyncio
async def test_invalid_provider(workflow):
    start_time = dt(2026,1,1)
    end_time = dt(2026,1,10)

    q = TimeseriesQuery(
        provider="INVALID_PROVIDER",
        station_id="INVALID_STATION",
        start_time = start_time,
        end_time = end_time
    )
    
    with pytest.raises(Exception):
        response, pending = await workflow.run_timeseries_query(q)

@pytest.mark.asyncio
async def test_timezone_equivalence(provider_station, workflow, runtime):
    provider, station_id = provider_station
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

    assert not df_utc.empty
    assert not df_local.empty
    assert df_utc.shape == df_local.shape
    assert df_utc.equals(df_local)

@pytest.mark.asyncio
async def test_raise_future_start_time(provider_station, workflow, runtime):
    provider, station_id = provider_station
    start_time = dt(2050,1,1)

    q = TimeseriesQuery(
        provider=provider,
        station_id=station_id,
        start_time = start_time,
    )
    
    with pytest.raises(Exception, match = r"Start time must be in the past .*"):
        response, pending = await workflow.run_timeseries_query(q)

@pytest.mark.asyncio
async def test_forecast_timezone_equivalence(forecast_provider_station, workflow, runtime):
    provider, station_id = forecast_provider_station

    rome_tz = pytz.timezone("Europe/Rome")
    local_start = datetime.now().astimezone(rome_tz)
    local_end = datetime.now().astimezone(rome_tz) + timedelta(days = 10)

    utc_tz = pytz.timezone('UTC')
    utc_start = local_start.astimezone(utc_tz)
    utc_end = local_end.astimezone(utc_tz)

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

    assert not df_utc.empty
    assert not df_local.empty
    assert df_utc.shape == df_local.shape
    assert df_utc.equals(df_local)

@pytest.mark.asyncio
async def test_forecast_raise_multiple_models(forecast_provider_station, workflow):
    provider, station_id = forecast_provider_station
    start = datetime.now()
    end = datetime.now() + timedelta(days = 10)
    models = ['meteoswiss_icon_seamless', 'best_match']

    q = TimeseriesQuery(
        provider=provider,
        station_id=station_id,
        start_time = start,
        end_time = end,
        models = models
    )
    
    with pytest.raises(NotImplementedError, match = r'.* single model .*'):
        response, pending = await workflow.run_timeseries_query(q)

@pytest.mark.asyncio
async def test_forecast_fetching(forecast_provider_station, timezone_name, runtime, workflow):
    ## Query full timeseries and test result
    provider, station_id = forecast_provider_station
    tz = pytz.timezone(timezone_name)
    start_time = datetime.now(tz = tz)
    end_time = start_time + timedelta(days = 10)

    provider_handler = runtime.provider_manager.get_provider(provider)

    start_round, end_round = _round_start_end_to_freq(start_time, end_time, freq = provider_handler.freq)
    full_range = pd.date_range(start=start_round, end=end_round, freq=provider_handler.freq, inclusive="both")

    response, pending = await _run_query(workflow, provider_handler, station_id, start_time, end_time, db = runtime.db)

    response = _normalize_response(response)
    pending = pending if isinstance(pending, pd.DataFrame) else pd.DataFrame()

    assert len(response) == len(full_range)
    assert len(pending) == len(full_range)
    assert np.array_equal(response.datetime.values, full_range.values)
    assert str(response.datetime.dt.tz) == str(start_time.tzinfo)
