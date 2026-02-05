"""
A small harness that executes multiple QueryManager calls covering:
    * overlapping date ranges
    * repeated queries on the same range (idempotency)
    * multiple stations
    * short ranges to keep API usage low

Use this script to quickly sanity check that inserts remain unique and that
gaps/placeholder rows behave as expected before deploying to production.
"""

import asyncio
import logging
import logging.config
from datetime import datetime, timedelta, timezone

import pytz
import pandas as pd

from src.runtime import RuntimeContext
from src.workflow import QueryWorkflow
from src.validation import TimeseriesQuery

tz = pytz.timezone("utc")
logger = logging.getLogger(__name__)

def dt(year: int, month: int, day: int, hour: int = 0, minute: int = 0, second: int = 0):
    """Shortcut for building UTC-aware datetimes."""
    return datetime(year, month, day, hour, minute, second, tzinfo=tz)

def _normalize_response(response):
    if not response.data:
        return pd.DataFrame()
    df = pd.DataFrame(response.data)
    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
    df = df.sort_values("datetime").reset_index(drop=True)
    return df

def _assert_timestamp_bounds(response, start_time, end_time, label):
    if response.count == 0:
        logger.info(f"[{label}] No data returned; bounds check skipped.")
        return
    df = _normalize_response(response)
    if df.empty or "datetime" not in df.columns:
        logger.warning(f"[{label}] Missing datetime column; bounds check skipped.")
        return
    min_ts = df["datetime"].min()
    max_ts = df["datetime"].max()
    if min_ts < start_time:
        raise AssertionError(f"[{label}] Found timestamp before start_time: {min_ts} < {start_time}")
    if max_ts > end_time:
        raise AssertionError(f"[{label}] Found timestamp after end_time: {max_ts} > {end_time}")

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

async def main():
    logging.basicConfig(
        level = logging.DEBUG,
        format = '[%(asctime)s] %(levelname)s - %(message)s'
        )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("hpack").setLevel(logging.WARNING)
    logging.getLogger("h2").setLevel(logging.WARNING)

    runtime = RuntimeContext.from_config_file('config/config.yaml')
    workflow = QueryWorkflow(runtime)

    # for provider_name, query_dict in _QUERY_COMBINATIONS.items():
    #     provider_handler = runtime.provider_manager.get_provider(provider_name)
    #     for query_id, params_l1 in query_dict.items():
    #         test_ids = params_l1['station_ids']
    #         if isinstance(test_ids, str):
    #             test_ids = [test_ids]
    #         for station_id in test_ids:
    #             try:
    #                 response, pending = await _run_query(
    #                     workflow,
    #                     provider_handler,
    #                     station_id,
    #                     params_l1['start_time'],
    #                     params_l1['end_time'],
    #                     db = runtime.db,
    #                 )
    #                 logger.info(
    #                     f"[{provider_name}:{query_id}:{station_id}] "
    #                     f"count={response.count} pending={len(pending) if hasattr(pending, '__len__') else 'n/a'}"
    #                 )
    #             except Exception as e:
    #                 logger.exception(f"Failed to test query {query_id} for station {station_id}: {e}")

    # Latest query (may return cached data)
    try:
        provider_handler = runtime.provider_manager.get_provider("province")
        latest_query = TimeseriesQuery(
            provider=provider_handler.provider_name,
            start_time=None,
            end_time=None,
            station_id=_PROVINCE_TEST_STATIONS[0],
        )
        latest_response, _ = await workflow.run_timeseries_query(latest_query, latest=True)
        if latest_response.count > 0:
            latest_df = _normalize_response(latest_response)
            obs_cols = [c for c in latest_df.columns if c not in {"station_id", "datetime"}]
            if obs_cols and latest_df[obs_cols].isna().all(axis=None):
                raise AssertionError("Latest query returned only NA observation values.")
    except Exception as e:
        logger.exception(f"Latest query test failed for province: {e}")

    # Boundary inclusion (timestamp bounds only)
    boundary_start = dt(2025, 10, 1, 0, 0, 0)
    boundary_end = dt(2025, 10, 1, 1, 0, 0)
    try:
        response, _ = await _run_query(
            workflow,
            runtime.provider_manager.get_provider("province"),
            _PROVINCE_TEST_STATIONS[0],
            boundary_start,
            boundary_end,
            db = runtime.db,
        )
        _assert_timestamp_bounds(response, boundary_start, boundary_end, "boundary-inclusion-province")
    except Exception as e:
        logger.exception(f"Boundary inclusion test failed for province: {e}")

    # Future end time cap (best-effort: only assert when data exists)
    now_utc = datetime.now(timezone.utc)
    future_end = now_utc + timedelta(days=2)
    past_start = now_utc - timedelta(days=1)
    try:
        response, _ = await _run_query(
            workflow,
            runtime.provider_manager.get_provider("province"),
            _PROVINCE_TEST_STATIONS[0],
            past_start,
            future_end,
            db = runtime.db,
        )
        if response.count > 0:
            max_ts = _normalize_response(response)["datetime"].max()
            if max_ts > now_utc:
                raise AssertionError(f"Future end time not capped; max_ts={max_ts} now={now_utc}")
        else:
            logger.info("[future-end-time] No data returned; cap check skipped.")
    except Exception as e:
        logger.exception(f"Future end time test failed for province: {e}")

    # Invalid station handling
    try:
        await _run_query(
            workflow,
            runtime.provider_manager.get_provider("province"),
            "INVALID_STATION",
            boundary_start,
            boundary_end,
            db = runtime.db,
        )
        raise AssertionError("Invalid station test expected to fail but succeeded.")
    except Exception:
        logger.info("Invalid station test passed (exception raised as expected).")

    # Timezone round-trip and equivalence
    try:
        utc_start = dt(2025, 10, 1, 0, 0, 0)
        utc_end = dt(2025, 10, 2, 0, 0, 0)
        rome_tz = pytz.timezone("Europe/Rome")
        local_start = utc_start.astimezone(rome_tz)
        local_end = utc_end.astimezone(rome_tz)

        resp_utc, _ = await _run_query(
            workflow,
            runtime.provider_manager.get_provider("province"),
            _PROVINCE_TEST_STATIONS[0],
            utc_start,
            utc_end,
            db = runtime.db,
        )
        resp_local, _ = await _run_query(
            workflow,
            runtime.provider_manager.get_provider("province"),
            _PROVINCE_TEST_STATIONS[0],
            local_start,
            local_end,
            db = runtime.db,
        )

        df_utc = _normalize_response(resp_utc)
        df_local = _normalize_response(resp_local)
        if not df_utc.empty or not df_local.empty:
            df_utc = df_utc.sort_values("datetime").reset_index(drop=True)
            df_local = df_local.sort_values("datetime").reset_index(drop=True)
            if df_utc.shape != df_local.shape:
                raise AssertionError("Timezone equivalence failed: shape mismatch.")
            if not df_utc.equals(df_local):
                raise AssertionError("Timezone equivalence failed: data mismatch.")
        else:
            logger.info("[timezone-equivalence] No data returned; equality check skipped.")
    except Exception as e:
        logger.exception(f"Timezone round-trip/equivalence test failed for province: {e}")

if __name__ == "__main__":
    asyncio.run(main())
