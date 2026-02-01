"""
A small harness that executes multiple QueryManager calls covering:
    * overlapping date ranges
    * repeated queries on the same range (idempotency)
    * multiple stations
    * short ranges to keep API usage low

Use this script to quickly sanity check that inserts remain unique and that
gaps/placeholder rows behave as expected before deploying to production.
"""

import logging
import logging.config
from datetime import datetime
import asyncio

import pytz

from src.runtime import RuntimeContext
from src.workflow import QueryWorkflow
from src.validation import TimeseriesQuery

tz = pytz.timezone("utc")
logger = logging.getLogger(__name__)

def dt(year: int, month: int, day: int, hour: int = 0, minute: int = 0, second: int = 0):
    """Shortcut for building UTC-aware datetimes."""
    return datetime(year, month, day, hour, minute, second, tzinfo=tz)

_PROVINCE_TEST_STATIONS = ["01110MS", "23200MS", "89190MS", "65350MS", "41000MS", "31410MS"]
_SBR_TEST_STATIONS = [103, 113, 96, 137, 140, 12]
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

    runtime = RuntimeContext.from_config_file('config/config.yaml')
    workflow = QueryWorkflow(runtime)

    for provider_name, query_dict in _QUERY_COMBINATIONS.items():
        for query_id, params_l1 in query_dict.items():
            test_ids = params_l1['station_ids']
            if isinstance(test_ids, str):
                test_ids = [test_ids]
            for station_id in test_ids:
                query = TimeseriesQuery(
                    provider = provider_name,
                    start_time = params_l1['start_time'],
                    end_time = params_l1['end_time'],
                    station_id = station_id,
                )

                try:
                    await workflow.run_timeseries_query(query)
                except Exception as e:
                    logger.exception(f"Failed to test query {query_id} for station {station_id}: {e}")

if __name__ == "__main__":
    asyncio.run(main())
