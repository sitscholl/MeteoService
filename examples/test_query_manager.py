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
from typing import Iterable, Tuple

import pytz

from webhandler.query_manager import QueryManager
from webhandler.config import load_config
from webhandler.database.db import MeteoDB
from webhandler.provider_manager import ProviderManager


tz = pytz.timezone("utc")
logger = logging.getLogger(__name__)


def dt(year: int, month: int, day: int, hour: int = 0, minute: int = 0, second: int = 0):
    """Shortcut for building UTC-aware datetimes."""
    return datetime(year, month, day, hour, minute, second, tzinfo=tz)


def summarize_result(df, start, end, station, scenario_name):
    """Log useful diagnostics for each query result."""
    if df.empty:
        logger.warning(f"[{scenario_name}] Station {station}: no rows returned for {start}–{end}")
        return

    duplicates = int(df.index.duplicated().sum())
    value_df = df.drop(columns=['station_id']) if 'station_id' in df.columns else df
    null_rows = int(value_df.isna().all(axis=1).sum())
    coverage = (df.index.max() - df.index.min()) if len(df.index) > 1 else 0
    logger.info(
        "[%s] Station %s: %s rows, %s unique timestamps, duplicates=%s, placeholder_rows=%s, coverage=%s",
        scenario_name,
        station,
        len(df),
        df.index.nunique(),
        duplicates,
        null_rows,
        coverage,
    )

    if duplicates:
        logger.error("Duplicated timestamps detected for station %s; inspect DB for uniqueness issues.", station)


def run_scenario(
    scenario_name: str,
    stations: Iterable[str],
    ranges: Iterable[Tuple[datetime, datetime]],
    repetitions: int,
    manager: QueryManager,
    db: MeteoDB,
    provider: str,
    variables=None,
):
    logger.info("=== Scenario: %s ===", scenario_name)
    for repetition in range(repetitions):
        logger.info("Repetition %s/%s", repetition + 1, repetitions)
        for station in stations:
            for start_time, end_time in ranges:
                logger.debug(
                    "[%s] Querying station=%s range=%s – %s", scenario_name, station, start_time, end_time
                )
                data = manager.get_data(
                    db=db,
                    provider=provider,
                    station_id=station,
                    start_time=start_time,
                    end_time=end_time,
                    variables=variables,
                )
                summarize_result(data, start_time, end_time, station, scenario_name)


def main():
    config = load_config("config/config.yaml")
    logging.config.dictConfig(config["logging"])
    global logger
    logger = logging.getLogger(__name__)

    provider_stations_dict = {
        'province': ["09700MS"],
        #'SBR': [103, 113]
    }
    provider_query = "SBR"
    stations_to_test = config.get("testing", {}).get("stations", [113])
    stations_to_test = [str(st) for st in stations_to_test]

    overlapping_ranges = [
        (dt(2025, 8, 25, 0, 0), dt(2025, 8, 25, 6, 0)),
        (dt(2025, 8, 25, 4, 0), dt(2025, 8, 25, 9, 0)),
        (dt(2025, 8, 25, 8, 30), dt(2025, 8, 25, 10, 0)),
    ]

    short_handoff_ranges = [
        (dt(2025, 8, 26, 22, 0), dt(2025, 8, 26, 23, 0)),
        (dt(2025, 8, 26, 23, 0), dt(2025, 8, 27, 0, 30)),
        (dt(2025, 8, 27, 0, 30), dt(2025, 8, 27, 2, 0)),
    ]

    mixed_day_ranges = [
        (dt(2025, 8, 28, 6, 0), dt(2025, 8, 28, 7, 0)),
        (dt(2025, 8, 28, 12, 0), dt(2025, 8, 28, 13, 0)),
        (dt(2025, 8, 28, 18, 0), dt(2025, 8, 28, 19, 0)),
    ]

    provider_manager = ProviderManager(provider_config=config["providers"])
    meteo_db = MeteoDB(provider_manager=provider_manager)
    query_manager = QueryManager(config, provider_manager=provider_manager)

    for provider_query, stations_to_test in provider_stations_dict.items():
        try:
            run_scenario(
                scenario_name="overlapping_windows_single_station",
                stations=stations_to_test[:1],
                ranges=overlapping_ranges,
                repetitions=2,
                manager=query_manager,
                db=meteo_db,
                provider=provider_query,
            )

            run_scenario(
                scenario_name="idempotent_re_query",
                stations=stations_to_test[:1],
                ranges=short_handoff_ranges[:1],
                repetitions=2,
                manager=query_manager,
                db=meteo_db,
                provider=provider_query,
            )

            run_scenario(
                scenario_name="adjacent_windows_multiple_stations",
                stations=stations_to_test,
                ranges=short_handoff_ranges,
                repetitions=1,
                manager=query_manager,
                db=meteo_db,
                provider=provider_query,
            )

            run_scenario(
                scenario_name="scattered_daytime_checks",
                stations=stations_to_test[:2],
                ranges=mixed_day_ranges,
                repetitions=1,
                manager=query_manager,
                db=meteo_db,
                provider=provider_query,
            )

        finally:
            meteo_db.close()


if __name__ == "__main__":
    main()
