import pandas as pd
from scipy.stats import mode

import logging
from typing import Callable, Any

logger = logging.getLogger(__name__)

def get_mode(column: pd.Series):
    values = column.dropna()
    if values.empty:
        return pd.NA
    return mode(values, nan_policy="omit").mode[0]

DEFAULT_RESAMPLE_COLMAP: dict[str, str] = {
    "tair_2m": "mean",
    "tsoil_25cm": "mean",
    "tdry_60cm": "mean",
    "twet_60cm": "mean",
    "relative_humidity": "mean",
    "wind_speed": "mean",
    "wind_gust": "max",
    "wind_direction": "mode",
    "precipitation": "sum",
    "irrigation": "max",
    "leaf_wetness": "mean",
    "air_pressure": "mean",
    "sun_duration": "mean",
    "solar_radiation": "sum",
    "snow_height": "mean",
    "water_level": "mean",
    "discharge": "mean",
}

class ColumnResampler:

    _AGG_STR_TO_FUNC: dict[str, str | Callable[[pd.Series], Any]] = {
        "mean": "mean",
        "sum": "sum",
        "max": "max",
        "min": "min",
        "median": "median",
        "first": "first",
        "last": "last",
        "mode": get_mode,
    }

    def __init__(self, freq: str, resample_colmap: dict[str, str | Callable] | None = None):
        
        self.freq = freq
        self.resample_colmap = (
            resample_colmap.copy() if resample_colmap is not None else DEFAULT_RESAMPLE_COLMAP.copy()
        )

    def update_aggfunc(self, column: str, aggfunc: str | Callable):
        if column in self.resample_colmap:
            logger.info(f"Column '{column}' found in resample_colmap. Updating aggregation function.")
        else:
            logger.info(f"Column '{column}' not found in resample_colmap. Add new entry.")
        self.resample_colmap[column] = aggfunc

    def _resolve_aggfunc(self, aggfunc: str | Callable):
        if callable(aggfunc):
            return aggfunc
        aggfunc_norm = aggfunc.strip().lower()
        if aggfunc_norm not in self._AGG_STR_TO_FUNC:
            raise ValueError(
                f"Invalid aggregation function '{aggfunc}'. "
                f"Choose one of {sorted(self._AGG_STR_TO_FUNC.keys())} or pass a callable."
            )
        return self._AGG_STR_TO_FUNC[aggfunc_norm]

    def apply_resampling(
        self,
        data: pd.DataFrame,
        default_aggfunc: str | Callable | None = None,
        datetime_col: str = "datetime",
        groupby_cols: list[str] | None = None,
    ) -> pd.DataFrame:

        data_copy = data.copy()
        resample_colmap = self.resample_colmap.copy()
        groupby_cols = groupby_cols or ["station_id", "model"]

        if data_copy.empty:
            return data_copy

        required_cols = [datetime_col] + groupby_cols
        missing_required = [c for c in required_cols if c not in data_copy.columns]
        if missing_required:
            raise ValueError(f"Cannot resample without required columns: {missing_required}")

        if not pd.api.types.is_datetime64_any_dtype(data_copy[datetime_col]):
            data_copy[datetime_col] = pd.to_datetime(data_copy[datetime_col], errors="coerce")
        data_copy.dropna(subset=[datetime_col], inplace=True)

        value_cols = [c for c in data_copy.columns if c not in required_cols]
        if not value_cols:
            logger.info("No value columns available for resampling.")
            return data_copy[required_cols].drop_duplicates().sort_values(by=[datetime_col] + groupby_cols)

        missing_columns = [col for col in value_cols if col not in resample_colmap]
        if default_aggfunc is None:
            if missing_columns:
                logger.info(
                    f"Columns missing from resample_colmap and ignored in resampling: {missing_columns}"
                )
            agg_columns = [c for c in value_cols if c in resample_colmap]
        else:
            for col in missing_columns:
                resample_colmap[col] = default_aggfunc
            agg_columns = value_cols

        if not agg_columns:
            logger.info("No value columns selected for resampling after aggregation mapping.")
            return data_copy[required_cols].drop_duplicates().sort_values(by=[datetime_col] + groupby_cols)

        agg_map = {col: self._resolve_aggfunc(resample_colmap[col]) for col in agg_columns}

        results = []
        for group_vals, group_df in data_copy.groupby(groupby_cols, sort=False, dropna=False):
            group_df = group_df.sort_values(datetime_col).set_index(datetime_col)
            group_resampled = group_df[agg_columns].resample(self.freq).agg(agg_map)
            group_resampled = group_resampled.reset_index()

            if not isinstance(group_vals, tuple):
                group_vals = (group_vals,)
            for col_name, col_val in zip(groupby_cols, group_vals):
                group_resampled[col_name] = col_val

            results.append(group_resampled)

        if not results:
            return pd.DataFrame(columns=required_cols + agg_columns)

        out = pd.concat(results, ignore_index=True)
        out = out[groupby_cols + [datetime_col] + agg_columns]
        out.sort_values(by=[datetime_col] + groupby_cols, inplace=True)
        return out
