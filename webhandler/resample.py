import numpy as np
import pandas as pd
from scipy.stats import mode

import logging
from typing import str, Callable

logger = logging.getLogger(__name__)

def get_mode(column):
    return mode(column, nan_policy='omit').mode[0]

class ColumnResampler:

    def __init__(self, freq: str):
        
        self.freq = freq

        self.resample_colmap = {
            "tair_2m": "mean",                           # Temperature 2m
            "tsoil_25cm": "mean",     # Soil temperature -25cm
            "tdry_60cm": "mean",           # Dry temperature 60cm
            "twet_60cm": "mean",           # Wet temperature
            "relative_humidity": "mean",           # Relative humidity
            "wind_speed": "mean",         # Wind speed
            "wind_gust": "max",      # Max wind gust
            "wind_direction": get_mode,
            "precipitation": "sum",                        # Precipitation
            "irrigation": "max",         # Irrigation
            "leaf_wetness": "mean",           # Leaf wetness
            "air_pressure": "mean",
            "sun_duration": "mean",
            "solar_radiation": "sum",
            "snow_height": "mean",
            "water_level": "mean",
            "discharge": "mean"
        }

    def update_aggfunc(self, column: str, aggfunc: str | Callable):
        if column in self._resample_colmap:
            logger.info(f"Column '{column}' found in resample_colmap. Updating aggregation function.")
        else:
            logger.info(f"Column '{column}' not found in resample_colmap. Add new entry.")
        self.resample_colmap[column] = aggfunc

    def apply_resampling(self, data: pd.DataFrame, default_aggfunc: str | Callable | None = None):

        data_copy = data.copy()
        resample_colmap = self.resample_colmap.copy()

        missing_columns = [col for col in data.columns if col not in self._resample_colmap]
        if default_aggfunc is None:
            logger.info(f"The following columns are missing from the resample_colmap: {missing_columns}. They are ignored from resampling")
            data_copy.drop(missing_columns, axis = 1, inplace = True)
        else:
            for i in missing_column:
                resample_colmap[i] = default_aggfunc
                
        return data.resample(self.freq).agg({i:j for i,j in resample_colmap.items() if i in data.columns})
