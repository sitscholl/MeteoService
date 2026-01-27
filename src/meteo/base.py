import asyncio
import httpx

from abc import ABC, abstractmethod
import pandas as pd
import pandera.pandas as pa
from typing import Any, Dict, Tuple
import logging

logger = logging.getLogger(__name__)

class BaseMeteoHandler(ABC):
    """
    Abstract base class for meteorological data handlers.
    
    This class defines the interface for retrieving, processing, and validating
    meteorological data from various sources.
    """

    def __init__(
        self, 
        timezone, 
        chunk_size_days: int = 365, 
        timeout: int = 20, 
        max_concurrent_requests: int = 5,
        sleep_time: int = 1,
        **kwargs
        ):
        
        self.timezone = timezone
        self.chunk_size_days = chunk_size_days
        self.timeout = timeout
        self.max_concurrent_requests = max_concurrent_requests

        if max_concurrent_requests < 1:
            raise ValueError(f"max concurrent requests should be greater than 0. Got {max_concurrent_requests}")

        self.sleep_time = sleep_time
        
        self._semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        self.station_info = None
        self._station_info_lock = asyncio.Lock()
        self.station_sensors = {}
        self._station_sensors_locks: dict[str, asyncio.Lock] = {}
        self._client = None

    @property
    @abstractmethod
    def freq(self):
        """Return frequency string for datetime frequency of provider measurements"""
        pass

    @property
    @abstractmethod
    def inclusive(self):
        """Return string indicating if query calls to the provider are left or right inclusive or both. Must be one of 'left', 'right' or 'both'."""
        pass

    def __enter__(self):
        """Enter context management."""
        raise ValueError("MeteoHandlers have to be used in an asyncronous context. Use async with...")

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context management."""
        raise ValueError("MeteoHandlers have to be used in an asyncronous context. Use async with...")

    async def __aenter__(self):
        """Start httpx client that is reused across requests"""
        logger.info("Opening API session...")
        self._client = httpx.AsyncClient(timeout = self.timeout)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Start httpx client that is reused across requests"""
        logger.info("Closing API session...")
        if self._client is not None:
            await self._client.aclose()

    @abstractmethod
    async def get_sensors(self, station_id: str) -> list[str]:
        """
        Get a list of available sensors for a station
        """
        pass

    @abstractmethod
    async def get_stations(self) -> list[str]:
        """
        Get a list of available station ids
        """
        pass

    @abstractmethod
    async def get_station_info(self, station_id: str | None) -> Dict[str, Any]:
        """
        Query information for a given station from the source, 
        such as elevation, latitude or longitude. Query for all stations if no station is given.

        Args:
            station_id (str): The unique identifier for the station.

        Returns:
            dict: A dictionary containing station information such as elevation, latitude, and longitude.
        """
        pass

    @abstractmethod
    async def get_raw_data(self, **kwargs) -> Tuple[pd.DataFrame | None, Dict]:
        """
        Query the raw data from the source.
        
        Args:
            **kwargs: Parameters for data retrieval
            
        """
        pass

    @abstractmethod
    def transform(self, raw_data: pd.DataFrame | None) -> pd.DataFrame | None:
        """
        Transform the raw data into a standardized format.
        
        Args:
            raw_data: Raw data to be transformed
            
        Returns:
            pd.DataFrame: Transformed data in standardized format
        """
        pass

    @property
    def output_schema(self) -> pa.DataFrameSchema:
        """
        Define the expected schema for SBR meteorological data output.
        
        Returns:
            pa.DataFrameSchema: Schema for validating SBR output data
        """
        return pa.DataFrameSchema(
            {
                "datetime": pa.Column(pd.DatetimeTZDtype(tz="UTC")),
                "station_id": pa.Column(str),

                "tair_2m": pa.Column(float, nullable=True, required = False),                           # Temperature 2m
                "tsoil_25cm": pa.Column(float, nullable=True, required=False),     # Soil temperature -25cm
                "tdry_60cm": pa.Column(float, nullable=True, required=False),           # Dry temperature 60cm
                "twet_60cm": pa.Column(float, nullable=True, required=False),           # Wet temperature
                "relative_humidity": pa.Column(float, nullable=True, required=False),           # Relative humidity
                "wind_speed": pa.Column(float, nullable=True, required=False),         # Wind speed
                "wind_gust": pa.Column(float, nullable=True, required=False),      # Max wind gust
                "wind_direction": pa.Column(float, nullable = True, required = False),
                "precipitation": pa.Column(float, nullable=True, required = False),                        # Precipitation
                "irrigation": pa.Column(int, nullable=True, required=False),         # Irrigation
                "leaf_wetness": pa.Column(float, nullable=True, required=False),           # Leaf wetness
                "millsperiode_start": pa.Column(float, nullable=True, required=False),          # Beginn Millsperiode
                "rain_start": pa.Column(pd.Timestamp, nullable=True, required=False),
                "air_pressure": pa.Column(float, nullable = True, required = False),
                "sun_duration": pa.Column(float, nullable = True, required = False),
                "solar_radiation": pa.Column(float, nullable = True, required = False),
                "snow_height": pa.Column(float, nullable = True, required = False),
                "water_level": pa.Column(float, nullable = True, required = False),
                "discharge": pa.Column(float, nullable = True, required = False)

            },
            index=pa.Index(int),
            strict=False  # Allow additional columns that might be added
        )

    def validate(self, transformed_data: pd.DataFrame) -> pd.DataFrame:
        """
        Validate the transformed data against the output schema.
        
        Args:
            transformed_data (pd.DataFrame): Data to validate
            
        Returns:
            pd.DataFrame: Validated data
            
        Raises:
            pa.errors.SchemaError: If validation fails
        """
        return self.output_schema.validate(transformed_data)

    async def run(self, drop_columns = False, **kwargs) -> pd.DataFrame | None:
        """
        Run the complete data processing pipeline.
        
        This method orchestrates the entire process:
        1. Get raw data
        2. Transform it
        3. Validate it
        4. Save it (optional)
        
        Args:
            **kwargs: Parameters for the pipeline
            
        Returns:
            pd.DataFrame: Processed and validated data
        """
        raw_data, _ = await self.get_raw_data(**kwargs)

        if raw_data is None:
            return None

        transformed_data = self.transform(raw_data)
        validated_data = self.validate(transformed_data)

        if drop_columns:
            validated_data = validated_data[[i for i in validated_data.columns if i in self.output_schema.columns]]
                    
        return validated_data