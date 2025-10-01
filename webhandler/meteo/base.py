from abc import ABC, abstractmethod
import pandas as pd
import pandera.pandas as pa
from typing import Any, Dict, Optional, List

class BaseMeteoHandler(ABC):
    """
    Abstract base class for meteorological data handlers.
    
    This class defines the interface for retrieving, processing, and validating
    meteorological data from various sources.
    """

    @abstractmethod
    def __enter__(self):
        """Enter context management."""
        return self

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context management."""
        pass

    @abstractmethod
    def get_data(self, **kwargs) -> pd.DataFrame:
        """
        Query the raw data from the source.
        
        Args:
            **kwargs: Parameters for data retrieval
            
        Returns:
            Any: Raw data from the source
        """
        pass

    @abstractmethod
    def transform(self, raw_data: Any) -> pd.DataFrame:
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

    def run(self, drop_columns = False, **kwargs) -> pd.DataFrame:
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
        raw_data = self.get_data(**kwargs)
        transformed_data = self.transform(raw_data)
        validated_data = self.validate(transformed_data)

        if drop_columns:
            validated_data = validated_data[[i for i in validated_data.columns if i in self.output_schema.columns]]
                    
        return validated_data