from abc import ABC, abstractmethod
import pandas as pd
import pandera.pandas as pa
from typing import Any, Dict, Optional


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
        #['Datum', 'create_time', 'T2m', 'TB -25cm', 'Nied.', 'Wind', 'Wg  max', 'RL', 'Ber.', 'Tt', 'Tf', 'mg22', 'mg23', 'station_id', 'Ausf.']
        return pa.DataFrameSchema(
            {
                "Datum": pa.Column(pd.Timestamp),
                "station_id": pa.Column(str),

                # SBR-specific columns based on sbr_colmap
                "T2m": pa.Column(float, nullable=True),          # Temperature 2m
                "TB -25cm": pa.Column(float, nullable=True, required=False),     # Soil temperature -25cm
                "Tt": pa.Column(float, nullable=True, required=False),           # Dry temperature 60cm
                "Tf": pa.Column(float, nullable=True, required=False),           # Wet temperature
                "RL": pa.Column(float, nullable=True, required=False),           # Relative humidity
                "Wind": pa.Column(float, nullable=True, required=False),         # Wind speed
                "Wg  max": pa.Column(float, nullable=True, required=False),      # Max wind gust
                "Nied.": pa.Column(float, nullable=True),        # Precipitation
                "Ber.": pa.Column(bool, nullable=True, required=False),         # Irrigation
                "Bn": pa.Column(float, nullable=True, required=False),           # Leaf wetness
                "Nied. sum": pa.Column(float, nullable=True, required=False),    # Precipitation sum
                "T2m avg": pa.Column(float, nullable=True, required=False),      # Average temperature 2m
                "BMp": pa.Column(float, nullable=True, required=False),          # Soil moisture
                "LMp": pa.Column(float, nullable=True, required=False),          # Air moisture
                "MMp": pa.Column(float, nullable=True, required=False),          # Medium moisture
                "SMp": pa.Column(float, nullable=True, required=False),          # Surface moisture
                
                # Additional columns that might be present
                "create_time": pa.Column(pd.Timestamp, nullable=True, required=False),
                "rainStart": pa.Column(pd.Timestamp, nullable=True, required=False),
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

    def save(self, validated_data: pd.DataFrame, output_path: Optional[str] = None) -> None:
        """
        Save the validated data to a database
                
        Args:
            validated_data (pd.DataFrame): Validated data to save
            output_path (Optional[str]): Path where to save the data
        """
        pass

    def run(self, **kwargs) -> pd.DataFrame:
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
        
        # Save if output_path is provided
        if 'output_path' in kwargs:
            self.save(validated_data, kwargs['output_path'])
            
        return validated_data