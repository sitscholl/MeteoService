from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd

from datetime import datetime, timezone
from numbers import Number
import logging
import pytz
from typing import Optional, Dict, List, Any

from . import models

logger = logging.getLogger(__name__)

class MeteoDB:

    def __init__(self, engine: str = 'sqlite:///database.db'):
        self.engine = create_engine(engine)
        models.Base.metadata.create_all(self.engine)
        self.session = sessionmaker(bind=self.engine, autocommit = False, autoflush = False)()

    def query_station(self, provider: str | None = None):
        query = self.session.query(models.Station)
        
        if provider is not None:
            query = query.filter(models.Station.provider == provider)

        return query.all()

    def query_data(self):
        query = self.session.query(models.Measurement).all()

        return query

    def insert_station(self, **kwargs):
        station_model = models.Station(**kwargs)
        self.session.add(station_model)
        self.session.commit()

    def get_or_create_station(self, provider: str, external_id: str, **kwargs) -> int:
        """
        Get existing station by provider and external_id, or create a new one.
        Returns the internal station ID.
        """
        # Try to find existing station
        station = self.session.query(models.Station).filter(
            models.Station.provider == provider,
            models.Station.external_id == external_id
        ).first()

        if station:
            return station.id

        # Create new station if not found
        station_data = {
            'provider': provider,
            'external_id': external_id,
            **kwargs
        }
        station = models.Station(**station_data)
        self.session.add(station)
        self.session.flush()  # Get the ID without committing
        return station.id

    def get_or_create_variable(self, name: str, unit: str = None, description: str = None) -> int:
        """
        Get existing variable by name, or create a new one.
        Returns the internal variable ID.
        """
        # Try to find existing variable
        variable = self.session.query(models.Variable).filter(
            models.Variable.name == name
        ).first()

        if variable:
            return variable.id

        # Create new variable if not found
        variable = models.Variable(name=name, unit=unit, description=description)
        self.session.add(variable)
        self.session.flush()  # Get the ID without committing
        return variable.id

    def insert_data(self, data: pd.DataFrame, provider: str = None,
                   station_metadata: Dict = None, variable_metadata: Dict = None,
                   index=False, index_label=None, if_exists='append'):
        """
        Insert measurement data into the database.

        Expected DataFrame columns:
        - 'external_station_id': External station identifier
        - 'variable_name': Name of the measured variable
        - 'timestamp': Measurement timestamp
        - 'value': Measurement value
        - 'provider': Provider name (optional if provided as parameter)

        Optional columns for station creation:
        - 'station_name', 'latitude', 'longitude', 'elevation'

        Optional columns for variable creation:
        - 'variable_unit', 'variable_description'
        """
        if data.empty:
            logger.warning("Empty DataFrame provided to insert_data")
            return

        # Validate required columns
        required_cols = ['external_station_id', 'variable_name', 'timestamp', 'value']
        missing_cols = [col for col in required_cols if col not in data.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        # Make a copy to avoid modifying the original DataFrame
        df = data.copy()

        # Use provider from parameter if not in DataFrame
        if 'provider' not in df.columns:
            if provider is None:
                raise ValueError("Provider must be specified either as parameter or in DataFrame column")
            df['provider'] = provider

        # Process each unique station
        station_mapping = {}
        for _, row in df[['provider', 'external_station_id']].drop_duplicates().iterrows():
            station_kwargs = {}

            # Add optional station metadata from DataFrame
            for col in ['station_name', 'latitude', 'longitude', 'elevation']:
                if col in df.columns:
                    # Get the first non-null value for this station
                    station_data = df[df['external_station_id'] == row['external_station_id']]
                    value = station_data[col].dropna().iloc[0] if not station_data[col].dropna().empty else None
                    if value is not None:
                        # Map column names to model field names
                        field_name = 'name' if col == 'station_name' else col
                        station_kwargs[field_name] = value

            # Add station metadata if provided
            if station_metadata:
                station_kwargs['station_metadata'] = station_metadata

            station_id = self.get_or_create_station(
                provider=row['provider'],
                external_id=row['external_station_id'],
                **station_kwargs
            )
            station_mapping[(row['provider'], row['external_station_id'])] = station_id

        # Process each unique variable
        variable_mapping = {}
        for variable_name in df['variable_name'].unique():
            variable_kwargs = {}

            # Add optional variable metadata from DataFrame
            variable_data = df[df['variable_name'] == variable_name]
            if 'variable_unit' in df.columns:
                unit = variable_data['variable_unit'].dropna().iloc[0] if not variable_data['variable_unit'].dropna().empty else None
                if unit is not None:
                    variable_kwargs['unit'] = unit

            if 'variable_description' in df.columns:
                desc = variable_data['variable_description'].dropna().iloc[0] if not variable_data['variable_description'].dropna().empty else None
                if desc is not None:
                    variable_kwargs['description'] = desc

            # Add variable metadata if provided
            if variable_metadata and variable_name in variable_metadata:
                variable_kwargs.update(variable_metadata[variable_name])

            variable_id = self.get_or_create_variable(
                name=variable_name,
                **variable_kwargs
            )
            variable_mapping[variable_name] = variable_id

        # Map external IDs to internal IDs
        df['station_id'] = df.apply(
            lambda row: station_mapping[(row['provider'], row['external_station_id'])],
            axis=1
        )
        df['variable_id'] = df['variable_name'].map(variable_mapping)

        # Insert the measurements
        # Validate and clean measurements before inserting: ensure required IDs, timestamps and values exist
        measurements_df['timestamp'] = pd.to_datetime(measurements_df['timestamp'], errors='coerce')
        measurements_df['value'] = pd.to_numeric(measurements_df['value'], errors='coerce')

        required_cols = ['station_id', 'variable_id', 'timestamp', 'value']
        # Insert the measurements
        # Validate and clean measurements before inserting: ensure required IDs, timestamps and values exist
        measurements_df['timestamp'] = pd.to_datetime(measurements_df['timestamp'], errors='coerce')
        measurements_df['value'] = pd.to_numeric(measurements_df['value'], errors='coerce')

        required_cols = ['station_id', 'variable_id', 'timestamp', 'value']
        before_count = len(measurements_df)
        measurements_clean = measurements_df.dropna(subset=required_cols).copy()
        dropped = before_count - len(measurements_clean)

        if measurements_clean.empty:
            logger.warning("No valid measurements to insert after cleaning. Nothing to do.")
            return

        try:
            measurements_clean.to_sql(
                name='measurements',
                con=self.engine,
                index=index,
                index_label=index_label,
                if_exists=if_exists
            )
            self.session.commit()
            logger.info(f"Successfully inserted {len(measurements_clean)} measurements (dropped {dropped} invalid rows)")
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error inserting data: {e}")
            raise


if __name__ == "__main__":
    from uuid import uuid4
    import numpy as np
    from datetime import datetime

    # Create database instance
    meteo_db = MeteoDB()

    # Example 1: Insert station manually (old way still works)
    meteo_db.insert_station(
        provider="test",
        external_id=str(uuid4()),
        latitude=3,
        longitude=4,
        elevation=5,
    )

    # Example 2: Insert data with automatic station and variable creation
    # Create sample data with external station IDs and variable names
    df = pd.DataFrame({
        'external_station_id': ['STATION_001', 'STATION_001', 'STATION_002', 'STATION_002'],
        'variable_name': ['temperature_2m', 'humidity', 'temperature_2m', 'pressure'],
        'timestamp': [datetime.now(), datetime.now(), datetime.now(), datetime.now()],
        'value': [25.5, 60.0, 23.2, 1013.25],
        'provider': ['weather_service', 'weather_service', 'weather_service', 'weather_service'],
        'station_name': ['Weather Station 1', 'Weather Station 1', 'Weather Station 2', 'Weather Station 2'],
        'latitude': [52.5, 52.5, 53.0, 53.0],
        'longitude': [13.4, 13.4, 14.0, 14.0],
        'elevation': [100, 100, 150, 150],
        'variable_unit': ['°C', '%', '°C', 'hPa'],
        'variable_description': ['Air temperature at 2m', 'Relative humidity', 'Air temperature at 2m', 'Atmospheric pressure']
    })

    # Insert data - stations and variables will be created automatically
    meteo_db.insert_data(df)

    # Query results
    stations = meteo_db.query_station()
    measurements = meteo_db.query_data()

    print("Stations:")
    for station in stations:
        print(f"  ID: {station.id}, Provider: {station.provider}, External ID: {station.external_id}, Name: {station.name}")

    print(f"\nTotal measurements: {len(measurements)}")

    # Example 3: Insert data with provider as parameter instead of column
    df2 = pd.DataFrame({
        'external_station_id': ['STATION_003'],
        'variable_name': ['wind_speed'],
        'timestamp': [datetime.now()],
        'value': [5.2],
        'variable_unit': ['m/s']
    })

    meteo_db.insert_data(df2, provider='wind_service')

    print(f"Total stations after second insert: {len(meteo_db.query_station())}")
    print(f"Total measurements after second insert: {len(meteo_db.query_data())}")

    # Example 4: Query measurements with filters
    print("\nQuerying measurements for weather_service provider:")
    weather_data = meteo_db.query_measurements(provider='weather_service')
    print(weather_data.head())

    print(f"\nAvailable variables: {meteo_db.list_variables()}")
    print(f"Available providers: {meteo_db.list_providers()}")