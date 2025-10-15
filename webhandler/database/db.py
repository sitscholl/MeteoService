from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd

import logging
from datetime import datetime, timezone
import pytz

from . import models
from ..provider_manager import ProviderManager

logger = logging.getLogger(__name__)

class MeteoDB:

    def __init__(self, engine: str = 'sqlite:///database.db'):
        self.engine = create_engine(engine)
        models.Base.metadata.create_all(self.engine)
        self.session = sessionmaker(bind=self.engine, autocommit = False, autoflush = False)()
        self.provider_manager_initialized = False

    def initialize_provider_manager(self, provider_manager: ProviderManager):
        self.provider_manager = provider_manager
        self.provider_manager_initialized = True
        logger.info("Provider manager initialized successfully.")

    def query_station(self, provider: str | None = None, external_id: str | None = None):
        query = self.session.query(models.Station)
        if provider is not None:
            query = query.filter(models.Station.provider == provider)
        if external_id is not None:
            query = query.filter(models.Station.external_id == external_id)
        return query.all()

    def query_variable(self, name: str = None):
        query = self.session.query(models.Variable)
        if name is not None:
            query = query.filter(models.Variable.name == name)
        return query.all()

    def query_data(
            self,
            provider: str,
            station_id: str,
            start_time: datetime,
            end_time: datetime,
            variables: list[str] | None = None,
        ):

        orig_timezone = start_time.tzinfo
        start_time_utc = start_time.astimezone(timezone.utc)
        end_time_utc = end_time.astimezone(timezone.utc)

        query = (
            self.session.query(
                models.Measurement.datetime.label("datetime"),
                models.Measurement.value.label("value"),
                models.Station.external_id.label("station_id"),
                models.Variable.name.label("variable"),
            )
            .join(models.Station, models.Measurement.station_id == models.Station.id)
            .join(models.Variable, models.Measurement.variable_id == models.Variable.id)
            .filter(
                models.Station.external_id == station_id,
                models.Measurement.datetime.between(start_time_utc, end_time_utc)
            )
        )

        if variables is not None:
            query = query.filter(
                models.Measurement.variable_id.in_(
                    [v.id for v in self.query_variable(name=variables)]
                )
            )

        df = pd.read_sql_query(sql=query.statement, con=self.engine)

        if not df.empty:
            try:
                df['datetime'] = df['datetime'].dt.tz_localize(timezone.utc).dt.tz_convert(orig_timezone)
            except Exception as e:
                logger.warning(f"Could not convert timezone back to {orig_timezone}: {e}. Keeping UTC timezone.")
                # Ensure index is UTC-aware if conversion fails
                if df['datetime'].tz is None:
                    df['datetime'] = df['datetime'].tz_localize('UTC')

            df = df.pivot(columns = 'variable', values = 'value', index = ['station_id', 'datetime'])
            df.reset_index(level = 0, inplace = True)

        return df

    def insert_station(self, provider: str, external_id: str, **kwargs):
        """
        Get existing station if it already exists or create a new one.
        """
        existing_station = self.query_station(provider=provider, external_id=external_id)

        if existing_station:
            return existing_station[0]

        #Fetch station information
        station_info = None
        if not self.provider_manager_initialized:
            logger.info("ProviderManager is not initialized. Cannot fetch station info")
        elif self.provider_manager.get_provider(provider.lower()) is None:
            logger.warning(f"Provider handler for provider '{provider}' could not be found. Station metadata will not be fetched. Available providers: {self.provider_manager.list_providers()}")
        else:
            try:
                with self.provider_manager.get_provider(provider.lower()) as provider_handler:
                    station_info = provider_handler.get_station_info(external_id)
                    station_info.update(**kwargs)
            except Exception as e:
                logger.error(f"Error fetching station information: {e}")

        if station_info is None:
            station_info = kwargs

        try:
            new_station = models.Station(provider = provider, external_id = external_id, **station_info)
            self.session.add(new_station)
            self.session.commit()
            logger.info(f"New station {new_station.external_id} inserted successfully.")
            self.session.refresh(new_station)
            return new_station
        except Exception as e:
            logger.error(f"Error inserting new station: {e}")
            return

    def insert_variable(self, name: str, unit: str | None = None, description: str | None = None):
        """
        Get existing variable if it already exists or create a new one.
        """
        existing_variable = self.query_variable(name=name)

        if existing_variable:
            return existing_variable[0]

        try:
            new_variable = models.Variable(name = name, unit = unit, description = description)
            self.session.add(new_variable)
            self.session.commit()
            logger.info(f"New variable {new_variable.name} inserted successfully.")
            self.session.refresh(new_variable)
            return new_variable
        except Exception as e: 
            logger.error(f"Error inserting new variable: {e}")
            return

    def insert_data(
        self, 
        data: pd.DataFrame, 
        provider: str,
        index=False, index_label=None, if_exists='append'):
        """
        Insert measurement data into the database. All columns other than 'datetime' and 'station_id' are assumed to contain variables and will be inserted into the Measurement table.
        Stations and variables which do not exist in the database will be created automatically in the respective tables.

        Expected DataFrame columns:
        - 'datetime': Measurement timestamp
        - 'station_id': External id of the station

        Optional attrs for station creation:
        - 'station_name', 'latitude', 'longitude', 'elevation'

        Optional attrs for variable creation:
        - 'variable_unit', 'variable_description'
        """
        if data.empty:
            logger.warning("Empty DataFrame provided to insert_data")
            return

        # Validate required columns
        required_cols = ['datetime', 'station_id']
        missing_cols = [col for col in required_cols if col not in data.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        variable_columns = [col for col in data.columns if col not in required_cols]
        if len(variable_columns) == 0:
            logger.warning('No variable columns found in data')
            return

        # Make a copy to avoid modifying the original DataFrame
        df = data.copy()

        for st_id, station_data in df.groupby('station_id'):

            #Get internal station entry and create if it does not exist
            # ##TODO: Query station info from provider class and get all attributes 
            station_entry = self.insert_station(provider, st_id)

            if station_entry is None:
                logger.warning(f"Skipping insertion of data from {st_id} as station could not be inserted into database")
                continue

            for var in variable_columns:
                variable_entry = self.insert_variable(name=var)

                if variable_entry is None:
                    logger.warning(f"Skipping insertion of data from {st_id} and variable {var} as variable could not be inserted into database")
                    continue

                measurements = station_data[['datetime', var]].copy()
                measurements['datetime'] = measurements['datetime'].dt.tz_convert('UTC')
                measurements['station_id'] = station_entry.id
                measurements['variable_id'] = variable_entry.id
                measurements.rename(columns = {var: 'value'}, inplace = True)
                
                try:
                    measurements.to_sql(
                        name='measurements',
                        con=self.engine,
                        index=index,
                        index_label=index_label,
                        if_exists=if_exists
                    )
                    self.session.commit()
                    logger.info(f"Successfully inserted {len(measurements)} measurements values for station {st_id} and variable {var}")
                except Exception as e:
                    self.session.rollback()
                    logger.error(f"Error inserting data for station {st_id} and variable {var}: {e}")


if __name__ == "__main__":
    import numpy as np
    from ..config import load_config

    config = load_config('config/config.yaml')

    # Create database instance
    meteo_db = MeteoDB()
    provider_manager = ProviderManager(provider_config = config['providers'])
    meteo_db.initialize_provider_manager(provider_manager)

    # Example 2: Insert data with automatic station and variable creation
    # Create sample data with external station IDs and variable names
    df = pd.DataFrame({
        'datetime': np.datetime64('2017-01-01') + np.array([np.timedelta64(i, 'D') for i in np.random.randint(0, 1000, size = 4)]),
        'station_id': ['3', '3', '32', '32'],
        "tair_2m": np.random.rand(4),                           # Temperature 2m8
        "tsoil_25cm": np.random.rand(4),     # Soil temperature -25cm
        "tdry_60cm": np.random.rand(4),           # Dry temperature 60cm
        "twet_60cm": np.random.rand(4),           # Wet temperature
        "relative_humidity": np.random.rand(4),           # Relative humidity
        "wind_speed": np.random.rand(4), 
    })

    # Insert data - stations and variables will be created automatically
    meteo_db.insert_data(df, provider = 'SBR')

    # Query results
    stations = meteo_db.query_station()
    measurements = meteo_db.query_data()

    print("Stations:")
    for station in stations:
        print(f"  ID: {station.id}, Provider: {station.provider}, External ID: {station.external_id}, Name: {station.name}")

    print(f"\nTotal measurements: {len(measurements)}")

    # # Example 3: Insert data with provider as parameter instead of column
    # df2 = pd.DataFrame({
    #     'external_station_id': ['STATION_003'],
    #     'variable_name': ['wind_speed'],
    #     'timestamp': [datetime.now()],
    #     'value': [5.2],
    #     'variable_unit': ['m/s']
    # })

    # meteo_db.insert_data(df2, provider='wind_service')

    # print(f"Total stations after second insert: {len(meteo_db.query_station())}")
    # print(f"Total measurements after second insert: {len(meteo_db.query_data())}")

    # # Example 4: Query measurements with filters
    # print("\nQuerying measurements for weather_service provider:")
    # weather_data = meteo_db.query_measurements(provider='weather_service')
    # print(weather_data.head())

    # print(f"\nAvailable variables: {meteo_db.list_variables()}")
    # print(f"Available providers: {meteo_db.list_providers()}")