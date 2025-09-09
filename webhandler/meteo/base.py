from abc import ABC, abstractmethod

class MeteoHandler(ABC):
    """Abstract base class for meteo handlers."""

    @abstractmethod
    def get_stationdata(self, picture, destination_dir):
        """
        Retrieves meteorological stationdata
        """
        pass

    @abstractmethod
    def __enter__(self):
        """Enter context management."""
        return self

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context management."""
        pass

import pandera.pandas as pa

import os
from abc import ABC, abstractmethod


class BaseMeteoHandler(ABC):

    @abstractmethod
    def __enter__(self):
        """
        Allows the object to be used in a 'with' block.
        """

    @abstractmethod
    def __exit__(self, exc_type, exc_value, traceback):
        """
        Ensure the session is closed when exiting the 'with' block.
        """

    @abstractmethod
    def get_data(self, **kwargs):
        """Query the raw data."""

    @abstractmethod
    def transform(self, raw_data):
        """Transform the raw data."""

    @property
    def output_schema(self):
        return pa.DataFrameSchema(
            {
                "Field": pa.Column(str),
                "Variety": pa.Column(str),
                "Sector": pa.Column(str),
                "Variety Group": pa.Column(str),
                "Year": pa.Column(int, pa.Check.ge(0)),
                "Tree Age": pa.Column(int),
                "Tree Height": pa.Column(float, nullable=True),
                "Harvest rounds": pa.Column(float, nullable=True),
                "Count Zupfen": pa.Column(float, nullable=True),
                "Count Ernte": pa.Column(float, nullable=True),
                "Hours Zupfen": pa.Column(float, nullable=True),
                "Hours Ernte": pa.Column(float, nullable=True)
            },
            index = pa.Index(int),
        )

    def validate(self, transformed_data):
        """Validate the transformed data."""
        self.output_schema.validate(transformed_data)

    def save(self, validated_data):
        """Save the transformed data to a database."""
        pass

    def run(self, **kwargs):
        """Run the data cleaning pipeline."""
        raw_data = self.get_stationdata(**kwargs)
        output = self.transform(raw_data)
        self.validate(output)
        self.save(output)