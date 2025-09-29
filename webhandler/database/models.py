from sqlalchemy import (
    Column, Integer, Float, String, ForeignKey, DateTime, Text, UniqueConstraint
)
from sqlalchemy.dialects.sqlite import JSON
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class Station(Base):
    __tablename__ = "stations"

    id = Column(Integer, primary_key=True)
    provider = Column(String, nullable=False)
    external_id = Column(String, nullable=False)
    name = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    elevation = Column(Float)
    station_metadata = Column(JSON)
    
    __table_args__ = (UniqueConstraint("provider", "external_id"), )

    measurements = relationship("Measurement", back_populates="station")


class Variable(Base):
    __tablename__ = "variables"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)   # e.g. "temperature_2m"
    unit = Column(String)
    description = Column(Text)

    measurements = relationship("Measurement", back_populates="variable")


class Measurement(Base):
    __tablename__ = "measurements"

    id = Column(Integer, primary_key=True)
    station_id = Column(Integer, ForeignKey("stations.id"), nullable=False)
    variable_id = Column(Integer, ForeignKey("variables.id"), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    value = Column(Float)

    station = relationship("Station", back_populates="measurements")
    variable = relationship("Variable", back_populates="measurements")

    __table_args__ = (UniqueConstraint("station_id", "variable_id", "timestamp"), )