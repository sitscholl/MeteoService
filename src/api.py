"""
FastAPI integration example for the MeteoDB timeseries database.
This demonstrates how to expose the database via REST API for fast access to cached meteo data.
"""

from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator
import pandas as pd

from datetime import datetime
from typing import Optional, List, Dict, Any, Union
import pytz
import logging

from .config import load_config
from .database.db import MeteoDB
from .query_manager import QueryManager
from .provider_manager import ProviderManager

# Load config file
config = load_config("config/config.yaml")

logger = logging.getLogger(__name__)

# Default timezone for the API (can be overridden by client)
DEFAULT_TIMEZONE = config.get('api', {}).get('default_timezone', 'UTC')

# Initialize FastAPI app
app = FastAPI(
    title="Meteorological Data API",
    description="Fast access to cached meteorological timeseries data with smart data fetching",
    version="1.0.0"
)

# Initialize ProviderManager
provider_manager = ProviderManager(config['providers'])

# Initialize QueryManager
query_manager = QueryManager(config, provider_manager = provider_manager)

# Database dependency with proper context management
async def get_db():
    """
    Dependency to get database instance with proper connection management.
    Uses async context manager to ensure database is properly connected and disconnected.
    """
    db = MeteoDB(config.get('database', {}).get('path', 'sqlite:///database.db'), provider_manager=provider_manager)
    try:
        yield db
    finally:
        db.close()

def get_query_manager() -> QueryManager:
    """Dependency to get data manager instance."""
    return query_manager

# Pydantic models for request/response
class TimeseriesQuery(BaseModel):
    provider: str = Field(..., description="Provider name (e.g., 'SBR')")
    start_time: datetime = Field(..., description="Start time (ISO format with timezone, e.g., '2025-01-15T10:30:00+01:00')")
    end_time: datetime = Field(..., description="End time (ISO format with timezone, e.g., '2025-01-15T10:30:00+01:00')")
    station_id: str = Field(..., description="External id of the station to return")
    variables: Optional[List[str]] = Field(None, description="Specific variables to return")
    timezone: Optional[str] = Field(None, description="Timezone for naive datetimes (e.g., 'Europe/Rome'). Only used if start_time/end_time are timezone-naive.")

    @field_validator('start_time', 'end_time', mode='before')
    @classmethod
    def ensure_timezone_aware(cls, v, info):
        """Ensure datetime is timezone-aware, using timezone field if provided."""
        if isinstance(v, str):
            # Parse string to datetime
            try:
                v = datetime.fromisoformat(v.replace('Z', '+00:00'))
            except ValueError:
                # Try parsing without timezone info
                v = datetime.fromisoformat(v)

        if isinstance(v, datetime):
            if v.tzinfo is None:
                # Naive datetime - use timezone field or default
                # In Pydantic v2, we need to get timezone from the data being validated
                data = info.data if hasattr(info, 'data') else {}
                tz_name = data.get('timezone', DEFAULT_TIMEZONE)
                try:
                    tz = pytz.timezone(tz_name)
                    v = tz.localize(v)
                except pytz.exceptions.UnknownTimeZoneError:
                    raise ValueError(f"Unknown timezone: {tz_name}")

        return v

    @field_validator('timezone')
    @classmethod
    def validate_timezone(cls, v):
        """Validate timezone string."""
        if v is not None:
            try:
                pytz.timezone(v)
            except pytz.exceptions.UnknownTimeZoneError:
                raise ValueError(f"Unknown timezone: {v}")
        return v

class TimeseriesResponse(BaseModel):
    data: List[Dict[str, Any]]
    count: int
    time_range: Dict[str, datetime]
    metadata: Dict[str, Any]

class DatabaseStats(BaseModel):
    providers: List[str]
    total_points: int
    time_ranges: Dict[str, Dict[str, datetime]]

# API Routes

@app.get("/", response_model=Dict[str, str])
async def root():
    """Root endpoint with API information."""
    return {
        "message": "Meteorological Data API",
        "version": "1.0.0",
        "docs": "/docs",
        "timezone_info": f"""
            default_timezone: {DEFAULT_TIMEZONE},
            supported_formats: [
                "2025-01-15T10:30:00+01:00 (timezone-aware)",
                "2025-01-15T10:30:00Z (UTC)",
                "2025-01-15T10:30:00 (naive, uses timezone parameter or default)"
            ]
        """
    }

@app.get("/health")
async def health_check(db: MeteoDB = Depends(get_db)):
    """Health check endpoint."""
    try:
        stations = db.query_station()
        return {
            "status": "healthy",
            "station_count": len(stations),
            "timestamp": datetime.now(pytz.timezone(DEFAULT_TIMEZONE))
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Database unavailable")

@app.get("/providers", response_model=List[str])
async def get_providers(db: MeteoDB = Depends(get_db)):
    """Get list of available providers."""
    try:
        return db.get_providers()
    except Exception as e:
        logger.error(f"Failed to get providers: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve providers")

@app.get("/providers/{provider}/stations")
async def get_provider_stations(
    provider: str,
    db: MeteoDB = Depends(get_db)
):
    """Get available stations for a specific provider."""
    try:
        provider_stations = db.query_station(provider = provider)
        if not provider_stations:
            raise HTTPException(status_code=404, detail=f"No stations for provider '{provider}' found")
        return provider_stations
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get stations for {provider}: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve stations")

##Endpoints to query data
#Helper function that is called by query endpoints
async def _run_timeseries_query(
    query: TimeseriesQuery,
    db: MeteoDB,
    query_manager: QueryManager
) -> TimeseriesResponse:
    if query.start_time >= query.end_time:
        raise HTTPException(status_code=400, detail="start_time must be before end_time")

    df = query_manager.get_data(
        db=db,
        provider=query.provider,
        station_id=query.station_id,
        start_time=query.start_time,
        end_time=query.end_time,
        variables=query.variables,
    )
    station = db.query_station(provider=query.provider, external_id=query.station_id)
    if not station:
        station_info = {}
    else:
        station = station[0]
        station_info = {"elevation": station.elevation, 'latitude': station.latitude, 'longitude': station.longitude}

    query_metadata = {
        "provider": query.provider,
        "station": query.station_id,
        "elevation": station_info.get('elevation'),
        "latitude": station_info.get('latitude'),
        "longitude": station_info.get('longitude'),
        "variables": query.variables,
        "timezone_used": str(query.start_time.tzinfo),
        }

    if df.empty:
        return TimeseriesResponse(
            data=[],
            count=0,
            time_range={"start": query.start_time, "end": query.end_time},
            metadata=query_metadata,
        )

    data = []
    for timestamp, row in df.iterrows():
        try:
            ts_val = timestamp.isoformat()
        except Exception:
            ts_val = str(timestamp)
        row_clean = row.where(pd.notna(row), None).to_dict()
        record = {"datetime": ts_val, **row_clean}
        data.append(record)

    query_metadata['result_timezone'] = str(getattr(df.index, "tz", None))

    return TimeseriesResponse(
        data=data,
        count=len(data),
        time_range={"start": df.index.min(), "end": df.index.max()},
        metadata=query_metadata,
    )

@app.post("/query", response_model=TimeseriesResponse)
async def query_timeseries(
        query: TimeseriesQuery,
        db: MeteoDB = Depends(get_db),
        query_manager: QueryManager = Depends(get_query_manager),
    ):
    try:
        return await _run_timeseries_query(query, db, query_manager)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Query failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")

@app.get("/query", response_model=TimeseriesResponse)
async def query_timeseries_get(
        provider: str = Query(..., description="Provider name, e.g., 'SBR'"),
        station_id: str = Query(..., description="External station id"),
        start_date: datetime = Query(..., description="ISO datetime; e.g. 2025-01-15T10:30:00Z"),
        end_date: datetime = Query(..., description="ISO datetime; e.g. 2025-01-15T12:30:00+01:00"),
        variables: Optional[List[str]] = Query(
            None,
            description="Repeat param for multiple, e.g. ?variables=tmp&variables=hum. Also accepts comma-separated.",
        ),
        timezone: Optional[str] = Query(
            None,
            description="Timezone for naive datetimes, e.g. 'Europe/Rome'. Ignored if start/end include tzinfo.",
        ),
        db: MeteoDB = Depends(get_db),
        query_manager: QueryManager = Depends(get_query_manager),
    ):
    # Support comma-separated fallback (besides repeated ?variables=)
    if variables and len(variables) == 1 and "," in variables[0]:
        variables = [v.strip() for v in variables[0].split(",") if v.strip()]

    # Reuse your Pydantic validator logic (timezone localization, checks, etc.)
    try:
        # Validate timezone value proactively (optional—Pydantic will also catch)
        if timezone:
            pytz.timezone(timezone)  # will raise UnknownTimeZoneError if invalid

        q = TimeseriesQuery(
            provider=provider,
            station_id=station_id,
            start_time=start_date,
            end_time=end_date,
            variables=variables,
            timezone=timezone,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        return await _run_timeseries_query(q, db, query_manager)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"GET /query failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")

# Error handlers
@app.exception_handler(ValueError)
async def value_error_handler(request, exc):
    return JSONResponse(
        status_code=400,
        content={"detail": str(exc)}
    )
