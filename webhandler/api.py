"""
FastAPI integration example for the MeteoDB timeseries database.
This demonstrates how to expose the database via REST API for fast access to cached meteo data.
"""

from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Optional, List, Dict, Any, Union
import pytz
import logging

from webhandler.config import load_config
from webhandler.db import MeteoDB
from webhandler.query_manager import QueryManager

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

# Initialize QueryManager
query_manager = QueryManager(config)

# Database dependency with proper context management
async def get_db():
    """
    Dependency to get database instance with proper connection management.
    Uses async context manager to ensure database is properly connected and disconnected.
    """
    db = MeteoDB(config.get('database', {}).get('path', 'db/db.csv'))
    try:
        # Enter the context manager to connect to the database
        db.__enter__()
        yield db
    finally:
        # Ensure database is properly disconnected
        db.__exit__(None, None, None)

def get_query_manager() -> QueryManager:
    """Dependency to get data manager instance."""
    return query_manager

# Pydantic models for request/response
class TimeseriesQuery(BaseModel):
    provider: str = Field(..., description="Provider name (e.g., 'SBR')")
    start_time: datetime = Field(..., description="Start time (ISO format with timezone, e.g., '2025-01-15T10:30:00+01:00')")
    end_time: datetime = Field(..., description="End time (ISO format with timezone, e.g., '2025-01-15T10:30:00+01:00')")
    tags: Optional[Dict[str, str]] = Field(None, description="Filter tags")
    fields: Optional[List[str]] = Field(None, description="Specific fields to return")
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
        providers = db.get_providers()
        return {
            "status": "healthy",
            "providers_count": len(providers),
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

@app.get("/providers/{provider}/tags")
async def get_provider_tags(
    provider: str,
    db: MeteoDB = Depends(get_db)
):
    """Get available tags for a specific provider."""
    try:
        tags = db.get_tags_for_provider(provider)
        if not tags:
            raise HTTPException(status_code=404, detail=f"provider '{provider}' not found")
        return tags
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get tags for {provider}: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve tags")

# @app.get("/providers/{provider}/time-range")
# async def get_provider_time_range(
#     provider: str,
#     station_id: Optional[str] = Query(None, description="Filter by station ID"),
#     db: MeteoDB = Depends(get_db)
# ):
#     """Get time range for a provider, optionally filtered by station."""
#     try:
#         tags = {"station_id": station_id} if station_id else None
#         time_range = db.get_time_range(provider, tags)
#
#         if not time_range:
#             raise HTTPException(
#                 status_code=404,
#                 detail=f"No data found for provider '{provider
#             )
#
#         return {
#             "provider": provider,
#             "start_time": time_range[0],
#             "end_time": time_range[1],
#             "tags": tags
#         }
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"Failed to get time range for {provider}: {e}")
#         raise HTTPException(status_code=500, detail="Failed to retrieve time range")

@app.post("/query", response_model=TimeseriesResponse)
async def query_timeseries(
    query: TimeseriesQuery,
    db: MeteoDB = Depends(get_db),
    query_manager: QueryManager = Depends(get_query_manager)
):
    """
    Query timeseries data with flexible filtering and smart data fetching.

    Supports timezone-aware datetime inputs in ISO format:
    - "2025-01-15T10:30:00+01:00" (with timezone offset)
    - "2025-01-15T10:30:00Z" (UTC)
    - "2025-01-15T10:30:00" (naive, uses timezone parameter or default)
    """
    try:
        # Validate time range
        if query.start_time >= query.end_time:
            raise HTTPException(
                status_code=400,
                detail="start_time must be before end_time"
            )

        # Use QueryManager to get data (includes smart fetching)
        df = query_manager.get_data(
            db=db,
            provider=query.provider,
            start_time=query.start_time,
            end_time=query.end_time,
            tags=query.tags,
            fields=query.fields
        )

        if df.empty:
            return TimeseriesResponse(
                data=[],
                count=0,
                time_range={
                    "start": query.start_time,
                    "end": query.end_time
                },
                metadata={
                    "provider": query.provider,
                    "tags": query.tags,
                    "fields": query.fields,
                    "timezone_used": str(query.start_time.tzinfo)
                }
            )

        # Convert DataFrame to list of dictionaries
        data = []
        for timestamp, row in df.iterrows():
            # Ensure timestamp is serializable (use ISO format when possible)
            try:
                ts_val = timestamp.isoformat()
            except Exception:
                ts_val = str(timestamp)

            record = {"datetime": ts_val}
            record.update(row.to_dict())
            data.append(record)

        return TimeseriesResponse(
            data=data,
            count=len(data),
            time_range={
                "start": df.index.min(),
                "end": df.index.max()
            },
            metadata={
                "provider": query.provider,
                "tags": query.tags,
                "fields": list(df.columns),
                "data_types": {col: str(dtype) for col, dtype in df.dtypes.items()},
                "query_timezone": str(query.start_time.tzinfo),
                "result_timezone": str(df.index.tz) if getattr(df.index, "tz", None) is not None else None
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Query failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")

@app.get("/stats", response_model=DatabaseStats)
async def get_database_stats(db: MeteoDB = Depends(get_db)):
    """Get overall database statistics."""
    try:
        providers = db.get_providers()

        # Get time ranges for each provider
        time_ranges = {}
        total_points = 0

        for provider in providers:
            time_range = db.get_time_range(provider)
            if time_range:
                time_ranges[provider] = {
                    "start": time_range[0],
                    "end": time_range[1]
                }

                # Rough estimate of points (this could be expensive for large datasets)
                # In production, you might want to cache this or compute it differently
                try:
                    df = db.query_data(provider, time_range[0], time_range[1])
                    total_points += len(df)
                except:
                    pass  # Skip if query fails

        return DatabaseStats(
            providers=providers,
            total_points=total_points,
            time_ranges=time_ranges
        )

    except Exception as e:
        logger.error(f"Failed to get database stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve database statistics")

# Error handlers
@app.exception_handler(ValueError)
async def value_error_handler(request, exc):
    return JSONResponse(
        status_code=400,
        content={"detail": str(exc)}
    )
