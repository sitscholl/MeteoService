"""
FastAPI integration example for the MeteoDB timeseries database.
This demonstrates how to expose the database via REST API for fast access to cached meteo data.
"""

from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List, Dict, Any
import pytz
import logging

from webhandler.config import load_config
from webhandler.db import MeteoDB
from webhandler.query_manager import QueryManager

# Load config file
config = load_config("config/config.yaml")

logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Meteorological Data API",
    description="Fast access to cached meteorological timeseries data with smart data fetching",
    version="1.0.0"
)

# Initialize QueryManager
query_manager = QueryManager(config)

# Database dependency
def get_db() -> MeteoDB:
    """Dependency to get database instance."""
    return MeteoDB(config.get('database', {}).get('path', 'db/db.csv'))

def get_query_manager() -> QueryManager:
    """Dependency to get data manager instance."""
    return query_manager

# Pydantic models for request/response
class TimeseriesQuery(BaseModel):
    measurement: str = Field(..., description="Measurement name (e.g., 'SBR')")
    start_time: datetime = Field(..., description="Start time (ISO format)")
    end_time: datetime = Field(..., description="End time (ISO format)")
    tags: Optional[Dict[str, str]] = Field(None, description="Filter tags")
    fields: Optional[List[str]] = Field(None, description="Specific fields to return")

class TimeseriesResponse(BaseModel):
    data: List[Dict[str, Any]]
    count: int
    time_range: Dict[str, datetime]
    metadata: Dict[str, Any]

class DatabaseStats(BaseModel):
    measurements: List[str]
    total_points: int
    time_ranges: Dict[str, Dict[str, datetime]]

# API Routes

@app.get("/", response_model=Dict[str, str])
async def root():
    """Root endpoint with API information."""
    return {
        "message": "Meteorological Data API",
        "version": "1.0.0",
        "docs": "/docs"
    }

@app.get("/health")
async def health_check(db: MeteoDB = Depends(get_db)):
    """Health check endpoint."""
    try:
        measurements = db.get_measurements()
        return {
            "status": "healthy",
            "measurements_count": len(measurements),
            "timestamp": datetime.now(pytz.timezone(TIMEZONE))
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Database unavailable")

@app.get("/measurements", response_model=List[str])
async def get_measurements(db: MeteoDB = Depends(get_db)):
    """Get list of available measurements."""
    try:
        return db.get_measurements()
    except Exception as e:
        logger.error(f"Failed to get measurements: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve measurements")

@app.get("/measurements/{measurement}/tags")
async def get_measurement_tags(
    measurement: str,
    db: MeteoDB = Depends(get_db)
):
    """Get available tags for a specific measurement."""
    try:
        tags = db.get_tags_for_measurement(measurement)
        if not tags:
            raise HTTPException(status_code=404, detail=f"Measurement '{measurement}' not found")
        return tags
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get tags for {measurement}: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve tags")

# @app.get("/measurements/{measurement}/time-range")
# async def get_measurement_time_range(
#     measurement: str,
#     station_id: Optional[str] = Query(None, description="Filter by station ID"),
#     db: MeteoDB = Depends(get_db)
# ):
#     """Get time range for a measurement, optionally filtered by station."""
#     try:
#         tags = {"station_id": station_id} if station_id else None
#         time_range = db.get_time_range(measurement, tags)

#         if not time_range:
#             raise HTTPException(
#                 status_code=404,
#                 detail=f"No data found for measurement '{measurement}'"
#             )

#         return {
#             "measurement": measurement,
#             "start_time": time_range[0],
#             "end_time": time_range[1],
#             "tags": tags
#         }
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"Failed to get time range for {measurement}: {e}")
#         raise HTTPException(status_code=500, detail="Failed to retrieve time range")

@app.post("/query", response_model=TimeseriesResponse)
async def query_timeseries(
    query: TimeseriesQuery,
    db: MeteoDB = Depends(get_db),
    query_manager: QueryManager = Depends(get_query_manager)
):
    """Query timeseries data with flexible filtering and smart data fetching."""
    try:
        # Ensure timezone awareness
        if query.start_time.tzinfo is None:
            tz = pytz.timezone(TIMEZONE)
            query.start_time = query.start_time.replace(tzinfo=tz)
        if query.end_time.tzinfo is None:
            tz = pytz.timezone(TIMEZONE)
            query.end_time = query.end_time.replace(tzinfo=tz)

        # Validate time range
        if query.start_time >= query.end_time:
            raise HTTPException(
                status_code=400,
                detail="start_time must be before end_time"
            )

        # Use QueryManager to get data (includes smart fetching)
        df = query_manager.get_data(
            db=db,
            measurement=query.measurement,
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
                    "measurement": query.measurement,
                    "tags": query.tags,
                    "fields": query.fields
                }
            )

        # Convert DataFrame to list of dictionaries
        data = []
        for timestamp, row in df.iterrows():
            record = {"datetime": timestamp}
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
                "measurement": query.measurement,
                "tags": query.tags,
                "fields": list(df.columns),
                "data_types": {col: str(dtype) for col, dtype in df.dtypes.items()}
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
        measurements = db.get_measurements()

        # Get time ranges for each measurement
        time_ranges = {}
        total_points = 0

        for measurement in measurements:
            time_range = db.get_time_range(measurement)
            if time_range:
                time_ranges[measurement] = {
                    "start": time_range[0],
                    "end": time_range[1]
                }

                # Rough estimate of points (this could be expensive for large datasets)
                # In production, you might want to cache this or compute it differently
                try:
                    df = db.query_data(measurement, time_range[0], time_range[1])
                    total_points += len(df)
                except:
                    pass  # Skip if query fails

        return DatabaseStats(
            measurements=measurements,
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
