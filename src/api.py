"""
FastAPI integration example for the MeteoDB timeseries database.
This demonstrates how to expose the database via REST API for fast access to cached meteo data.
"""

from fastapi import FastAPI, HTTPException, Depends, Query, BackgroundTasks
from fastapi.responses import JSONResponse

from datetime import datetime
from typing import Optional, List, Dict
import pytz
import logging

from . import validation as validation_module
from .validation import TimeseriesResponse, TimeseriesQuery
from .runtime import RuntimeContext
from .workflow import QueryWorkflow

logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Meteorological Data API",
    description="Fast access to cached meteorological timeseries data with smart data fetching",
    version="1.0.0"
)

## Initialize runtime context
runtime = RuntimeContext.from_config_file("config/config.yaml")
workflow = QueryWorkflow(runtime)

# Default timezone for the API (can be overridden by client)
DEFAULT_TIMEZONE = runtime.default_timezone
validation_module.DEFAULT_TIMEZONE = DEFAULT_TIMEZONE

def get_workflow() -> QueryWorkflow:
    return workflow

@app.on_event("shutdown")
def shutdown_event():
    runtime.db.close()

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
async def health_check():
    """Health check endpoint."""
    try:
        stations = runtime.db.query_station()
        return {
            "status": "healthy",
            "station_count": len(stations),
            "timestamp": datetime.now(pytz.timezone(DEFAULT_TIMEZONE))
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Database unavailable")

@app.get("/providers", response_model=List[str])
async def get_providers():
    """Get list of available providers."""
    try:
        return runtime.provider_manager.list_providers()
    except Exception as e:
        logger.error(f"Failed to get providers: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve providers")

@app.post("/query", response_model=TimeseriesResponse)
async def query_timeseries(
    query: TimeseriesQuery,
    background_tasks: BackgroundTasks,
    workflow: QueryWorkflow = Depends(get_workflow),
):
    try:
        response, pending = await workflow.run_timeseries_query(query)
        if pending is not None and not pending.empty:
            background_tasks.add_task(runtime.db.insert_data, pending, query.provider)
        return response
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Query failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")

@app.get("/query", response_model=TimeseriesResponse)
async def query_timeseries_get(
        background_tasks: BackgroundTasks,
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
        workflow: QueryWorkflow = Depends(get_workflow),
    ):
    # Support comma-separated fallback (besides repeated ?variables=)
    if variables and len(variables) == 1 and "," in variables[0]:
        variables = [v.strip() for v in variables[0].split(",") if v.strip()]

    try:
        q = TimeseriesQuery(
            provider=provider,
            station_id=station_id,
            start_time=start_date,
            end_time=end_date,
            variables=variables,
            timezone=timezone or DEFAULT_TIMEZONE,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        response, pending = await workflow.run_timeseries_query(q)
        if pending is not None and not pending.empty:
            background_tasks.add_task(runtime.db.insert_data, pending, provider)
        return response
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
