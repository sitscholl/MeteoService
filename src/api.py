"""
FastAPI integration example for the MeteoDB timeseries database.
This demonstrates how to expose the database via REST API for fast access to cached meteo data.
"""

from fastapi import FastAPI, HTTPException, Depends, Query, Path, BackgroundTasks
from fastapi.responses import JSONResponse

from datetime import datetime
from typing import Optional, List, Dict
import pytz
import logging

from . import validation as validation_module
from .validation import TimeseriesResponse, TimeseriesQuery
from .runtime import RuntimeContext
from .workflow import QueryWorkflow
from .utils import split_url_parameters

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
        "version": "2.0.0",
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

@app.get("/api/health")
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

@app.get("/api/providers", response_model=List[str])
async def get_providers():
    """Get list of available providers."""
    try:
        return runtime.provider_manager.list_providers()
    except Exception as e:
        logger.error(f"Failed to get providers: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve providers")

@app.get("/api/{provider}/stations", response_model=List[str])
async def get_stations(provider: str):
    """Get list of available stations for a given provider."""
    try:
        provider_handler = runtime.provider_manager.get_provider(provider.lower())
        if provider_handler is None:
            raise ValueError(f"Unknown provider {provider.lower()}. Check /providers endpoint for available providers.")
        async with provider_handler as prv:
            station_list = await provider_handler.get_stations()
        return station_list
    except Exception as e:
        logger.error(f"Failed to get stations for provider {provider}: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve stations")

@app.get("/api/{provider}/{query_type}", response_model=TimeseriesResponse)
async def query_timeseries_get(
        background_tasks: BackgroundTasks,
        provider: str = Path(..., description="Provider name, e.g., 'province'"),
        query_type: str = Path(..., description="Type of query, must be either timeseries to return a longer timeseries, or latest to return the latest measurement."),
        station_id: str = Query(..., description="External station id"),
        start_date: Optional[datetime] = Query(None, description="ISO datetime; e.g. 2025-01-15T10:30:00Z"),
        end_date: Optional[datetime] = Query(None, description="ISO datetime; e.g. 2025-01-15T12:30:00+01:00"),
        variables: Optional[List[str]] = Query(
            None,
            description="Repeat param for multiple, e.g. ?variables=tmp&variables=hum. Also accepts comma-separated.",
        ),
        models: Optional[List[str]] = Query(
            None,
            description="Repeat param for multiple, e.g. ?models=model1&models=model2. Also accepts comma-separated.",
        ),
        timezone: Optional[str] = Query(
            None,
            description="Timezone for naive datetimes, e.g. 'Europe/Rome'. Ignored if start/end include tzinfo.",
        ),
        agg: Optional[str] = Query(
            None,
            description="Optional aggregation frequency. Use '1D' for daily aggregation.",
        ),
        min_size: Optional[int] = Query(
            None,
            ge=1,
            description="Optional minimum number of samples per aggregation bucket. If fewer points are available, return null.",
        ),
        workflow: QueryWorkflow = Depends(get_workflow),
    ):

    provider_handler = runtime.provider_manager.get_provider(provider.lower())
    if provider_handler is None:
        raise HTTPException(status_code=400, detail=f"No provider named {provider} found. Choose one of {runtime.provider_manager.list_providers()}")

    if query_type.lower() not in ['timeseries', 'latest']:
        raise HTTPException(status_code=400, detail=f"Invalid query type: {query_type}. Must be one of 'timeseries' or 'latest'.")
    latest = query_type.lower() == 'latest'

    if latest and (start_date is not None or end_date is not None):
        raise HTTPException(status_code=400, detail="start_date and end_date not allowed for query type latest.")
    if latest and agg is not None:
        raise HTTPException(status_code=400, detail="Aggregation is not supported for query type latest.")

    if agg is not None:
        agg_norm = agg.strip().lower()
        if agg_norm in {"d", "1d"}:
            agg = "1D"
        else:
            raise HTTPException(status_code=400, detail="Only daily aggregation is supported currently. Use agg=1D.")
    elif min_size is not None:
        raise HTTPException(status_code=400, detail="min_size requires aggregation. Use agg=1D.")

    # Support comma-separated fallback (besides repeated ?variables=)
    if variables:
        variables = split_url_parameters(variables)
    if models:
        models = split_url_parameters(models)

    try:
        q = TimeseriesQuery(
            provider=provider,
            station_id=station_id,
            start_time=start_date,
            end_time=end_date,
            variables=variables,
            models = models,
            timezone=timezone or DEFAULT_TIMEZONE,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        response, pending = await workflow.run_timeseries_query(
            q,
            latest=latest,
            agg=agg,
            min_size=min_size,
        )
        if pending is not None and not pending.empty and not latest:
            if provider_handler.cache_data:
                background_tasks.add_task(runtime.db.insert_data, pending, provider_handler)
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
