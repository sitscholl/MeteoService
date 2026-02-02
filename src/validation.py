from pydantic import BaseModel, Field, field_validator
import pandas as pd

from datetime import datetime
from typing import Optional, List, Dict, Any
import pytz

DEFAULT_TIMEZONE = "UTC"

# Pydantic models for request/response
class TimeseriesQuery(BaseModel):
    provider: str = Field(..., description="Provider name (e.g., 'SBR')")
    start_time: Optional[datetime] = Field(None, description="Start time (ISO format with timezone, e.g., '2025-01-15T10:30:00+01:00')")
    end_time: Optional[datetime] = Field(None, description="End time (ISO format with timezone, e.g., '2025-01-15T10:30:00+01:00')")
    station_id: str = Field(..., description="External id of the station to return")
    variables: Optional[List[str]] = Field(None, description="Specific variables to return")
    timezone: Optional[str] = Field(None, description="Timezone for naive datetimes (e.g., 'Europe/Rome'). Only used if start_time/end_time are timezone-naive.")

    @field_validator('start_time', 'end_time', mode='before')
    @classmethod
    def ensure_timezone_aware(cls, v, info):
        """Ensure datetime is timezone-aware, using timezone field if provided."""
        if v is None:
            return v
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
    time_range: Dict[str, datetime] | None
    metadata: Dict[str, Any] | None

    @classmethod
    def from_dataframe(cls, df, latest = False):

        if df.empty:
            return cls(
                data = [],
                count = 0,
                time_range = None,
                metadata = None
            )

        if latest:
            df = df.sort_index().iloc[[-1]]

        data = []
        for timestamp, row in df.iterrows():
            try:
                ts_val = timestamp.isoformat()
            except Exception:
                ts_val = str(timestamp)
            row_clean = row.where(pd.notna(row), None).to_dict()
            record = {"datetime": ts_val, **row_clean}
            data.append(record)

        return cls(
            data=data,
            count=len(data),
            time_range={"start": df.index.min(), "end": df.index.max()},
            metadata=None,
        )


class DatabaseStats(BaseModel):
    providers: List[str]
    total_points: int
    time_ranges: Dict[str, Dict[str, datetime]]
