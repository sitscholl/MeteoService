# API Query Examples

This directory contains example JSON files for making requests to the meteorological data API.

## Timezone Handling Options

The API supports three different ways to handle timezones in your requests:

### 1. Timezone-Aware Datetimes (Recommended)
**File:** `query_timezone_aware.json`

Use ISO 8601 format with timezone offset:
```json
{
  "provider": "SBR",
  "start_time": "2025-08-25T00:00:00+02:00",
  "end_time": "2025-08-26T23:59:59+02:00",
  "tags": {
    "station_id": "103"
  }
}
```

### 2. UTC Timezone
**File:** `query_utc.json`

Use UTC timezone with 'Z' suffix:
```json
{
  "provider": "SBR",
  "start_time": "2025-08-25T00:00:00Z",
  "end_time": "2025-08-26T23:59:59Z",
  "tags": {
    "station_id": "103"
  },
  "fields": ["tair_2m", "precipitation", "relative_humidity"]
}
```

### 3. Naive Datetimes with Timezone Parameter
**File:** `query_naive_with_timezone.json`

Use naive datetimes with explicit timezone:
```json
{
  "provider": "SBR",
  "start_time": "2025-08-25T00:00:00",
  "end_time": "2025-08-26T23:59:59",
  "timezone": "Europe/Rome",
  "tags": {
    "station_id": "103"
  }
}
```

### 4. Naive Datetimes with Default Timezone
**File:** `query_naive_default_timezone.json`

Use naive datetimes (will use default timezone from config):
```json
{
  "provider": "SBR",
  "start_time": "2025-08-25T10:30:00",
  "end_time": "2025-08-25T14:30:00",
  "tags": {
    "station_id": "103"
  },
  "fields": ["tair_2m", "precipitation"]
}
```

## How to Use

### Using curl:
```bash
# Example with timezone-aware datetime
curl -X POST "http://localhost:8000/query" \
  -H "Content-Type: application/json" \
  -d @examples/api_requests/query_timezone_aware.json

# Example with UTC
curl -X POST "http://localhost:8000/query" \
  -H "Content-Type: application/json" \
  -d @examples/api_requests/query_utc.json
```

### Using Python requests:
```python
import requests
import json

# Load example JSON
with open('examples/api_requests/query_timezone_aware.json', 'r') as f:
    query_data = json.load(f)

# Make request
response = requests.post(
    'http://localhost:8000/query',
    json=query_data
)

print(response.json())
```

## Available Fields

Common meteorological fields you can request:
- `tair_2m`: Air temperature at 2m height
- `tsoil_25cm`: Soil temperature at 25cm depth
- `precipitation`: Precipitation amount
- `relative_humidity`: Relative humidity
- `wind_speed`: Wind speed
- `wind_gust`: Maximum wind gust
- `tdry_60cm`: Dry temperature at 60cm
- `twet_60cm`: Wet temperature at 60cm
- `irrigation`: Irrigation status
- `leaf_wetness`: Leaf wetness

## Timezone Utilities

Check available timezones and get conversion help:
```bash
# Get timezone information
curl "http://localhost:8000/timezones"

# Convert between timezones
curl "http://localhost:8000/timezones/convert?datetime_str=2025-08-25T10:30:00&from_tz=Europe/Rome&to_tz=UTC"
```

## Response Format

The API returns data in this format:
```json
{
  "data": [
    {
      "datetime": "2025-08-25T00:00:00+02:00",
      "tair_2m": 23.5,
      "precipitation": 0.0,
      "station_id": "103"
    }
  ],
  "count": 1,
  "time_range": {
    "start": "2025-08-25T00:00:00+02:00",
    "end": "2025-08-25T00:00:00+02:00"
  },
  "metadata": {
    "provider": "SBR",
    "tags": {"station_id": "103"},
    "fields": ["tair_2m", "precipitation"],
    "query_timezone": "Europe/Rome",
    "result_timezone": "Europe/Rome"
  }
}
```