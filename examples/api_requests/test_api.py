#!/usr/bin/env python3
"""
Simple test script for the meteorological data API timezone handling.
This script demonstrates how to use the different JSON examples.
"""

import requests
import json
import os
from pathlib import Path

# API base URL
API_BASE_URL = "http://localhost:8000"

def load_json_example(filename):
    """Load a JSON example file."""
    examples_dir = Path(__file__).parent
    file_path = examples_dir / filename
    
    with open(file_path, 'r') as f:
        return json.load(f)

def test_query_endpoint(example_name, json_data):
    """Test the query endpoint with given JSON data."""
    print(f"\n{'='*50}")
    print(f"Testing: {example_name}")
    print(f"{'='*50}")
    
    try:
        response = requests.post(
            f"{API_BASE_URL}/query",
            json=json_data,
            timeout=30
        )
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"Data points returned: {result['count']}")
            print(f"Time range: {result['time_range']['start']} to {result['time_range']['end']}")
            print(f"Query timezone: {result['metadata'].get('query_timezone', 'N/A')}")
            print(f"Result timezone: {result['metadata'].get('result_timezone', 'N/A')}")
            
            if result['data']:
                print(f"First data point: {result['data'][0]}")
        else:
            print(f"Error: {response.text}")
            
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")

def test_timezone_utilities():
    """Test the timezone utility endpoints."""
    print(f"\n{'='*50}")
    print("Testing Timezone Utilities")
    print(f"{'='*50}")
    
    try:
        # Test timezone info endpoint
        response = requests.get(f"{API_BASE_URL}/timezones")
        if response.status_code == 200:
            tz_info = response.json()
            print(f"Default timezone: {tz_info['default_timezone']}")
            print(f"Current UTC time: {tz_info['current_time_utc']}")
            print(f"Current default time: {tz_info['current_time_default']}")
        
        # Test timezone conversion
        response = requests.get(
            f"{API_BASE_URL}/timezones/convert",
            params={
                "datetime_str": "2025-08-25T10:30:00",
                "from_tz": "Europe/Rome",
                "to_tz": "UTC"
            }
        )
        if response.status_code == 200:
            conversion = response.json()
            print(f"Conversion example:")
            print(f"  Original: {conversion['original']['datetime']} ({conversion['original']['timezone']})")
            print(f"  Converted: {conversion['converted']['datetime']} ({conversion['converted']['timezone']})")
            
    except requests.exceptions.RequestException as e:
        print(f"Timezone utilities test failed: {e}")

def main():
    """Run all tests."""
    print("Testing Meteorological Data API - Timezone Handling")
    
    # Test health endpoint first
    try:
        response = requests.get(f"{API_BASE_URL}/health")
        if response.status_code != 200:
            print(f"API health check failed: {response.status_code}")
            print("Make sure the API server is running with: uvicorn webhandler.api:app --reload")
            return
        print("✓ API is healthy")
    except requests.exceptions.RequestException:
        print("✗ Cannot connect to API server")
        print("Make sure the API server is running with: uvicorn webhandler.api:app --reload")
        return
    
    # Test examples
    examples = [
        ("Timezone-aware datetimes", "query_timezone_aware.json"),
        ("UTC timezone", "query_utc.json"),
        ("Naive with timezone parameter", "query_naive_with_timezone.json"),
        ("Naive with default timezone", "query_naive_default_timezone.json")
    ]
    
    for name, filename in examples:
        try:
            json_data = load_json_example(filename)
            test_query_endpoint(name, json_data)
        except FileNotFoundError:
            print(f"Example file not found: {filename}")
        except json.JSONDecodeError:
            print(f"Invalid JSON in file: {filename}")
    
    # Test timezone utilities
    test_timezone_utilities()
    
    print(f"\n{'='*50}")
    print("Testing completed!")
    print(f"{'='*50}")

if __name__ == "__main__":
    main()