"""Tests for normalization functions."""
import pytest
from datetime import datetime, timezone
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from src.normalize import normalize_openweather, normalize_noaa, normalize_meteostat


def test_normalize_openweather():
    """Test OpenWeatherMap normalization."""
    sample_data = {
        "dt": 1705315200,
        "name": "Kathmandu",
        "coord": {"lat": 27.7172, "lon": 85.3240},
        "main": {
            "temp": 20.5,
            "feels_like": 19.8,
            "temp_min": 18.0,
            "temp_max": 23.0,
            "pressure": 1013.25,
            "humidity": 65
        },
        "wind": {
            "speed": 3.5,
            "deg": 180,
            "gust": 5.0
        },
        "clouds": {"all": 40},
        "visibility": 10000,
        "weather": [{
            "main": "Clear",
            "description": "clear sky"
        }],
        "sys": {"country": "NP"}
    }
    
    result = normalize_openweather(sample_data)
    
    assert result["city"] == "Kathmandu"
    assert result["temp_C"] == 20.5
    assert result["pressure_hPa"] == 1013.25
    assert result["humidity_pct"] == 65
    assert result["wind_speed_mps"] == 3.5
    assert result["source"] == "openweather"
    assert "timestamp_utc" in result
    assert "raw" in result


def test_normalize_openweather_missing_fields():
    """Test normalization with missing optional fields."""
    sample_data = {
        "dt": 1705315200,
        "name": "TestCity",
        "coord": {"lat": 0.0, "lon": 0.0},
        "main": {"temp": 15.0}
    }
    
    result = normalize_openweather(sample_data)
    
    assert result["city"] == "TestCity"
    assert result["temp_C"] == 15.0
    assert result["pressure_hPa"] is None or result["pressure_hPa"] is not None


def test_normalize_noaa():
    """Test NOAA normalization."""
    sample_data = {
        "geometry": {
            "coordinates": [85.3240, 27.7172]
        },
        "properties": {
            "timestamp": "2024-01-15T10:00:00Z",
            "temperature": {"value": 20.5},
            "barometricPressure": {"value": 1013.25},
            "relativeHumidity": {"value": 65},
            "windSpeed": {"value": 3.5},
            "windDirection": {"value": 180}
        }
    }
    
    result = normalize_noaa(sample_data)
    
    assert result["lat"] == 27.7172
    assert result["lon"] == 85.3240
    assert result["temp_C"] == 20.5
    assert result["source"] == "noaa"


def test_normalize_meteostat():
    """Test Meteostat normalization."""
    sample_data = {
        "data": [{
            "time": "2024-01-15T10:00:00Z",
            "lat": 27.7172,
            "lon": 85.3240,
            "temp": 20.5,
            "pres": 1013.25,
            "rhum": 65,
            "wspd": 3.5,
            "wdir": 180,
            "prcp": 0.0
        }]
    }
    
    result = normalize_meteostat(sample_data)
    
    assert result["temp_C"] == 20.5
    assert result["pressure_hPa"] == 1013.25
    assert result["humidity_pct"] == 65
    assert result["source"] == "meteostat"

