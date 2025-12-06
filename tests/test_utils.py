"""Tests for utility functions."""
import pytest
from datetime import datetime, timezone, timedelta
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from src.utils import validate_weather_record, deduplicate_by_key, normalize_timezone


def test_validate_weather_record_valid():
    """Test validation of valid weather record."""
    record = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "source": "openweather",
        "temp_C": 20.5,
        "humidity_pct": 65,
        "pressure_hPa": 1013.25
    }
    
    is_valid, error = validate_weather_record(record)
    assert is_valid is True
    assert error is None


def test_validate_weather_record_missing_required():
    """Test validation with missing required field."""
    record = {
        "temp_C": 20.5
        # Missing timestamp_utc and source
    }
    
    is_valid, error = validate_weather_record(record)
    assert is_valid is False
    assert "Missing required field" in error


def test_validate_weather_record_future_timestamp():
    """Test validation with future timestamp."""
    future_time = datetime.now(timezone.utc) + timedelta(days=1)
    record = {
        "timestamp_utc": future_time.isoformat(),
        "source": "openweather",
        "temp_C": 20.5
    }
    
    is_valid, error = validate_weather_record(record)
    assert is_valid is False
    assert "future" in error.lower()


def test_validate_weather_record_invalid_temp():
    """Test validation with invalid temperature."""
    record = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "source": "openweather",
        "temp_C": 150.0  # Too high
    }
    
    is_valid, error = validate_weather_record(record)
    assert is_valid is False
    assert "Temperature out of range" in error


def test_validate_weather_record_invalid_humidity():
    """Test validation with invalid humidity."""
    record = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "source": "openweather",
        "humidity_pct": 150  # > 100%
    }
    
    is_valid, error = validate_weather_record(record)
    assert is_valid is False
    assert "Humidity out of range" in error


def test_deduplicate_by_key():
    """Test deduplication by key fields."""
    records = [
        {"city": "Kathmandu", "timestamp_utc": "2024-01-15T10:00:00Z", "temp": 20},
        {"city": "Kathmandu", "timestamp_utc": "2024-01-15T10:00:00Z", "temp": 21},  # Duplicate
        {"city": "Pokhara", "timestamp_utc": "2024-01-15T10:00:00Z", "temp": 22},
        {"city": "Kathmandu", "timestamp_utc": "2024-01-15T11:00:00Z", "temp": 23},  # Different time
    ]
    
    unique = deduplicate_by_key(records, ["city", "timestamp_utc"])
    
    assert len(unique) == 3  # Should have 3 unique records


def test_normalize_timezone():
    """Test timezone normalization."""
    timestamp = "2024-01-15T10:00:00+05:45"  # Nepal time
    normalized = normalize_timezone(timestamp, "UTC")
    
    assert normalized.endswith("+00:00") or "Z" in normalized

