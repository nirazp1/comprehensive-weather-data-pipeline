"""Utility functions for retries, error handling, and data validation."""
from typing import Callable, Any, Optional, Dict
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    Retrying
)
import requests
import aiohttp
from datetime import datetime, timezone
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def with_retry(
    func: Callable,
    max_attempts: int = 3,
    backoff_factor: float = 2.0,
    exceptions: tuple = (requests.HTTPError, requests.ConnectionError, requests.Timeout)
):
    """
    Decorator to add retry logic with exponential backoff to a function.
    
    Args:
        func: Function to wrap
        max_attempts: Maximum number of retry attempts
        backoff_factor: Exponential backoff multiplier
        exceptions: Tuple of exceptions to retry on
    """
    @retry(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=backoff_factor),
        retry=retry_if_exception_type(exceptions),
        reraise=True
    )
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    
    return wrapper


def validate_weather_record(record: Dict[str, Any]) -> tuple[bool, Optional[str]]:
    """
    Validate a normalized weather record against schema constraints.
    
    Args:
        record: Normalized weather record
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    # Check required fields
    required_fields = ["timestamp_utc", "source"]
    for field in required_fields:
        if field not in record:
            return False, f"Missing required field: {field}"
    
    # Validate timestamp (not in future, not too old)
    try:
        ts = datetime.fromisoformat(record["timestamp_utc"].replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        if ts > now:
            return False, f"Timestamp in future: {ts}"
        # Allow up to 7 days old (adjust as needed)
        if (now - ts).days > 7:
            return False, f"Timestamp too old: {ts}"
    except Exception as e:
        return False, f"Invalid timestamp format: {e}"
    
    # Validate numeric ranges
    if "temp_C" in record and record["temp_C"] is not None:
        temp = record["temp_C"]
        if not (-100 <= temp <= 70):
            return False, f"Temperature out of range: {temp}°C"
    
    if "humidity_pct" in record and record["humidity_pct"] is not None:
        humidity = record["humidity_pct"]
        if not (0 <= humidity <= 100):
            return False, f"Humidity out of range: {humidity}%"
    
    if "pressure_hPa" in record and record["pressure_hPa"] is not None:
        pressure = record["pressure_hPa"]
        if not (800 <= pressure <= 1100):
            return False, f"Pressure out of range: {pressure} hPa"
    
    return True, None


def deduplicate_by_key(records: list, key_fields: list) -> list:
    """
    Remove duplicate records based on key fields.
    
    Args:
        records: List of record dictionaries
        key_fields: List of field names to use as composite key
        
    Returns:
        Deduplicated list of records
    """
    seen = set()
    unique_records = []
    
    for record in records:
        # Create composite key from specified fields
        key = tuple(record.get(field) for field in key_fields)
        if key not in seen:
            seen.add(key)
            unique_records.append(record)
    
    return unique_records


def normalize_timezone(timestamp_str: str, target_tz: str = "UTC") -> str:
    """
    Normalize timestamp to target timezone (default UTC).
    
    Args:
        timestamp_str: ISO format timestamp string
        target_tz: Target timezone (default UTC)
        
    Returns:
        Normalized timestamp string
    """
    try:
        dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        if target_tz == "UTC":
            dt = dt.astimezone(timezone.utc)
        return dt.isoformat()
    except Exception as e:
        logger.warning(f"Failed to normalize timestamp {timestamp_str}: {e}")
        return timestamp_str

