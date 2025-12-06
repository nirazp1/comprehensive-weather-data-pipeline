"""Simple end-to-end pipeline: fetch → normalize → save to Parquet."""
import requests
import pandas as pd
import time
from pathlib import Path
from typing import Optional
from datetime import datetime
from .config import settings
from .normalize import normalize_openweather
from .utils import validate_weather_record, logger


def fetch(city: str, api_key: Optional[str] = None) -> dict:
    """
    Fetch weather data for a city from OpenWeatherMap API.
    
    Args:
        city: City name to fetch weather for
        api_key: OpenWeatherMap API key (uses config if not provided)
        
    Returns:
        JSON response from API
        
    Raises:
        ValueError: If API key is not configured
    """
    api_key = api_key or settings.openweather_api_key
    
    if not api_key:
        raise ValueError(
            "OpenWeatherMap API key is required. "
            "Please set OPENWEATHER_API_KEY in your .env file or pass it as an argument. "
            "Get a free API key at: https://openweathermap.org/api"
        )
    
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": city,
        "appid": api_key,
        "units": "metric"
    }
    
    response = requests.get(url, params=params, timeout=settings.request_timeout)
    response.raise_for_status()
    return response.json()


def save_to_parquet(record: dict, city: str, output_dir: Optional[Path] = None) -> Path:
    """
    Save normalized record to Parquet file (daily partitioning).
    
    Args:
        record: Normalized weather record
        city: City name (used in filename)
        output_dir: Output directory (uses config if not provided)
        
    Returns:
        Path to saved Parquet file
    """
    output_dir = output_dir or settings.processed_data_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Create daily partitioned filename
    date_str = time.strftime("%Y%m%d")
    filename = f"weather_{city.replace(' ', '_')}_{date_str}.parquet"
    filepath = output_dir / filename
    
    # Convert record to DataFrame
    df_new = pd.DataFrame([record])
    
    # Append to existing file if it exists
    if filepath.exists():
        try:
            df_existing = pd.read_parquet(filepath)
            df = pd.concat([df_existing, df_new], ignore_index=True)
            # Remove duplicates based on timestamp and city
            df = df.drop_duplicates(subset=["timestamp_utc", "city"], keep="last")
            df = df.sort_values("timestamp_utc")
        except Exception as e:
            logger.warning(f"Error reading existing Parquet file, creating new: {e}")
            df = df_new
    else:
        df = df_new
    
    # Save to Parquet
    df.to_parquet(filepath, index=False, engine="pyarrow")
    logger.info(f"✓ Wrote {len(df)} records to {filepath}")
    return filepath


def run(city: str = "Kathmandu", validate: bool = True) -> Optional[Path]:
    """
    Run end-to-end pipeline for a single city.
    
    Args:
        city: City name to process
        validate: Whether to validate the normalized record
        
    Returns:
        Path to saved Parquet file or None if error
    """
    try:
        # Step 1: Fetch raw data
        logger.info(f"Fetching weather for {city}...")
        raw_data = fetch(city)
        
        # Step 2: Normalize
        logger.info(f"Normalizing data for {city}...")
        normalized = normalize_openweather(raw_data)
        
        # Step 3: Validate (optional)
        if validate:
            is_valid, error_msg = validate_weather_record(normalized)
            if not is_valid:
                logger.error(f"Validation failed for {city}: {error_msg}")
                return None
        
        # Step 4: Save to Parquet
        logger.info(f"Saving processed data for {city}...")
        filepath = save_to_parquet(normalized, city)
        
        return filepath
        
    except requests.HTTPError as e:
        logger.error(f"HTTP error fetching {city}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error processing {city}: {e}")
        return None


def main():
    """Main entry point for end-to-end pipeline."""
    cities = ["Kathmandu", "Pokhara", "Lalitpur"]
    
    results = []
    for city in cities:
        result = run(city)
        if result:
            results.append(result)
        time.sleep(1)  # Rate limiting
    
    print(f"\n✓ Processed {len(results)}/{len(cities)} cities successfully")
    if results:
        print(f"✓ Data saved to: {results[0].parent}")


if __name__ == "__main__":
    main()

