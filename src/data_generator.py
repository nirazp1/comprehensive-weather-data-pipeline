"""
Big Data Generator for Weather Data Pipeline
Optimized for generating BILLIONS of records with memory-efficient streaming.
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from pathlib import Path
import random
from typing import List, Dict, Any, Optional, Callable
from .config import settings
import json
import gc

# Optimize numpy random generation
np.random.seed(42)
random.seed(42)

def generate_weather_record_vectorized(
    cities_data: np.ndarray,
    timestamps: np.ndarray,
    base_temps: np.ndarray
) -> pd.DataFrame:
    """
    Vectorized generation of weather records for massive datasets.
    Much faster than row-by-row generation.
    
    Args:
        cities_data: Array of [city_name, lat, lon] for each city
        timestamps: Array of datetime objects
        base_temps: Array of base temperatures per city
        
    Returns:
        DataFrame with weather records
    """
    n_records = len(timestamps) * len(cities_data)
    
    # Expand arrays to match all combinations
    city_indices = np.repeat(np.arange(len(cities_data)), len(timestamps))
    timestamp_indices = np.tile(np.arange(len(timestamps)), len(cities_data))
    
    cities_expanded = cities_data[city_indices]
    timestamps_expanded = timestamps[timestamp_indices]
    
    # Extract day of year and hour for all timestamps
    days_of_year = np.array([ts.timetuple().tm_yday for ts in timestamps_expanded])
    hours = np.array([ts.hour for ts in timestamps_expanded])
    
    # Vectorized calculations
    seasonal_variation = 10 * np.sin(2 * np.pi * days_of_year / 365)
    daily_variation = 5 * np.sin(2 * np.pi * hours / 24)
    random_variation = np.random.normal(0, 3, n_records)
    temps = base_temps[city_indices] + seasonal_variation + daily_variation + random_variation
    
    # Generate other parameters
    humidity = np.clip(50 + np.random.normal(0, 15, n_records), 0, 100)
    pressure = 1013 + np.random.normal(0, 10, n_records)
    wind_speed = np.maximum(0, np.random.exponential(3, n_records))
    wind_direction = np.random.randint(0, 360, n_records)
    cloudiness = np.random.randint(0, 100, n_records)
    visibility = np.random.randint(5000, 10000, n_records)
    
    # Weather conditions
    weather_main = np.where(temps < 0, "Snow",
                   np.where(humidity > 80, "Rain",
                   np.where(humidity < 30, "Clear", "Clouds")))
    
    weather_desc = np.where(weather_main == "Snow", "light snow",
                   np.where(weather_main == "Rain", "light rain",
                   np.where(weather_main == "Clear", "clear sky", "scattered clouds")))
    
    rain = np.where(weather_main == "Rain", np.random.uniform(0, 5, n_records), 0)
    snow = np.where(weather_main == "Snow", np.random.uniform(0, 2, n_records), 0)
    
    # Build DataFrame
    df = pd.DataFrame({
        "timestamp_utc": [ts.isoformat() for ts in timestamps_expanded],
        "lat": cities_expanded[:, 1],
        "lon": cities_expanded[:, 2],
        "city": cities_expanded[:, 0],
        "country": "NP",
        "temp_C": np.round(temps, 2),
        "feels_like_C": np.round(temps - 2, 2),
        "temp_min_C": np.round(temps - 3, 2),
        "temp_max_C": np.round(temps + 3, 2),
        "pressure_hPa": np.round(pressure, 2),
        "humidity_pct": np.round(humidity, 1),
        "wind_speed_mps": np.round(wind_speed, 2),
        "wind_direction_deg": wind_direction,
        "wind_gust_mps": np.round(wind_speed * 1.5, 2),
        "cloudiness_pct": cloudiness,
        "visibility_m": visibility,
        "weather_main": weather_main,
        "weather_description": weather_desc,
        "rain_1h_mm": np.round(rain, 2),
        "snow_1h_mm": np.round(snow, 2),
        "source": "synthetic"
    })
    
    return df

def generate_cities_list(num_cities: int = 1000) -> np.ndarray:
    """
    Generate a list of American cities with coordinates (optimized for large numbers).
    Cincinnati is always included and marked as special.
    
    Args:
        num_cities: Number of cities to generate
        
    Returns:
        NumPy array of [city_name, lat, lon] tuples
    """
    # American cities - Cincinnati first with star marker
    base_cities = [
        ("Cincinnati", 39.1031, -84.5120),  # ⭐ Your city!
        ("New York", 40.7128, -74.0060),
        ("Los Angeles", 34.0522, -118.2437),
        ("Chicago", 41.8781, -87.6298),
        ("Houston", 29.7604, -95.3698),
        ("Phoenix", 33.4484, -112.0740),
        ("Philadelphia", 39.9526, -75.1652),
        ("San Antonio", 29.4241, -98.4936),
        ("San Diego", 32.7157, -117.1611),
        ("Dallas", 32.7767, -96.7970),
        ("San Jose", 37.3382, -121.8863),
        ("Austin", 30.2672, -97.7431),
        ("Jacksonville", 30.3322, -81.6557),
        ("Fort Worth", 32.7555, -97.3308),
        ("Columbus", 39.9612, -82.9988),
        ("Charlotte", 35.2271, -80.8431),
        ("San Francisco", 37.7749, -122.4194),
        ("Indianapolis", 39.7684, -86.1581),
        ("Seattle", 47.6062, -122.3321),
        ("Denver", 39.7392, -104.9903),
        ("Washington", 38.9072, -77.0369),
        ("Boston", 42.3601, -71.0589),
        ("El Paso", 31.7619, -106.4850),
        ("Nashville", 36.1627, -86.7816),
        ("Detroit", 42.3314, -83.0458),
        ("Oklahoma City", 35.4676, -97.5164),
        ("Portland", 45.5152, -122.6784),
        ("Las Vegas", 36.1699, -115.1398),
        ("Memphis", 35.1495, -90.0490),
        ("Louisville", 38.2527, -85.7585),
    ]
    
    cities_list = []
    
    # Use base cities and generate synthetic ones for larger datasets
    for i in range(num_cities):
        if i < len(base_cities):
            name, lat, lon = base_cities[i]
        else:
            # Generate synthetic American cities (US coordinates)
            name = f"City_{i+1:06d}"
            lat = 25.0 + random.uniform(0, 25)  # US latitude range: ~25°N to ~50°N
            lon = -125.0 + random.uniform(0, 55)  # US longitude range: ~125°W to ~70°W
        
        cities_list.append([name, round(lat, 4), round(lon, 4)])
    
    return np.array(cities_list, dtype=object)

def generate_big_data_streaming(
    num_cities: int,
    start_date: datetime,
    end_date: datetime,
    frequency: str = "hourly",
    batch_size: int = 100000,
    output_dir: Optional[Path] = None,
    progress_callback: Optional[Callable[[int, int], None]] = None
) -> tuple[int, List[Path]]:
    """
    Generate massive datasets using streaming/batch processing to avoid memory issues.
    Can generate billions of records efficiently.
    
    Args:
        num_cities: Number of cities
        start_date: Start timestamp
        end_date: End timestamp
        frequency: 'hourly' or 'daily'
        batch_size: Records per batch (for memory efficiency)
        output_dir: Directory to save files
        progress_callback: Function(callback(current, total)) for progress updates
        
    Returns:
        Tuple of (total_records, list_of_saved_files)
    """
    # Calculate time delta
    if frequency == "hourly":
        delta = timedelta(hours=1)
        records_per_city = int((end_date - start_date).total_seconds() / 3600) + 1
    else:
        delta = timedelta(days=1)
        records_per_city = (end_date - start_date).days + 1
    
    total_expected_records = num_cities * records_per_city
    
    # Generate cities
    cities_data = generate_cities_list(num_cities)
    base_temps = np.random.uniform(15, 25, num_cities)  # Different base temp per city
    
    # Generate all timestamps once
    timestamps = []
    current = start_date
    while current <= end_date:
        timestamps.append(current)
        current += delta
    
    timestamps = np.array(timestamps)
    
    # Setup output
    if output_dir:
        output_dir.mkdir(parents=True, exist_ok=True)
        saved_files = []
    else:
        saved_files = []
    
    # Process in batches to avoid memory issues
    total_generated = 0
    batch_num = 0
    
    # Process cities in batches
    cities_per_batch = max(1, batch_size // records_per_city)
    
    for city_start in range(0, num_cities, cities_per_batch):
        city_end = min(city_start + cities_per_batch, num_cities)
        cities_batch = cities_data[city_start:city_end]
        temps_batch = base_temps[city_start:city_end]
        
        # Generate batch
        batch_df = generate_weather_record_vectorized(
            cities_batch,
            timestamps,
            temps_batch
        )
        
        total_generated += len(batch_df)
        
        # Save batch if output directory provided
        if output_dir:
            if frequency == "hourly":
                # Partition by date
                batch_df['timestamp_utc'] = pd.to_datetime(batch_df['timestamp_utc'])
                batch_df['date'] = batch_df['timestamp_utc'].dt.date
                
                for date, group in batch_df.groupby('date'):
                    date_str = date.strftime('%Y%m%d')
                    filename = f"weather_{date_str}_batch_{batch_num:06d}.parquet"
                    filepath = output_dir / filename
                    group.drop(columns=['date'], errors='ignore').to_parquet(
                        filepath, index=False, engine='pyarrow', compression='snappy'
                    )
                    if filepath not in saved_files:
                        saved_files.append(filepath)
            else:
                # Save as single batch file
                filename = f"weather_batch_{batch_num:06d}.parquet"
                filepath = output_dir / filename
                batch_df.to_parquet(
                    filepath, index=False, engine='pyarrow', compression='snappy'
                )
                saved_files.append(filepath)
        
        batch_num += 1
        
        # Progress callback
        if progress_callback:
            progress_callback(total_generated, total_expected_records)
        
        # Memory cleanup
        del batch_df
        gc.collect()
    
    return total_generated, saved_files

def generate_millions_of_records(
    num_cities: int = 1000,
    days: int = 365,
    frequency: str = "hourly",
    output_dir: Optional[Path] = None,
    progress_callback: Optional[Callable[[int, int], None]] = None
) -> tuple[int, List[Path]]:
    """
    Generate millions/billions of weather records for big data demonstration.
    Uses streaming to handle massive datasets efficiently.
    
    Args:
        num_cities: Number of cities
        days: Number of days of data
        frequency: 'hourly' or 'daily'
        output_dir: Directory to save files (if None, returns data in memory - use carefully!)
        progress_callback: Function(current, total) for progress updates
        
    Returns:
        Tuple of (total_records, list_of_saved_files)
    """
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=days)
    
    if frequency == "hourly":
        expected_records = num_cities * days * 24
    else:
        expected_records = num_cities * days
    
    print(f"Generating data for {num_cities:,} cities over {days} days...")
    print(f"Expected records: {expected_records:,} ({expected_records/1_000_000:.2f} million)")
    
    # Use streaming for large datasets
    if expected_records > 1_000_000 or output_dir:
        return generate_big_data_streaming(
            num_cities=num_cities,
            start_date=start_date,
            end_date=end_date,
            frequency=frequency,
            batch_size=100000,  # Process 100k records at a time
            output_dir=output_dir,
            progress_callback=progress_callback
        )
    else:
        # For smaller datasets, use in-memory generation
        cities_data = generate_cities_list(num_cities)
        base_temps = np.random.uniform(15, 25, num_cities)
        
        if frequency == "hourly":
            delta = timedelta(hours=1)
        else:
            delta = timedelta(days=1)
        
        timestamps = []
        current = start_date
        while current <= end_date:
            timestamps.append(current)
            current += delta
        
        timestamps = np.array(timestamps)
        
        df = generate_weather_record_vectorized(cities_data, timestamps, base_temps)
        
        saved_files = []
        if output_dir:
            output_dir.mkdir(parents=True, exist_ok=True)
            if frequency == "hourly":
                df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
                df['date'] = df['timestamp_utc'].dt.date
                for date, group in df.groupby('date'):
                    date_str = date.strftime('%Y%m%d')
                    filename = f"weather_{date_str}.parquet"
                    filepath = output_dir / filename
                    group.drop(columns=['date'], errors='ignore').to_parquet(
                        filepath, index=False, engine='pyarrow', compression='snappy'
                    )
                    saved_files.append(filepath)
            else:
                filename = "weather_data.parquet"
                filepath = output_dir / filename
                df.to_parquet(filepath, index=False, engine='pyarrow', compression='snappy')
                saved_files.append(filepath)
        
        return len(df), saved_files

def get_big_data_presets() -> Dict[str, Dict[str, Any]]:
    """
    Get preset configurations for common big data scenarios.
    
    Returns:
        Dictionary of preset configurations
    """
    return {
        "Small (1M records)": {
            "num_cities": 100,
            "days": 365,
            "frequency": "hourly",
            "description": "~876,000 records - Good for testing"
        },
        "Medium (10M records)": {
            "num_cities": 1000,
            "days": 365,
            "frequency": "hourly",
            "description": "~8.76 million records - Medium scale"
        },
        "Large (100M records)": {
            "num_cities": 5000,
            "days": 730,
            "frequency": "hourly",
            "description": "~87.6 million records - Large scale"
        },
        "Very Large (500M records)": {
            "num_cities": 10000,
            "days": 1825,
            "frequency": "hourly",
            "description": "~438 million records - Very large scale"
        },
        "Massive (1B+ records)": {
            "num_cities": 20000,
            "days": 1825,
            "frequency": "hourly",
            "description": "~876 million records - Massive scale"
        },
        "Ultra Massive (5B+ records)": {
            "num_cities": 50000,
            "days": 3650,
            "frequency": "hourly",
            "description": "~4.38 billion records - Ultra massive (may take hours)"
        },
        "5 Years Historical (Recommended)": {
            "num_cities": 30,
            "days": 1825,  # 5 years
            "frequency": "hourly",
            "description": "~1.3 million records - 5 years of hourly data for 30 American cities (perfect for statistics!)"
        },
        "5 Years All Cities": {
            "num_cities": 30,
            "days": 1825,  # 5 years
            "frequency": "daily",
            "description": "~54,750 records - 5 years of daily data for 30 cities (faster generation)"
        }
    }

if __name__ == "__main__":
    # Example: Generate 10 million records
    print("Generating big data for demonstration...")
    output_dir = settings.processed_data_dir / "big_data"
    total_records, saved_files = generate_millions_of_records(
        num_cities=1000,
        days=365,
        frequency="hourly",
        output_dir=output_dir
    )
    print(f"\nGenerated {total_records:,} records")
    print(f"Saved to {len(saved_files)} Parquet files")
