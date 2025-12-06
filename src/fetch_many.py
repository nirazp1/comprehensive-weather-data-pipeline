"""Asynchronous fetch script for multiple weather stations using aiohttp."""
import asyncio
import aiohttp
import json
import time
from pathlib import Path
from typing import List, Optional
from .config import settings


async def fetch(
    session: aiohttp.ClientSession,
    city: str,
    api_key: Optional[str] = None
) -> Optional[dict]:
    """
    Fetch weather data for a city asynchronously.
    
    Args:
        session: aiohttp ClientSession
        city: City name to fetch weather for
        api_key: OpenWeatherMap API key (uses config if not provided)
        
    Returns:
        JSON response from API or None if error
    """
    api_key = api_key or settings.openweather_api_key
    
    if not api_key:
        print(f"✗ Error: OpenWeatherMap API key is required. "
              "Please set OPENWEATHER_API_KEY in your .env file. "
              "Get a free API key at: https://openweathermap.org/api")
        return None
    
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": city,
        "appid": api_key,
        "units": "metric"
    }
    
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=settings.request_timeout)) as response:
            response.raise_for_status()
            data = await response.json()
            return data
    except aiohttp.ClientError as e:
        print(f"✗ Error fetching {city}: {e}")
        return None
    except Exception as e:
        print(f"✗ Unexpected error for {city}: {e}")
        return None


async def save_raw_async(data: dict, city: str, output_dir: Optional[Path] = None) -> Path:
    """
    Save raw JSON response to file (async-safe).
    
    Args:
        data: JSON data to save
        city: City name (used in filename)
        output_dir: Output directory (uses config if not provided)
        
    Returns:
        Path to saved file
    """
    output_dir = output_dir or settings.raw_data_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = int(time.time())
    filename = f"{city.replace(' ', '_')}_{timestamp}.json"
    filepath = output_dir / filename
    
    # File I/O in async context - use asyncio.to_thread for Python 3.9+
    # For simplicity, we'll do sync I/O here (fine for small files)
    filepath.write_text(json.dumps(data, indent=2))
    print(f"✓ Saved {city}: {filepath.name}")
    return filepath


async def fetch_and_save(
    session: aiohttp.ClientSession,
    city: str,
    output_dir: Optional[Path] = None
):
    """Fetch and save weather data for a city."""
    data = await fetch(session, city)
    if data:
        await save_raw_async(data, city, output_dir)


async def main(cities: Optional[List[str]] = None, max_concurrent: int = 10):
    """
    Main async entry point for fetching multiple cities.
    
    Args:
        cities: List of city names to fetch
        max_concurrent: Maximum concurrent requests
    """
    if cities is None:
        cities = ["Kathmandu", "Pokhara", "Lalitpur", "Biratnagar", "Dharan"]
    
    # Create connector with connection limit
    connector = aiohttp.TCPConnector(limit=max_concurrent, limit_per_host=max_concurrent)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_and_save(session, city) for city in cities]
        await asyncio.gather(*tasks)
    
    print(f"\n✓ Completed fetching {len(cities)} cities")


if __name__ == "__main__":
    # Run with default cities
    asyncio.run(main())

