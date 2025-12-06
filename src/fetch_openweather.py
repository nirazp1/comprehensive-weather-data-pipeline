"""Basic PoC script to fetch weather data from OpenWeatherMap API (synchronous)."""
import requests
import json
import time
from pathlib import Path
from typing import Optional
from .config import settings


def fetch(city: str = "Kathmandu", api_key: Optional[str] = None) -> dict:
    """
    Fetch weather data for a city from OpenWeatherMap API.
    
    Args:
        city: City name to fetch weather for
        api_key: OpenWeatherMap API key (uses config if not provided)
        
    Returns:
        JSON response from API
        
    Raises:
        ValueError: If API key is not configured
        requests.HTTPError: If API request fails
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


def save_raw(data: dict, city: str, output_dir: Optional[Path] = None) -> Path:
    """
    Save raw JSON response to file.
    
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
    
    filepath.write_text(json.dumps(data, indent=2))
    print(f"✓ Saved raw data: {filepath}")
    return filepath


def main():
    """Main entry point for basic fetch script."""
    cities = ["Kathmandu", "Pokhara", "Lalitpur"]
    
    for city in cities:
        try:
            print(f"Fetching weather for {city}...")
            data = fetch(city)
            save_raw(data, city)
            time.sleep(1)  # Be respectful of rate limits
        except requests.HTTPError as e:
            print(f"✗ Error fetching {city}: {e}")
        except Exception as e:
            print(f"✗ Unexpected error for {city}: {e}")


if __name__ == "__main__":
    main()

