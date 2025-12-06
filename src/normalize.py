"""Normalization functions to convert various weather API responses to a canonical schema."""
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import json


def normalize_openweather(json_doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize OpenWeatherMap API response to canonical schema.
    
    Args:
        json_doc: Raw JSON response from OpenWeatherMap API
        
    Returns:
        Normalized dictionary with canonical schema
    """
    main = json_doc.get("main", {})
    wind = json_doc.get("wind", {})
    coord = json_doc.get("coord", {})
    weather = json_doc.get("weather", [{}])[0] if json_doc.get("weather") else {}
    
    # Handle timestamp - OpenWeatherMap uses Unix timestamp
    dt = json_doc.get("dt", 0)
    if dt:
        timestamp_utc = datetime.fromtimestamp(dt, tz=timezone.utc).isoformat()
    else:
        timestamp_utc = datetime.now(timezone.utc).isoformat()
    
    out = {
        "timestamp_utc": timestamp_utc,
        "lat": coord.get("lat"),
        "lon": coord.get("lon"),
        "city": json_doc.get("name"),
        "country": json_doc.get("sys", {}).get("country"),
        "temp_C": main.get("temp"),
        "feels_like_C": main.get("feels_like"),
        "temp_min_C": main.get("temp_min"),
        "temp_max_C": main.get("temp_max"),
        "pressure_hPa": main.get("pressure"),
        "humidity_pct": main.get("humidity"),
        "wind_speed_mps": wind.get("speed"),
        "wind_direction_deg": wind.get("deg"),
        "wind_gust_mps": wind.get("gust"),
        "cloudiness_pct": json_doc.get("clouds", {}).get("all"),
        "visibility_m": json_doc.get("visibility"),
        "weather_main": weather.get("main"),
        "weather_description": weather.get("description"),
        "rain_1h_mm": json_doc.get("rain", {}).get("1h", 0),
        "snow_1h_mm": json_doc.get("snow", {}).get("1h", 0),
        "source": "openweather",
        "raw": json_doc
    }
    return out


def normalize_noaa(json_doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize NOAA API response to canonical schema.
    
    Args:
        json_doc: Raw JSON response from NOAA API
        
    Returns:
        Normalized dictionary with canonical schema
    """
    # NOAA API structure varies by endpoint
    # This is a template - adjust based on actual NOAA endpoint structure
    properties = json_doc.get("properties", {})
    geometry = json_doc.get("geometry", {})
    coordinates = geometry.get("coordinates", [])
    
    timestamp_utc = datetime.now(timezone.utc).isoformat()
    if "timestamp" in properties:
        try:
            timestamp_utc = datetime.fromisoformat(
                properties["timestamp"].replace("Z", "+00:00")
            ).isoformat()
        except:
            pass
    
    out = {
        "timestamp_utc": timestamp_utc,
        "lat": coordinates[1] if len(coordinates) >= 2 else None,
        "lon": coordinates[0] if len(coordinates) >= 2 else None,
        "temp_C": properties.get("temperature", {}).get("value"),
        "pressure_hPa": properties.get("barometricPressure", {}).get("value"),
        "humidity_pct": properties.get("relativeHumidity", {}).get("value"),
        "wind_speed_mps": properties.get("windSpeed", {}).get("value"),
        "wind_direction_deg": properties.get("windDirection", {}).get("value"),
        "source": "noaa",
        "raw": json_doc
    }
    return out


def normalize_meteostat(json_doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize Meteostat API response to canonical schema.
    
    Args:
        json_doc: Raw JSON response from Meteostat API
        
    Returns:
        Normalized dictionary with canonical schema
    """
    data = json_doc.get("data", [])
    if not data:
        data = [json_doc]
    
    # Meteostat returns arrays, take first record
    record = data[0] if isinstance(data, list) else json_doc
    
    # Parse timestamp - Meteostat uses ISO format or Unix timestamp
    time_str = record.get("time") or record.get("timestamp")
    if time_str:
        try:
            if isinstance(time_str, (int, float)):
                timestamp_utc = datetime.fromtimestamp(time_str, tz=timezone.utc).isoformat()
            else:
                timestamp_utc = datetime.fromisoformat(
                    str(time_str).replace("Z", "+00:00")
                ).isoformat()
        except:
            timestamp_utc = datetime.now(timezone.utc).isoformat()
    else:
        timestamp_utc = datetime.now(timezone.utc).isoformat()
    
    out = {
        "timestamp_utc": timestamp_utc,
        "lat": record.get("lat"),
        "lon": record.get("lon"),
        "temp_C": record.get("temp"),
        "pressure_hPa": record.get("pres"),
        "humidity_pct": record.get("rhum"),
        "wind_speed_mps": record.get("wspd"),
        "wind_direction_deg": record.get("wdir"),
        "precipitation_mm": record.get("prcp"),
        "source": "meteostat",
        "raw": record
    }
    return out


def normalize_satellite(data: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize satellite data to canonical schema."""
    return {
        "timestamp_utc": data.get("timestamp_utc", datetime.now(timezone.utc).isoformat()),
        "source": "satellite",
        "lat": data.get("coordinates", {}).get("lat") if isinstance(data.get("coordinates"), dict) else None,
        "lon": data.get("coordinates", {}).get("lon") if isinstance(data.get("coordinates"), dict) else None,
        "satellite_type": data.get("satellite_type"),
        "image_type": data.get("image_type"),
        "cloud_cover_pct": data.get("cloud_cover_pct"),
        "raw": data
    }


def normalize_radar(data: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize radar data to canonical schema."""
    data_dict = data.get("data", {}) if isinstance(data.get("data"), dict) else {}
    coords = data.get("coordinates", {}) if isinstance(data.get("coordinates"), dict) else {}
    return {
        "timestamp_utc": data.get("timestamp_utc", datetime.now(timezone.utc).isoformat()),
        "source": "radar",
        "lat": coords.get("lat"),
        "lon": coords.get("lon"),
        "station_id": data.get("station_id"),
        "precipitation_detected": data_dict.get("precipitation_detected", False),
        "reflectivity_dbz": data_dict.get("reflectivity_max_dbz"),
        "raw": data
    }


def normalize_buoy(data: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize ocean buoy data to canonical schema."""
    water_data = data.get("water_data", {}) if isinstance(data.get("water_data"), dict) else {}
    atmospheric_data = data.get("atmospheric_data", {}) if isinstance(data.get("atmospheric_data"), dict) else {}
    coords = data.get("coordinates", {}) if isinstance(data.get("coordinates"), dict) else {}
    return {
        "timestamp_utc": data.get("timestamp_utc", datetime.now(timezone.utc).isoformat()),
        "source": "buoy",
        "lat": coords.get("lat"),
        "lon": coords.get("lon"),
        "buoy_id": data.get("buoy_id"),
        "temp_C": atmospheric_data.get("air_temperature_C"),
        "water_temp_C": water_data.get("water_temperature_C"),
        "wind_speed_mps": atmospheric_data.get("wind_speed_mps"),
        "wind_direction_deg": atmospheric_data.get("wind_direction_deg"),
        "pressure_hPa": atmospheric_data.get("pressure_hPa"),
        "humidity_pct": atmospheric_data.get("humidity_pct"),
        "wave_height_m": water_data.get("wave_height_m"),
        "raw": data
    }


def get_normalizer(source: str):
    """Get the appropriate normalizer function for a data source."""
    normalizers = {
        "openweather": normalize_openweather,
        "noaa": normalize_noaa,
        "meteostat": normalize_meteostat,
        "satellite": normalize_satellite,
        "radar": normalize_radar,
        "buoy": normalize_buoy,
    }
    return normalizers.get(source.lower(), normalize_openweather)

