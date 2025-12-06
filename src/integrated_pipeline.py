"""
Integrated Pipeline for Scenario #8
Combines all data sources: weather stations, satellites, radar, and buoys.
Distributes data to forecasting models and external channels.
"""
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
import asyncio

from .config import settings
from .fetch_openweather import fetch as fetch_weather_station
from .fetch_satellite import fetch_satellite_imagery, fetch_satellite_weather_data
from .fetch_radar import fetch_radar_data, fetch_radar_for_location
from .fetch_buoy import fetch_buoy_data, fetch_buoys_in_region
from .normalize import get_normalizer
# Storage will be handled directly with pandas
from .distribution import WeatherDataDistributor
from .predictions import train_and_predict
from .utils import logger, validate_weather_record
import pandas as pd


class IntegratedWeatherPipeline:
    """
    Complete pipeline for Scenario #8:
    - Collects from multiple sources (stations, satellites, radar, buoys)
    - Normalizes and validates data
    - Stores in big data format (Parquet)
    - Feeds forecasting models
    - Distributes to external channels
    """
    
    def __init__(self):
        """Initialize integrated pipeline."""
        self.distributor = WeatherDataDistributor()
        self.normalized_data = []
    
    def collect_all_sources(
        self,
        city: str = "Cincinnati",
        lat: float = 39.1031,
        lon: float = -84.5120,
        region: str = "us"
    ) -> List[Dict[str, Any]]:
        """
        Collect data from all sources: stations, satellites, radar, buoys.
        
        Args:
            city: City name
            lat: Latitude
            lon: Longitude
            region: Geographic region
            
        Returns:
            List of raw data from all sources
        """
        logger.info(f"Collecting data from all sources for {city}")
        all_data = []
        
        # 1. Weather Station Data
        try:
            logger.info("Fetching weather station data...")
            station_data = fetch_weather_station(city)
            station_data["_source_type"] = "weather_station"
            all_data.append(station_data)
            logger.info("✓ Weather station data collected")
        except Exception as e:
            logger.warning(f"✗ Weather station data failed: {e}")
        
        # 2. Satellite Data
        try:
            logger.info("Fetching satellite data...")
            satellite_data = fetch_satellite_imagery(region=region, image_type="visible")
            satellite_data["_source_type"] = "satellite"
            all_data.append(satellite_data)
            
            satellite_weather = fetch_satellite_weather_data(lat=lat, lon=lon)
            satellite_weather["_source_type"] = "satellite_weather"
            all_data.append(satellite_weather)
            logger.info("✓ Satellite data collected")
        except Exception as e:
            logger.warning(f"✗ Satellite data failed: {e}")
        
        # 3. Radar Data
        try:
            logger.info("Fetching radar data...")
            radar_data = fetch_radar_for_location(lat=lat, lon=lon)
            radar_data["_source_type"] = "radar"
            all_data.append(radar_data)
            logger.info("✓ Radar data collected")
        except Exception as e:
            logger.warning(f"✗ Radar data failed: {e}")
        
        # 4. Buoy Data (if near coast)
        try:
            logger.info("Fetching buoy data...")
            # Example: Fetch buoys in region
            buoys = fetch_buoys_in_region(
                lat_min=lat - 5.0,
                lat_max=lat + 5.0,
                lon_min=lon - 5.0,
                lon_max=lon + 5.0
            )
            for buoy in buoys:
                buoy["_source_type"] = "buoy"
                all_data.append(buoy)
            logger.info(f"✓ Buoy data collected ({len(buoys)} buoys)")
        except Exception as e:
            logger.warning(f"✗ Buoy data failed: {e}")
        
        logger.info(f"✓ Collected data from {len(all_data)} sources")
        return all_data
    
    def normalize_all_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Normalize all data from different sources to canonical schema.
        
        Args:
            raw_data: List of raw data dictionaries
            
        Returns:
            List of normalized data
        """
        logger.info("Normalizing data from all sources...")
        normalized = []
        
        for data in raw_data:
            source_type = data.get("_source_type", "unknown")
            
            # Map source types to normalizers
            source_map = {
                "weather_station": "openweather",
                "satellite": "satellite",
                "satellite_weather": "satellite",
                "radar": "radar",
                "buoy": "buoy"
            }
            
            normalizer_name = source_map.get(source_type, "openweather")
            
            try:
                normalizer = get_normalizer(normalizer_name)
                normalized_data = normalizer(data)
                
                # Validate
                is_valid, error = validate_weather_record(normalized_data)
                if is_valid:
                    normalized.append(normalized_data)
                else:
                    logger.warning(f"Validation failed for {source_type}: {error}")
            except Exception as e:
                logger.warning(f"Normalization failed for {source_type}: {e}")
        
        logger.info(f"✓ Normalized {len(normalized)} records")
        self.normalized_data = normalized
        return normalized
    
    def store_to_big_data(self, normalized_data: List[Dict[str, Any]]) -> Path:
        """
        Store normalized data to big data storage (Parquet).
        
        Args:
            normalized_data: List of normalized data
            
        Returns:
            Path to saved file
        """
        logger.info("Storing to big data format (Parquet)...")
        
        if not normalized_data:
            logger.warning("No data to store")
            return None
        
        df = pd.DataFrame(normalized_data)
        
        # Save to Parquet
        output_dir = settings.processed_data_dir / "integrated"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filepath = output_dir / f"weather_integrated_{timestamp}.parquet"
        
        df.to_parquet(filepath, index=False, engine="pyarrow", compression="snappy")
        logger.info(f"✓ Stored {len(df)} records to {filepath}")
        
        return filepath
    
    def feed_forecasting_models(self, city: str = "Cincinnati") -> Dict[str, Any]:
        """
        Feed data to forecasting models and generate predictions.
        
        Args:
            city: City name
            
        Returns:
            Dictionary with forecast results
        """
        logger.info(f"Feeding data to forecasting models for {city}...")
        
        data_dir = settings.processed_data_dir / "5year_historical"
        
        if not data_dir.exists():
            logger.warning("No historical data for model training")
            return None
        
        try:
            result = train_and_predict(
                city=city,
                data_dir=data_dir,
                model_type="random_forest",
                hours_ahead=24
            )
            
            logger.info(f"✓ Forecast generated: R²={result['metrics']['test_r2']:.3f}")
            return result
            
        except Exception as e:
            logger.error(f"✗ Forecasting failed: {e}")
            return None
    
    def distribute_to_channels(
        self,
        forecast: Dict[str, Any],
        channels: List[str] = ["kafka"]
    ) -> Dict[str, bool]:
        """
        Distribute forecasts to external channels.
        
        Args:
            forecast: Forecast data
            channels: List of channels to use
            
        Returns:
            Dictionary with distribution results
        """
        logger.info(f"Distributing to channels: {channels}")
        
        results = self.distributor.distribute_forecast(forecast, channels=channels)
        
        logger.info(f"✓ Distribution results: {results}")
        return results
    
    def run_complete_pipeline(
        self,
        city: str = "Cincinnati",
        lat: float = 39.1031,
        lon: float = -84.5120,
        distribute: bool = True
    ) -> Dict[str, Any]:
        """
        Run the complete Scenario #8 pipeline.
        
        Args:
            city: City name
            lat: Latitude
            lon: Longitude
            distribute: Whether to distribute to external channels
            
        Returns:
            Dictionary with pipeline results
        """
        logger.info("="*70)
        logger.info("RUNNING COMPLETE SCENARIO #8 PIPELINE")
        logger.info("="*70)
        
        results = {
            "city": city,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "sources_collected": 0,
            "records_normalized": 0,
            "storage_path": None,
            "forecast": None,
            "distribution": None
        }
        
        # Step 1: Collect from all sources
        raw_data = self.collect_all_sources(city, lat, lon)
        results["sources_collected"] = len(raw_data)
        
        # Step 2: Normalize
        normalized_data = self.normalize_all_data(raw_data)
        results["records_normalized"] = len(normalized_data)
        
        # Step 3: Store to big data
        storage_path = self.store_to_big_data(normalized_data)
        results["storage_path"] = str(storage_path) if storage_path else None
        
        # Step 4: Feed forecasting models
        forecast = self.feed_forecasting_models(city)
        results["forecast"] = forecast
        
        # Step 5: Distribute to channels
        if distribute and forecast:
            distribution = self.distribute_to_channels(forecast, channels=["kafka"])
            results["distribution"] = distribution
        
        logger.info("="*70)
        logger.info("PIPELINE COMPLETE")
        logger.info("="*70)
        
        return results


if __name__ == "__main__":
    # Example: Run complete pipeline for Cincinnati
    pipeline = IntegratedWeatherPipeline()
    results = pipeline.run_complete_pipeline(
        city="Cincinnati",
        lat=39.1031,
        lon=-84.5120,
        distribute=True
    )
    
    print("\nPipeline Results:")
    print(f"  Sources collected: {results['sources_collected']}")
    print(f"  Records normalized: {results['records_normalized']}")
    print(f"  Storage: {results['storage_path']}")
    print(f"  Forecast: {'Generated' if results['forecast'] else 'Failed'}")
    print(f"  Distribution: {results['distribution']}")

