"""
Data Distribution Module
Distributes weather data and forecasts to news agencies, apps, and websites.
"""
import json
from pathlib import Path
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime, timezone
import requests
from confluent_kafka import Producer
import logging

from .config import settings
from .utils import logger

logger = logging.getLogger(__name__)


class WeatherDataDistributor:
    """
    Distributes weather data to various channels:
    - News agencies (REST API)
    - Weather apps (WebSocket, REST API)
    - Weather websites (REST API, webhooks)
    """
    
    def __init__(self):
        """Initialize distributor."""
        self.kafka_producer = None
        if settings.kafka_bootstrap_servers:
            try:
                self.kafka_producer = Producer({
                    "bootstrap.servers": settings.kafka_bootstrap_servers
                })
            except Exception as e:
                logger.warning(f"Kafka producer not available: {e}")
    
    def distribute_to_news_agency(
        self,
        data: Dict[str, Any],
        agency_endpoint: str,
        api_key: Optional[str] = None
    ) -> bool:
        """
        Distribute weather data to a news agency via REST API.
        
        Args:
            data: Weather data to distribute
            agency_endpoint: News agency API endpoint
            api_key: API key for authentication
            
        Returns:
            True if successful
        """
        try:
            headers = {
                "Content-Type": "application/json",
                "User-Agent": "WeatherPipeline/1.0"
            }
            
            if api_key:
                headers["Authorization"] = f"Bearer {api_key}"
            
            payload = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "data": data,
                "source": "weather_ingestion_pipeline"
            }
            
            response = requests.post(
                agency_endpoint,
                json=payload,
                headers=headers,
                timeout=10
            )
            response.raise_for_status()
            
            logger.info(f"✓ Distributed to news agency: {agency_endpoint}")
            return True
            
        except Exception as e:
            logger.error(f"✗ Failed to distribute to news agency: {e}")
            return False
    
    def distribute_to_weather_app(
        self,
        data: Dict[str, Any],
        app_endpoint: str,
        app_id: Optional[str] = None
    ) -> bool:
        """
        Distribute weather data to a weather app.
        
        Args:
            data: Weather data
            app_endpoint: App API endpoint
            app_id: App identifier
            
        Returns:
            True if successful
        """
        try:
            headers = {"Content-Type": "application/json"}
            
            if app_id:
                headers["X-App-ID"] = app_id
            
            payload = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "weather": data,
                "format": "standard"
            }
            
            response = requests.post(
                app_endpoint,
                json=payload,
                headers=headers,
                timeout=10
            )
            response.raise_for_status()
            
            logger.info(f"✓ Distributed to weather app: {app_endpoint}")
            return True
            
        except Exception as e:
            logger.error(f"✗ Failed to distribute to weather app: {e}")
            return False
    
    def distribute_to_website(
        self,
        data: Dict[str, Any],
        webhook_url: str,
        secret: Optional[str] = None
    ) -> bool:
        """
        Distribute weather data to a website via webhook.
        
        Args:
            data: Weather data
            webhook_url: Webhook URL
            secret: Webhook secret for authentication
            
        Returns:
            True if successful
        """
        try:
            headers = {"Content-Type": "application/json"}
            
            if secret:
                headers["X-Webhook-Secret"] = secret
            
            payload = {
                "event": "weather_update",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "data": data
            }
            
            response = requests.post(
                webhook_url,
                json=payload,
                headers=headers,
                timeout=10
            )
            response.raise_for_status()
            
            logger.info(f"✓ Distributed to website: {webhook_url}")
            return True
            
        except Exception as e:
            logger.error(f"✗ Failed to distribute to website: {e}")
            return False
    
    def distribute_via_kafka(
        self,
        data: Dict[str, Any],
        topic: str = "weather-distribution"
    ) -> bool:
        """
        Distribute weather data via Kafka topic.
        Multiple consumers (apps, websites, agencies) can subscribe.
        
        Args:
            data: Weather data
            topic: Kafka topic name
            
        Returns:
            True if successful
        """
        if not self.kafka_producer:
            logger.warning("Kafka producer not available")
            return False
        
        try:
            payload = json.dumps(data).encode("utf-8")
            
            self.kafka_producer.produce(
                topic,
                value=payload,
                callback=lambda err, msg: logger.info(f"✓ Distributed via Kafka: {topic}") if not err else logger.error(f"✗ Kafka error: {err}")
            )
            
            self.kafka_producer.flush(timeout=5)
            return True
            
        except Exception as e:
            logger.error(f"✗ Failed to distribute via Kafka: {e}")
            return False
    
    def distribute_forecast(
        self,
        forecast: Dict[str, Any],
        channels: List[str] = ["kafka"]
    ) -> Dict[str, bool]:
        """
        Distribute weather forecast to multiple channels.
        
        Args:
            forecast: Forecast data
            channels: List of channels ("kafka", "api", "webhook")
            
        Returns:
            Dictionary with channel -> success status
        """
        results = {}
        
        # Distribute via Kafka
        if "kafka" in channels:
            results["kafka"] = self.distribute_via_kafka(forecast, topic="weather-forecasts")
        
        # Distribute via API (example endpoints)
        if "api" in channels:
            # Example: Distribute to news agency
            news_endpoint = getattr(settings, "news_agency_endpoint", None)
            if news_endpoint:
                results["news_agency"] = self.distribute_to_news_agency(forecast, news_endpoint)
        
        # Distribute via webhook
        if "webhook" in channels:
            webhook_url = getattr(settings, "webhook_url", None)
            if webhook_url:
                results["webhook"] = self.distribute_to_website(forecast, webhook_url)
        
        return results


def create_distribution_api(
    host: str = "0.0.0.0",
    port: int = 8080
):
    """
    Create a REST API server for distributing weather data.
    This can be used by news agencies, apps, and websites.
    
    Note: Requires Flask or FastAPI to be installed.
    This is a template showing the architecture.
    """
    try:
        from flask import Flask, jsonify, request
        app = Flask(__name__)
        
        @app.route("/api/v1/weather/current", methods=["GET"])
        def get_current_weather():
            """API endpoint for current weather data."""
            city = request.args.get("city", "Cincinnati")
            # In production, fetch from database/storage
            return jsonify({"city": city, "status": "API endpoint ready"})
        
        @app.route("/api/v1/weather/forecast", methods=["GET"])
        def get_forecast():
            """API endpoint for weather forecasts."""
            city = request.args.get("city", "Cincinnati")
            hours = int(request.args.get("hours", 24))
            # In production, fetch from prediction models
            return jsonify({"city": city, "hours": hours, "status": "API endpoint ready"})
        
        @app.route("/api/v1/weather/subscribe", methods=["POST"])
        def subscribe():
            """Subscribe to weather updates via webhook."""
            data = request.json
            webhook_url = data.get("webhook_url")
            # In production, store subscription and push updates
            return jsonify({"status": "Subscription endpoint ready"})
        
        logger.info(f"Distribution API server ready at http://{host}:{port}")
        return app
        
    except ImportError:
        logger.warning("Flask not installed. Install with: pip install flask")
        return None


if __name__ == "__main__":
    # Example usage
    distributor = WeatherDataDistributor()
    
    # Example weather data
    weather_data = {
        "city": "Cincinnati",
        "temp_C": 20.5,
        "humidity_pct": 65,
        "forecast": "Clear skies"
    }
    
    # Distribute via Kafka
    distributor.distribute_via_kafka(weather_data)
    
    # Distribute to multiple channels
    forecast = {"city": "Cincinnati", "temp_24h": 22.0}
    results = distributor.distribute_forecast(forecast, channels=["kafka", "api"])
    print(f"Distribution results: {results}")

