"""Example Kafka producer for streaming weather data."""
from confluent_kafka import Producer
import json
import logging
from typing import Dict, Any, Optional
import sys
from pathlib import Path

# Add src to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from src.config import settings

logger = logging.getLogger(__name__)


class WeatherKafkaProducer:
    """Kafka producer for weather data."""
    
    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None
    ):
        """
        Initialize Kafka producer.
        
        Args:
            bootstrap_servers: Kafka broker addresses
            topic: Kafka topic name
        """
        self.bootstrap_servers = bootstrap_servers or settings.kafka_bootstrap_servers
        self.topic = topic or settings.kafka_topic
        
        self.producer = Producer({
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": "weather-producer"
        })
        
        logger.info(f"Initialized Kafka producer: topic={self.topic}, servers={self.bootstrap_servers}")
    
    def produce(self, data: Dict[str, Any], key: Optional[str] = None):
        """
        Produce weather data to Kafka topic.
        
        Args:
            data: Weather data dictionary
            key: Optional message key (e.g., city name)
        """
        try:
            value = json.dumps(data).encode("utf-8")
            key_bytes = key.encode("utf-8") if key else None
            
            self.producer.produce(
                self.topic,
                value=value,
                key=key_bytes,
                callback=self._delivery_callback
            )
            
            # Trigger delivery callbacks
            self.producer.poll(0)
            
        except Exception as e:
            logger.error(f"Error producing to Kafka: {e}")
            raise
    
    def flush(self, timeout: float = 10.0):
        """
        Flush pending messages.
        
        Args:
            timeout: Maximum time to wait for messages to be delivered
        """
        self.producer.flush(timeout)
        logger.info("Flushed Kafka producer")
    
    @staticmethod
    def _delivery_callback(err, msg):
        """Callback for message delivery confirmation."""
        if err:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def example_usage():
    """Example usage of Kafka producer."""
    producer = WeatherKafkaProducer()
    
    # Example weather data
    weather_data = {
        "timestamp_utc": "2024-01-15T10:00:00+00:00",
        "city": "Kathmandu",
        "temp_C": 20.5,
        "humidity_pct": 65,
        "source": "openweather"
    }
    
    # Produce message
    producer.produce(weather_data, key="Kathmandu")
    
    # Flush to ensure delivery
    producer.flush()
    
    print("✓ Message sent to Kafka")


if __name__ == "__main__":
    example_usage()

