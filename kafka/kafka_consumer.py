"""Kafka consumer for weather data streaming."""
from confluent_kafka import Consumer, KafkaError
import json
import logging
import signal
import sys
from typing import Dict, Any, Optional
import sys
from pathlib import Path

# Add src to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from src.config import settings
from src.storage import get_storage

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class WeatherKafkaConsumer:
    """Kafka consumer for weather data."""
    
    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None,
        group_id: str = "weather-consumer-group"
    ):
        """
        Initialize Kafka consumer.
        
        Args:
            bootstrap_servers: Kafka broker addresses
            topic: Kafka topic name
            group_id: Consumer group ID
        """
        self.bootstrap_servers = bootstrap_servers or settings.kafka_bootstrap_servers
        self.topic = topic or settings.kafka_topic
        self.group_id = group_id
        
        self.consumer = Consumer({
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True
        })
        
        self.consumer.subscribe([self.topic])
        self.running = True
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info(f"Initialized Kafka consumer: topic={self.topic}, group={self.group_id}, servers={self.bootstrap_servers}")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info("Shutdown signal received, closing consumer...")
        self.running = False
    
    def consume(self, timeout: float = 1.0):
        """
        Consume messages from Kafka topic.
        
        Args:
            timeout: Timeout in seconds for polling
        """
        logger.info(f"Starting to consume from topic: {self.topic}")
        
        try:
            while self.running:
                msg = self.consumer.poll(timeout=timeout)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Reached end of partition {msg.partition()}")
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        continue
                
                # Process message
                try:
                    data = json.loads(msg.value().decode("utf-8"))
                    key = msg.key().decode("utf-8") if msg.key() else None
                    
                    logger.info(f"Received message: key={key}, partition={msg.partition()}, offset={msg.offset()}")
                    self._process_message(data, key)
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode message: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
        
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        finally:
            logger.info("Closing consumer...")
            self.consumer.close()
    
    def _process_message(self, data: Dict[str, Any], key: Optional[str] = None):
        """
        Process a consumed message.
        
        Args:
            data: Weather data dictionary
            key: Message key (e.g., city name)
        """
        city = key or data.get("city", "unknown")
        logger.info(f"Processing weather data for {city}: temp={data.get('temp_C')}°C")
        
        # You can add custom processing here:
        # - Save to database
        # - Trigger alerts
        # - Update dashboards
        # - etc.
        
        # Example: Save to storage
        try:
            storage = get_storage(backend="local")
            timestamp = data.get("timestamp_utc", "").replace(":", "-")
            filename = f"kafka_{city}_{timestamp}.json"
            storage.save_json(filename, data, subdir="kafka")
            logger.info(f"✓ Saved message to storage: {filename}")
        except Exception as e:
            logger.warning(f"Could not save to storage: {e}")


def main():
    """Main entry point for Kafka consumer."""
    consumer = WeatherKafkaConsumer()
    consumer.consume()


if __name__ == "__main__":
    main()

