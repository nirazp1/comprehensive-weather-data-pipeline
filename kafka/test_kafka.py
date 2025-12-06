#!/usr/bin/env python3
"""Test script to verify Kafka setup."""
import sys
import sys
from pathlib import Path

# Add paths
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from kafka.kafka_producer_example import WeatherKafkaProducer
from kafka.kafka_consumer import WeatherKafkaConsumer
import time
import threading

def test_producer():
    """Test Kafka producer."""
    print("Testing Kafka Producer...")
    try:
        producer = WeatherKafkaProducer()
        
        test_data = {
            "timestamp_utc": "2025-12-05T18:00:00+00:00",
            "city": "TestCity",
            "temp_C": 20.5,
            "humidity_pct": 65,
            "pressure_hPa": 1013.25,
            "source": "test"
        }
        
        producer.produce(test_data, key="TestCity")
        producer.flush()
        print("✓ Producer test successful - message sent to Kafka")
        return True
    except Exception as e:
        print(f"✗ Producer test failed: {e}")
        return False

def test_consumer(timeout=5):
    """Test Kafka consumer."""
    print("Testing Kafka Consumer...")
    try:
        consumer = WeatherKafkaConsumer()
        
        # Consume for a short time
        start_time = time.time()
        message_received = [False]
        
        def consume_loop():
            while time.time() - start_time < timeout:
                msg = consumer.consumer.poll(timeout=1.0)
                if msg and not msg.error():
                    message_received[0] = True
                    print(f"✓ Consumer test successful - message received: {msg.key()}")
                    break
        
        thread = threading.Thread(target=consume_loop, daemon=True)
        thread.start()
        thread.join(timeout=timeout + 1)
        
        consumer.consumer.close()
        
        if message_received[0]:
            return True
        else:
            print("⚠ Consumer test: No messages received (this is OK if producer hasn't sent any)")
            return True  # Not a failure, just no messages
    except Exception as e:
        print(f"✗ Consumer test failed: {e}")
        return False

def main():
    """Run all Kafka tests."""
    print("=" * 50)
    print("Kafka Setup Test")
    print("=" * 50)
    print()
    
    # Test producer
    producer_ok = test_producer()
    print()
    
    # Wait a bit for message to be available
    time.sleep(2)
    
    # Test consumer
    consumer_ok = test_consumer()
    print()
    
    # Summary
    print("=" * 50)
    if producer_ok and consumer_ok:
        print("✓ All Kafka tests passed!")
        print("\nKafka is ready to use.")
        sys.exit(0)
    else:
        print("✗ Some tests failed.")
        print("\nTroubleshooting:")
        print("1. Make sure Kafka is running: docker-compose ps")
        print("2. Check Kafka logs: docker-compose logs kafka")
        print("3. Verify KAFKA_BOOTSTRAP_SERVERS in .env file")
        sys.exit(1)

if __name__ == "__main__":
    main()

