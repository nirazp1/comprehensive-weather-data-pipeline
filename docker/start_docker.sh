#!/bin/bash
# Quick start script for Docker and Kafka setup

set -e

echo "=========================================="
echo "Weather Data Pipeline - Docker Setup"
echo "=========================================="
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "✗ Docker is not running. Please start Docker Desktop."
    exit 1
fi

echo "✓ Docker is running"
echo ""

# Start Zookeeper and Kafka
echo "Starting Zookeeper and Kafka..."
docker-compose up -d zookeeper kafka

echo ""
echo "Waiting for Kafka to be ready..."
sleep 10

# Check if services are healthy
if docker-compose ps | grep -q "kafka.*healthy"; then
    echo "✓ Kafka is ready"
else
    echo "⚠ Kafka may still be starting. Check with: docker-compose ps"
fi

echo ""
echo "=========================================="
echo "Services Status:"
echo "=========================================="
docker-compose ps

echo ""
echo "=========================================="
echo "Next Steps:"
echo "=========================================="
echo "1. Test Kafka: python3 kafka/test_kafka.py"
echo "2. Run pipeline: python3 -m kafka.simple_end_to_end_kafka"
echo "3. Or use Docker: cd docker && docker-compose up weather-ingester"
echo "4. View logs: docker-compose logs -f"
echo ""
echo "To stop services: docker-compose down"
echo ""

