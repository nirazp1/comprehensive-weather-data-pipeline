# Dockerfile for weather data ingestion pipeline
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r /app/requirements.txt

# Install Playwright browsers (if needed)
# RUN playwright install chromium

# Copy application code
COPY src/ /app/src/
COPY kafka/ /app/kafka/
COPY .env* /app/ 2>/dev/null || true

# Create data directories
RUN mkdir -p /app/data/raw /app/data/processed

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src

# Default command (can be overridden)
# Use kafka.simple_end_to_end_kafka for Kafka integration
CMD ["python", "-m", "src.simple_end_to_end"]

