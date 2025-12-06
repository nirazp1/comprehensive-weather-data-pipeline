# Weather Data Ingestion Pipeline - Big Data Edition

A comprehensive, production-ready pipeline for scraping, ingesting, and storing weather data from multiple sources. **Optimized for large-scale data processing with support for generating and managing BILLIONS of records.**

## Features

- **🌐 Large-Scale Data Generation**: Generate millions to billions of weather records using CLI or web interface
- **Multiple Data Sources**: OpenWeatherMap, NOAA, Meteostat APIs
- **Synchronous & Asynchronous Fetching**: Basic scripts and high-performance async implementations
- **Data Normalization**: Convert various API formats to a canonical schema
- **Storage Backends**: Local filesystem, S3, Parquet for analytics (optimized for big data)
- **Streaming Support**: Kafka producer for real-time data pipelines
- **Error Handling**: Retries with exponential backoff, validation, deduplication
- **Web Scraping**: Playwright support for JavaScript-rendered sites
- **Production Ready**: Configuration management, logging, monitoring hooks
- **Memory-Efficient Processing**: Vectorized operations and streaming for handling massive datasets

## Project Structure

```
.
├── src/                          # Core pipeline source code
│   ├── __init__.py
│   ├── config.py                # Configuration management
│   ├── normalize.py             # Data normalization
│   ├── fetch_openweather.py     # Synchronous fetcher
│   ├── fetch_many.py            # Async fetcher
│   ├── simple_end_to_end.py     # Main pipeline
│   ├── storage.py               # Storage backends
│   ├── utils.py                 # Utilities and validation
│   └── data_generator.py        # Big data generator
│
├── docker/                       # Docker configuration
│   ├── Dockerfile
│   ├── docker-compose.yml
│   ├── .dockerignore
│   └── start_docker.sh
│
├── kafka/                        # Kafka-related code
│   ├── kafka_producer_example.py
│   ├── kafka_consumer.py
│   ├── simple_end_to_end_kafka.py
│   └── test_kafka.py
│
├── web/                          # Web interface
│   └── streamlit_app.py         # Streamlit application
│
├── scripts/                      # CLI scripts
│   ├── generate_big_data_cli.py # Big data generator CLI
│   └── run_streamlit.sh         # Streamlit launcher
│
├── examples/                     # Example code
│   ├── playwright_example.py
│   └── airflow_dag_example.py
│
├── tests/                        # Test suite
│   ├── test_normalize.py
│   └── test_utils.py
│
├── docs/                         # Documentation
│   ├── README.md                # Main documentation
│   ├── RESEARCH_PAPER.tex       # LaTeX research paper
│   └── about/                   # Additional guides
│
├── data/                         # Data directories
│   ├── raw/                     # Raw JSON data
│   └── processed/               # Processed Parquet files
│
├── requirements.txt             # Python dependencies
├── .env.example                 # Environment variables template
├── pytest.ini                   # Pytest configuration
└── README.md                    # This file
```

## Quick Start

> **Note**: On macOS, use `python3` and `pip3` instead of `python` and `pip`. The examples below use `python3`.

### 1. Installation

```bash
# Install dependencies
pip3 install -r requirements.txt

# Install Playwright browsers (if using web scraping)
playwright install chromium
```

### 2. Configuration

Copy the example environment file and fill in your API keys:

```bash
cp .env.example .env
```

Edit `.env` and add your API keys:
- `OPENWEATHER_API_KEY`: Get from [OpenWeatherMap](https://openweathermap.org/api)
- `NOAA_API_KEY`: Optional, for NOAA data
- AWS credentials if using S3 storage

### 3. Generate Large-Scale Data (Big Data Projects)

**🌐 Generate Massive Datasets via CLI:**

```bash
# List available presets
python3 scripts/generate_big_data_cli.py --list-presets

# Generate 10 million records using preset
python3 scripts/generate_big_data_cli.py --preset "Medium (10M records)"

# Generate custom dataset: 5000 cities, 2 years, hourly
python3 scripts/generate_big_data_cli.py --cities 5000 --days 730 --frequency hourly

# Generate 1 billion+ records (massive scale)
python3 scripts/generate_big_data_cli.py --cities 20000 --days 1825 --frequency hourly

# Generate with custom output directory
python3 scripts/generate_big_data_cli.py --cities 10000 --days 365 --output-dir ./my_big_data
```

**Example Output:**
```
🌐 BIG DATA GENERATOR - Configuration
======================================================================
  Cities:           10,000
  Days:             365 (1.0 years)
  Frequency:        hourly
  Expected Records: 87,600,000 (87.60M)
  Output Directory: data/processed/big_data
  Estimated Size:   16.33 GB
  Estimated Time:   ~88 minutes
======================================================================

🚀 Starting data generation...
🔄 Progress: 87,600,000 / 87,600,000 (100.0%) | Rate: 16,500 rec/s | ETA: 0.0 min

✅ GENERATION COMPLETE!
======================================================================
  Total Records:    87,600,000 (87.60M)
  Files Created:    365
  Total Size:       15.42 GB
  Time Elapsed:     88.5 minutes
  Generation Rate:  16,500 records/second
======================================================================
```

**Available Presets:**
- **Small (1M records)**: 100 cities × 365 days × hourly
- **Medium (10M records)**: 1,000 cities × 365 days × hourly
- **Large (100M records)**: 5,000 cities × 730 days × hourly
- **Very Large (500M records)**: 10,000 cities × 1,825 days × hourly
- **Massive (1B+ records)**: 20,000 cities × 1,825 days × hourly
- **Ultra Massive (5B+ records)**: 50,000 cities × 3,650 days × hourly

### 4. Run Basic PoC

**Option A: Command Line**
```bash
# Simple synchronous fetch
python3 -m src.fetch_openweather

# Asynchronous fetch for multiple cities
python3 -m src.fetch_many

# End-to-end pipeline (fetch → normalize → save to Parquet)
python3 -m src.simple_end_to_end

# End-to-end pipeline with Kafka (requires Kafka running)
python3 -m kafka.simple_end_to_end_kafka
```

**Option B: Web Interface (Recommended)**
```bash
# Start Streamlit web application
streamlit run web/streamlit_app.py

# Or use the quick start script
./scripts/run_streamlit.sh
```

Then open http://localhost:8501 in your browser for an easy-to-use web interface!

**The web interface includes:**
- 🌐 Big Data Generator page for generating massive datasets
- 🔍 Visual Pipeline Flow showing processing steps
- 📊 Analytics for exploring large datasets
- 📥 Data fetching and viewing capabilities

### 5. Docker and Kafka Setup (Optional)

```bash
# Navigate to docker directory
cd docker

# Start Kafka and Zookeeper
docker-compose up -d zookeeper kafka

# Test Kafka setup
python3 ../kafka/test_kafka.py

# Run pipeline with Docker
docker-compose up weather-ingester

# Consume from Kafka
python3 ../kafka/kafka_consumer.py
```

Or use the convenience script:
```bash
./docker/start_docker.sh
```

See `docs/about/DOCKER_KAFKA_SETUP.md` for detailed instructions.

## Usage Examples

### 🌐 Generate Massive Datasets (Big Data)

**CLI Tool for Large-Scale Data Generation:**

```bash
# List available presets
python3 scripts/generate_big_data_cli.py --list-presets

# Generate 10 million records using preset
python3 scripts/generate_big_data_cli.py --preset "Medium (10M records)"

# Generate custom: 10,000 cities, 1 year, hourly (87.6M records)
python3 scripts/generate_big_data_cli.py --cities 10000 --days 365 --frequency hourly

# Generate 1 billion+ records (massive scale)
python3 scripts/generate_big_data_cli.py --cities 20000 --days 1825 --frequency hourly

# Generate with custom output directory
python3 scripts/generate_big_data_cli.py --cities 5000 --days 730 --output-dir ./my_big_data
```

**Example: Generating 100 Million Records**
```bash
$ python3 scripts/generate_big_data_cli.py --cities 5000 --days 730 --frequency hourly

🌐 BIG DATA GENERATOR - Configuration
======================================================================
  Cities:           5,000
  Days:             730 (2.0 years)
  Frequency:        hourly
  Expected Records: 87,600,000 (87.60M)
  Output Directory: data/processed/big_data
  Estimated Size:   16.33 GB
  Estimated Time:   ~88 minutes
======================================================================

🚀 Starting data generation...
🔄 Progress: 87,600,000 / 87,600,000 (100.0%) | Rate: 16,500 rec/s

✅ GENERATION COMPLETE!
  Total Records:    87,600,000 (87.60M)
  Files Created:    730
  Total Size:       15.42 GB
  Time Elapsed:     88.5 minutes
  Generation Rate:  16,500 records/second
```

**Key Features:**
- ✅ Memory-efficient streaming (handles billions of records)
- ✅ Vectorized NumPy operations (10-100x faster)
- ✅ Automatic file partitioning by date
- ✅ Progress tracking and ETA
- ✅ Snappy compression for Parquet files
- ✅ Support for 100,000+ cities
- ✅ Support for 10,000+ days (~27 years)
- ✅ **American cities** (Cincinnati highlighted with ⭐)

### Web Interface (Easiest)

```bash
# Start the web app
streamlit run web/streamlit_app.py
```

Then use the browser interface to:
- 🌐 **Big Data Generator**: Generate millions to billions of records
- 🔍 **Visual Pipeline Flow**: See processing steps in real-time
- 📥 Fetch weather data with one click
- 📊 View and filter large datasets
- ⚡ Run the complete pipeline
- 📈 Explore analytics and visualizations
- ⚙️ Check system settings

### Command Line

```python
from src.fetch_openweather import fetch, save_raw

# Fetch weather for a city
data = fetch("Cincinnati")  # ⭐ Your city!
save_raw(data, "Cincinnati")
```

### Async Fetch Multiple Cities

```python
import asyncio
from src.fetch_many import main

# Fetch multiple cities concurrently
asyncio.run(main(cities=["Cincinnati", "New York", "Los Angeles"], max_concurrent=5))
```

### End-to-End Pipeline

```python
from src.simple_end_to_end import run

# Fetch, normalize, validate, and save to Parquet
run("Cincinnati")  # ⭐ Your city!
```

### Normalize Data

```python
from src.normalize import normalize_openweather, get_normalizer

# Normalize OpenWeatherMap response
raw_data = {...}  # API response
normalized = normalize_openweather(raw_data)

# Or use generic normalizer
normalizer = get_normalizer("openweather")
normalized = normalizer(raw_data)
```

### Store to S3

```python
from src.storage import S3Storage

storage = S3Storage()
storage.upload_json("weather_20240115.json", weather_data, prefix="raw/")
```

### Stream to Kafka

```python
from kafka.kafka_producer_example import WeatherKafkaProducer

producer = WeatherKafkaProducer()
producer.produce(weather_data, key="Cincinnati")  # ⭐ Your city!
producer.flush()
```

## Data Schema

The canonical weather data schema includes:

```python
{
    "timestamp_utc": "2024-01-15T10:00:00+00:00",  # ISO format UTC
    "lat": 39.1031,  # Cincinnati coordinates
    "lon": -84.5120,
    "city": "Cincinnati",  # ⭐ Your city!
    "country": "US",
    "temp_C": 20.5,
    "feels_like_C": 19.8,
    "temp_min_C": 18.0,
    "temp_max_C": 23.0,
    "pressure_hPa": 1013.25,
    "humidity_pct": 65,
    "wind_speed_mps": 3.5,
    "wind_direction_deg": 180,
    "wind_gust_mps": 5.0,
    "cloudiness_pct": 40,
    "visibility_m": 10000,
    "weather_main": "Clear",
    "weather_description": "clear sky",
    "rain_1h_mm": 0.0,
    "snow_1h_mm": 0.0,
    "source": "openweather",
    "raw": {...}  # Original API response
}
```

## Configuration

All configuration is managed through environment variables (see `.env.example`):

- **API Keys**: `OPENWEATHER_API_KEY`, `NOAA_API_KEY`
- **Storage**: `RAW_DATA_DIR`, `PROCESSED_DATA_DIR`, S3 settings
- **Kafka**: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`
- **Retry Settings**: `MAX_RETRIES`, `RETRY_BACKOFF_FACTOR`

## Data Validation

The pipeline includes validation for:
- Required fields
- Timestamp validity (not in future, not too old)
- Numeric ranges (temperature, humidity, pressure)
- Schema compliance

```python
from src.utils import validate_weather_record

is_valid, error = validate_weather_record(normalized_data)
if not is_valid:
    print(f"Validation error: {error}")
```

## Error Handling & Retries

Automatic retries with exponential backoff:

```python
from src.utils import with_retry
import requests

@with_retry(max_attempts=3, backoff_factor=2.0)
def fetch_with_retry(url):
    return requests.get(url)
```

## Next Steps

### Production Deployment

1. **Orchestration**: Set up Apache Airflow DAGs for scheduled ingestion
2. **Time-Series Database**: Load data into TimescaleDB or InfluxDB
3. **Monitoring**: Add Prometheus metrics and Grafana dashboards
4. **Containerization**: Dockerize and deploy to Kubernetes
5. **CI/CD**: Set up automated testing and deployment

### Example Airflow DAG Structure

See `examples/airflow_dag_example.py` for a complete example.

## Operational Checklist

- [ ] Confirm source Terms of Service and rate limits
- [ ] Store API keys in secrets manager (AWS Secrets Manager, Vault)
- [ ] Test PoC scripts with your API keys
- [ ] Set up data storage (S3 bucket, local directories)
- [ ] Configure monitoring and alerting
- [ ] Set up orchestration (Airflow, cron, etc.)
- [ ] Implement data retention policies
- [ ] Document data sources and schemas
- [ ] Set up CI/CD pipeline
- [ ] Create runbooks for common issues

## Tips & Best Practices

1. **Rate Limiting**: Always respect API rate limits. Use delays between requests.
2. **Raw Data**: Always store raw API responses for debugging and reprocessing.
3. **Idempotency**: Deduplicate by `(source, city, timestamp)`.
4. **Timezone**: Normalize all timestamps to UTC.
5. **Partitioning**: Use date-based partitioning for Parquet files (YYYY/MM/DD).
6. **Monitoring**: Track success rates, latency, data freshness.
7. **Secrets**: Never commit API keys. Use environment variables or secrets managers.

## Troubleshooting

### API Key Issues
- Verify your API key is correct in `.env`
- Check API key permissions and rate limits
- Ensure you're not hitting rate limits

### Import Errors
- Make sure all dependencies are installed: `pip3 install -r requirements.txt`
- Check Python version (3.9+ recommended)
- Ensure you're using the correct import paths (e.g., `from src.config import settings`)

### Storage Issues
- Verify S3 credentials and bucket permissions
- Check local directory permissions
- Ensure sufficient disk space


## Contributing

Feel free to extend this pipeline with:
- Additional data sources
- More sophisticated validation rules
- ML model integration
- Real-time dashboards
- Advanced monitoring

## Resources

- [OpenWeatherMap API Docs](https://openweathermap.org/api)
- [NOAA API Documentation](https://www.weather.gov/documentation/services-web-api)
- [Meteostat API](https://dev.meteostat.net/)
- [Apache Airflow](https://airflow.apache.org/)
- [TimescaleDB](https://www.timescale.com/)
- [Kafka Documentation](https://kafka.apache.org/documentation/)

