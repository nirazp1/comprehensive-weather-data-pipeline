"""Configuration management for weather data pipeline."""
import os
from pathlib import Path
from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # API Keys
    openweather_api_key: Optional[str] = Field(None, env="OPENWEATHER_API_KEY")
    noaa_api_key: Optional[str] = Field(None, env="NOAA_API_KEY")
    
    # AWS Configuration
    aws_access_key_id: Optional[str] = Field(None, env="AWS_ACCESS_KEY_ID")
    aws_secret_access_key: Optional[str] = Field(None, env="AWS_SECRET_ACCESS_KEY")
    aws_region: str = Field("us-east-1", env="AWS_REGION")
    s3_bucket: Optional[str] = Field(None, env="S3_BUCKET")
    
    # Kafka Configuration
    kafka_bootstrap_servers: str = Field("localhost:9092", env="KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic: str = Field("weather-data", env="KAFKA_TOPIC")
    
    # Data Paths
    raw_data_dir: Path = Field(Path("./data/raw"), env="RAW_DATA_DIR")
    processed_data_dir: Path = Field(Path("./data/processed"), env="PROCESSED_DATA_DIR")
    
    # API Settings
    request_timeout: int = Field(10, env="REQUEST_TIMEOUT")
    max_retries: int = Field(3, env="MAX_RETRIES")
    retry_backoff_factor: float = Field(2.0, env="RETRY_BACKOFF_FACTOR")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


# Global settings instance
# Try to load from .env file, but don't fail if it doesn't exist
env_file = Path(".env")
if not env_file.exists():
    print("⚠️  Warning: .env file not found.")
    print("   Please create a .env file with your API keys. See .env.example for reference.")
    print("   The pipeline will work, but you'll need to set OPENWEATHER_API_KEY to fetch data.")

# Load settings (will use environment variables if .env doesn't exist)
settings = Settings()

# Create directories if they don't exist
settings.raw_data_dir.mkdir(parents=True, exist_ok=True)
settings.processed_data_dir.mkdir(parents=True, exist_ok=True)

