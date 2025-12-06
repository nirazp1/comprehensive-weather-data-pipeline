"""
Example Apache Airflow DAG for weather data ingestion pipeline.

To use this DAG:
1. Install Airflow: pip install apache-airflow
2. Place this file in your Airflow DAGs folder (usually ~/airflow/dags/)
3. Configure connections in Airflow UI for API keys and S3
4. Adjust schedule_interval and other parameters as needed
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Import your pipeline functions
# Note: You'll need to make these functions importable or define them here
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from fetch_openweather import fetch, save_raw
from normalize import normalize_openweather
from simple_end_to_end import save_to_parquet
from storage import get_storage
from utils import validate_weather_record
import logging

logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'weather-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

# Define the DAG
dag = DAG(
    'weather_ingestion_pipeline',
    default_args=default_args,
    description='Hourly weather data ingestion from OpenWeatherMap',
    schedule_interval='@hourly',  # Run every hour
    catchup=False,
    tags=['weather', 'ingestion', 'etl'],
)


def fetch_weather_data(**context):
    """Fetch raw weather data from API."""
    cities = ["Cincinnati", "New York", "Los Angeles"]  # Example American cities
    fetched_data = []
    
    for city in cities:
        try:
            logger.info(f"Fetching weather for {city}")
            data = fetch(city)
            filepath = save_raw(data, city)
            fetched_data.append({"city": city, "filepath": str(filepath), "data": data})
            logger.info(f"✓ Fetched and saved {city}")
        except Exception as e:
            logger.error(f"Error fetching {city}: {e}")
    
    # Store in XCom for next task
    context['ti'].xcom_push(key='fetched_data', value=fetched_data)
    return fetched_data


def normalize_weather_data(**context):
    """Normalize weather data to canonical schema."""
    # Pull data from previous task
    fetched_data = context['ti'].xcom_pull(key='fetched_data', task_ids='fetch_weather')
    
    normalized_data = []
    for item in fetched_data:
        try:
            raw_data = item["data"]
            normalized = normalize_openweather(raw_data)
            
            # Validate
            is_valid, error = validate_weather_record(normalized)
            if not is_valid:
                logger.warning(f"Validation failed for {item['city']}: {error}")
                continue
            
            normalized_data.append(normalized)
            logger.info(f"✓ Normalized {item['city']}")
        except Exception as e:
            logger.error(f"Error normalizing {item['city']}: {e}")
    
    context['ti'].xcom_push(key='normalized_data', value=normalized_data)
    return normalized_data


def store_processed_data(**context):
    """Store normalized data to Parquet and optionally S3."""
    normalized_data = context['ti'].xcom_pull(
        key='normalized_data',
        task_ids='normalize_weather_data'
    )
    
    stored_files = []
    for record in normalized_data:
        try:
            city = record.get("city", "unknown")
            filepath = save_to_parquet(record, city)
            stored_files.append(str(filepath))
            logger.info(f"✓ Stored {city} to {filepath}")
        except Exception as e:
            logger.error(f"Error storing {city}: {e}")
    
    # Optionally upload to S3
    try:
        storage = get_storage(backend="s3")
        for record in normalized_data:
            city = record.get("city", "unknown")
            timestamp = record.get("timestamp_utc", "").replace(":", "-")
            key = f"weather_{city}_{timestamp}.json"
            storage.upload_json(key, record, prefix="processed/")
    except Exception as e:
        logger.warning(f"Could not upload to S3: {e}")
    
    context['ti'].xcom_push(key='stored_files', value=stored_files)
    return stored_files


# Define tasks
fetch_task = PythonOperator(
    task_id='fetch_weather',
    python_callable=fetch_weather_data,
    dag=dag,
)

normalize_task = PythonOperator(
    task_id='normalize_weather_data',
    python_callable=normalize_weather_data,
    dag=dag,
)

store_task = PythonOperator(
    task_id='store_processed_data',
    python_callable=store_processed_data,
    dag=dag,
)

# Optional: Data quality check task
def data_quality_check(**context):
    """Perform data quality checks."""
    normalized_data = context['ti'].xcom_pull(
        key='normalized_data',
        task_ids='normalize_weather_data'
    )
    
    if not normalized_data:
        raise ValueError("No normalized data to check")
    
    # Check data freshness
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    
    for record in normalized_data:
        ts_str = record.get("timestamp_utc", "")
        try:
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            age_hours = (now - ts).total_seconds() / 3600
            if age_hours > 2:
                logger.warning(f"Data for {record.get('city')} is {age_hours:.1f} hours old")
        except:
            pass
    
    logger.info(f"✓ Data quality check passed for {len(normalized_data)} records")
    return True


quality_check_task = PythonOperator(
    task_id='data_quality_check',
    python_callable=data_quality_check,
    dag=dag,
)

# Define task dependencies
fetch_task >> normalize_task >> store_task >> quality_check_task

