"""
Statistics and Analytics Module for Weather Data
Generates comprehensive statistics from historical weather data.
"""
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import json

def load_all_data(data_dir: Path, limit: Optional[int] = None) -> pd.DataFrame:
    """
    Load all Parquet files from data directory.
    
    Args:
        data_dir: Directory containing Parquet files
        limit: Optional limit on number of records to load
        
    Returns:
        Combined DataFrame
    """
    if not data_dir.exists():
        return pd.DataFrame()
    
    parquet_files = list(data_dir.glob("**/*.parquet"))
    if not parquet_files:
        return pd.DataFrame()
    
    dfs = []
    for file in parquet_files:
        try:
            df = pd.read_parquet(file)
            dfs.append(df)
            if limit and len(pd.concat(dfs, ignore_index=True)) >= limit:
                break
        except Exception as e:
            print(f"Warning: Error reading {file.name}: {e}")
    
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return pd.DataFrame()

def calculate_city_statistics(df: pd.DataFrame, city: str) -> Dict[str, Any]:
    """
    Calculate comprehensive statistics for a specific city.
    
    Args:
        df: DataFrame with weather data
        city: City name
        
    Returns:
        Dictionary with statistics
    """
    city_data = df[df['city'] == city].copy()
    
    if city_data.empty:
        return {}
    
    # Convert timestamp if needed
    if 'timestamp_utc' in city_data.columns:
        city_data['timestamp_utc'] = pd.to_datetime(city_data['timestamp_utc'])
        city_data['year'] = city_data['timestamp_utc'].dt.year
        city_data['month'] = city_data['timestamp_utc'].dt.month
        city_data['day'] = city_data['timestamp_utc'].dt.day
    
    stats = {
        'city': city,
        'total_records': len(city_data),
        'date_range': {
            'start': str(city_data['timestamp_utc'].min()) if 'timestamp_utc' in city_data.columns else None,
            'end': str(city_data['timestamp_utc'].max()) if 'timestamp_utc' in city_data.columns else None,
            'span_days': (city_data['timestamp_utc'].max() - city_data['timestamp_utc'].min()).days if 'timestamp_utc' in city_data.columns else 0
        }
    }
    
    # Temperature statistics
    if 'temp_C' in city_data.columns:
        stats['temperature'] = {
            'mean': float(city_data['temp_C'].mean()),
            'median': float(city_data['temp_C'].median()),
            'min': float(city_data['temp_C'].min()),
            'max': float(city_data['temp_C'].max()),
            'std': float(city_data['temp_C'].std()),
            'q25': float(city_data['temp_C'].quantile(0.25)),
            'q75': float(city_data['temp_C'].quantile(0.75))
        }
    
    # Humidity statistics
    if 'humidity_pct' in city_data.columns:
        stats['humidity'] = {
            'mean': float(city_data['humidity_pct'].mean()),
            'median': float(city_data['humidity_pct'].median()),
            'min': float(city_data['humidity_pct'].min()),
            'max': float(city_data['humidity_pct'].max()),
            'std': float(city_data['humidity_pct'].std())
        }
    
    # Pressure statistics
    if 'pressure_hPa' in city_data.columns:
        stats['pressure'] = {
            'mean': float(city_data['pressure_hPa'].mean()),
            'median': float(city_data['pressure_hPa'].median()),
            'min': float(city_data['pressure_hPa'].min()),
            'max': float(city_data['pressure_hPa'].max()),
            'std': float(city_data['pressure_hPa'].std())
        }
    
    # Wind statistics
    if 'wind_speed_mps' in city_data.columns:
        stats['wind'] = {
            'mean_speed': float(city_data['wind_speed_mps'].mean()),
            'max_speed': float(city_data['wind_speed_mps'].max()),
            'mean_direction': float(city_data['wind_direction_deg'].mean()) if 'wind_direction_deg' in city_data.columns else None
        }
    
    # Weather conditions distribution
    if 'weather_main' in city_data.columns:
        weather_counts = city_data['weather_main'].value_counts()
        stats['weather_conditions'] = {
            'distribution': weather_counts.to_dict(),
            'most_common': weather_counts.index[0] if len(weather_counts) > 0 else None,
            'most_common_pct': float((weather_counts.iloc[0] / len(city_data)) * 100) if len(weather_counts) > 0 else 0
        }
    
    # Monthly statistics
    if 'month' in city_data.columns and 'temp_C' in city_data.columns:
        monthly_stats = city_data.groupby('month')['temp_C'].agg(['mean', 'min', 'max']).to_dict()
        stats['monthly_temperature'] = {
            f'month_{month}': {
                'mean': float(monthly_stats['mean'][month]),
                'min': float(monthly_stats['min'][month]),
                'max': float(monthly_stats['max'][month])
            }
            for month in monthly_stats['mean'].keys()
        }
    
    # Yearly statistics
    if 'year' in city_data.columns and 'temp_C' in city_data.columns:
        yearly_stats = city_data.groupby('year')['temp_C'].agg(['mean', 'min', 'max', 'count']).to_dict()
        stats['yearly_temperature'] = {
            str(year): {
                'mean': float(yearly_stats['mean'][year]),
                'min': float(yearly_stats['min'][year]),
                'max': float(yearly_stats['max'][year]),
                'records': int(yearly_stats['count'][year])
            }
            for year in yearly_stats['mean'].keys()
        }
    
    # Seasonal statistics (Northern Hemisphere)
    if 'month' in city_data.columns and 'temp_C' in city_data.columns:
        city_data['season'] = city_data['month'].apply(lambda m: 
            'Winter' if m in [12, 1, 2] else
            'Spring' if m in [3, 4, 5] else
            'Summer' if m in [6, 7, 8] else 'Fall'
        )
        seasonal_stats = city_data.groupby('season')['temp_C'].agg(['mean', 'min', 'max']).to_dict()
        stats['seasonal_temperature'] = {
            season: {
                'mean': float(seasonal_stats['mean'][season]),
                'min': float(seasonal_stats['min'][season]),
                'max': float(seasonal_stats['max'][season])
            }
            for season in seasonal_stats['mean'].keys()
        }
    
    return stats

def generate_all_cities_statistics(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Generate statistics for all cities in the dataset.
    
    Args:
        df: DataFrame with weather data
        
    Returns:
        Dictionary with statistics for all cities
    """
    if df.empty or 'city' not in df.columns:
        return {}
    
    cities = df['city'].unique()
    all_stats = {}
    
    for city in cities:
        all_stats[city] = calculate_city_statistics(df, city)
    
    # Overall statistics
    overall = {
        'total_cities': len(cities),
        'total_records': len(df),
        'date_range': {
            'start': str(df['timestamp_utc'].min()) if 'timestamp_utc' in df.columns else None,
            'end': str(df['timestamp_utc'].max()) if 'timestamp_utc' in df.columns else None
        }
    }
    
    if 'temp_C' in df.columns:
        overall['temperature'] = {
            'global_mean': float(df['temp_C'].mean()),
            'global_min': float(df['temp_C'].min()),
            'global_max': float(df['temp_C'].max())
        }
    
    all_stats['_overall'] = overall
    
    return all_stats

def save_statistics(stats: Dict[str, Any], output_file: Path):
    """
    Save statistics to JSON file.
    
    Args:
        stats: Statistics dictionary
        output_file: Output file path
    """
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Convert numpy types to native Python types for JSON serialization
    def convert_types(obj):
        if isinstance(obj, (np.integer, np.int64)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, dict):
            return {key: convert_types(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [convert_types(item) for item in obj]
        return obj
    
    stats_serializable = convert_types(stats)
    
    with open(output_file, 'w') as f:
        json.dump(stats_serializable, f, indent=2, default=str)
    
    print(f"✓ Statistics saved to {output_file}")

def generate_statistics_report(df: pd.DataFrame, output_dir: Path) -> Path:
    """
    Generate a comprehensive statistics report.
    
    Args:
        df: DataFrame with weather data
        output_dir: Output directory
        
    Returns:
        Path to statistics file
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate statistics
    all_stats = generate_all_cities_statistics(df)
    
    # Save to JSON
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    stats_file = output_dir / f"statistics_{timestamp}.json"
    save_statistics(all_stats, stats_file)
    
    # Generate summary text report
    summary_file = output_dir / f"statistics_summary_{timestamp}.txt"
    with open(summary_file, 'w') as f:
        f.write("=" * 70 + "\n")
        f.write("WEATHER DATA STATISTICS REPORT\n")
        f.write("=" * 70 + "\n\n")
        
        if '_overall' in all_stats:
            overall = all_stats['_overall']
            f.write(f"Total Cities: {overall.get('total_cities', 0)}\n")
            f.write(f"Total Records: {overall.get('total_records', 0):,}\n")
            f.write(f"Date Range: {overall.get('date_range', {}).get('start', 'N/A')} to {overall.get('date_range', {}).get('end', 'N/A')}\n")
            if 'temperature' in overall:
                f.write(f"Global Average Temperature: {overall['temperature'].get('global_mean', 0):.2f}°C\n")
            f.write("\n" + "=" * 70 + "\n\n")
        
        # City statistics
        for city, city_stats in all_stats.items():
            if city == '_overall':
                continue
            
            f.write(f"\n{'=' * 70}\n")
            f.write(f"CITY: {city}\n")
            f.write(f"{'=' * 70}\n")
            f.write(f"Total Records: {city_stats.get('total_records', 0):,}\n")
            
            if 'date_range' in city_stats:
                f.write(f"Date Range: {city_stats['date_range'].get('start', 'N/A')} to {city_stats['date_range'].get('end', 'N/A')}\n")
                f.write(f"Span: {city_stats['date_range'].get('span_days', 0)} days\n")
            
            if 'temperature' in city_stats:
                temp = city_stats['temperature']
                f.write(f"\nTemperature Statistics:\n")
                f.write(f"  Mean: {temp.get('mean', 0):.2f}°C\n")
                f.write(f"  Median: {temp.get('median', 0):.2f}°C\n")
                f.write(f"  Min: {temp.get('min', 0):.2f}°C\n")
                f.write(f"  Max: {temp.get('max', 0):.2f}°C\n")
                f.write(f"  Std Dev: {temp.get('std', 0):.2f}°C\n")
            
            if 'humidity' in city_stats:
                hum = city_stats['humidity']
                f.write(f"\nHumidity Statistics:\n")
                f.write(f"  Mean: {hum.get('mean', 0):.1f}%\n")
                f.write(f"  Min: {hum.get('min', 0):.1f}%\n")
                f.write(f"  Max: {hum.get('max', 0):.1f}%\n")
            
            if 'seasonal_temperature' in city_stats:
                f.write(f"\nSeasonal Averages:\n")
                for season, data in city_stats['seasonal_temperature'].items():
                    f.write(f"  {season}: {data.get('mean', 0):.2f}°C\n")
            
            if 'weather_conditions' in city_stats:
                f.write(f"\nMost Common Weather: {city_stats['weather_conditions'].get('most_common', 'N/A')} "
                       f"({city_stats['weather_conditions'].get('most_common_pct', 0):.1f}%)\n")
            
            f.write("\n")
    
    print(f"✓ Statistics report saved to {summary_file}")
    return stats_file

if __name__ == "__main__":
    from .config import settings
    
    # Load data
    data_dir = settings.processed_data_dir
    df = load_all_data(data_dir)
    
    if df.empty:
        print("No data found. Generate data first.")
    else:
        print(f"Loaded {len(df):,} records")
        stats_file = generate_statistics_report(df, data_dir / "statistics")
        print(f"Statistics generated: {stats_file}")

