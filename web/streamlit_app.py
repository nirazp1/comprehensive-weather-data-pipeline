"""
Streamlit Web Application for Weather Data Ingestion Pipeline
Big Data Edition - Visual Pipeline Flow with Millions of Records
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import json
import time
from datetime import datetime, timedelta
import sys
import os
import asyncio
from typing import List, Dict, Any

# Add project root and src to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from src.config import settings
from src.fetch_openweather import fetch, save_raw
from src.fetch_many import main as fetch_many_async
from src.normalize import normalize_openweather, get_normalizer
from src.simple_end_to_end import run as run_pipeline
try:
    from kafka.simple_end_to_end_kafka import run as run_pipeline_kafka
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    run_pipeline_kafka = None
from src.utils import validate_weather_record
from src.storage import get_storage

# Import big data generator
try:
    from src.data_generator import (
        generate_millions_of_records,
        get_big_data_presets
    )
    BIG_DATA_AVAILABLE = True
except ImportError:
    BIG_DATA_AVAILABLE = False
    get_big_data_presets = None

# Import statistics module
try:
    from src.statistics import (
        load_all_data,
        calculate_city_statistics,
        generate_all_cities_statistics,
        generate_statistics_report
    )
    STATISTICS_AVAILABLE = True
except ImportError:
    STATISTICS_AVAILABLE = False

# Page configuration
st.set_page_config(
    page_title="Weather Data Pipeline - Big Data",
    page_icon="🌤️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for visual pipeline
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .pipeline-step {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .pipeline-step-active {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        animation: pulse 2s infinite;
    }
    .pipeline-step-complete {
        background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
    }
    .pipeline-arrow {
        text-align: center;
        font-size: 2rem;
        color: #667eea;
        margin: 0.5rem 0;
    }
    .metric-big {
        font-size: 2rem;
        font-weight: bold;
    }
    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.7; }
    }
    .thinking-box {
        background-color: #f8f9fa;
        border-left: 4px solid #667eea;
        padding: 1rem;
        margin: 1rem 0;
        border-radius: 0.25rem;
    }
</style>
""", unsafe_allow_html=True)

def format_city_name(city: str) -> str:
    """Format city name with star for Cincinnati."""
    if city == "Cincinnati":
        return f"⭐ {city} ⭐"  # Star marker for your city!
    return city

def init_session_state():
    """Initialize session state variables."""
    if 'pipeline_running' not in st.session_state:
        st.session_state.pipeline_running = False
    if 'pipeline_steps' not in st.session_state:
        st.session_state.pipeline_steps = []
    if 'last_fetch_time' not in st.session_state:
        st.session_state.last_fetch_time = None
    if 'fetch_results' not in st.session_state:
        st.session_state.fetch_results = []
    if 'big_data_stats' not in st.session_state:
        st.session_state.big_data_stats = {
            'total_records': 0,
            'total_cities': 0,
            'total_files': 0,
            'data_size_mb': 0
        }

def check_api_keys():
    """Check if API keys are configured (without exposing them)."""
    has_openweather = bool(settings.openweather_api_key and 
                           settings.openweather_api_key != "your_openweather_api_key_here" and
                           len(settings.openweather_api_key) > 10)
    has_noaa = bool(settings.noaa_api_key and 
                   settings.noaa_api_key != "your_noaa_api_key_here" and
                   len(settings.noaa_api_key) > 10)
    return has_openweather, has_noaa

def load_processed_data(limit: int = None):
    """Load processed Parquet files."""
    processed_dir = settings.processed_data_dir
    if not processed_dir.exists():
        return pd.DataFrame()
    
    parquet_files = list(processed_dir.glob("*.parquet"))
    if not parquet_files:
        return pd.DataFrame()
    
    dfs = []
    total_size = 0
    for file in parquet_files:
        try:
            df = pd.read_parquet(file)
            dfs.append(df)
            total_size += file.stat().st_size
        except Exception as e:
            st.warning(f"Error reading {file.name}: {e}")
    
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        if limit:
            combined_df = combined_df.head(limit)
        return combined_df, len(parquet_files), total_size / (1024 * 1024)  # MB
    return pd.DataFrame(), 0, 0

def visualize_pipeline_flow(steps: List[Dict[str, Any]], current_step: int = None):
    """Visualize the pipeline flow like LLM reasoning chains."""
    st.subheader("🔍 Pipeline Flow - What's Happening Under the Hood")
    
    for i, step in enumerate(steps):
        status_class = ""
        if current_step is not None:
            if i < current_step:
                status_class = "pipeline-step-complete"
            elif i == current_step:
                status_class = "pipeline-step-active"
        
        st.markdown(f"""
        <div class="pipeline-step {status_class}">
            <h4>Step {i+1}: {step['name']}</h4>
            <p>{step['description']}</p>
            {f"<strong>Status:</strong> {step.get('status', 'Pending')}<br>" if 'status' in step else ""}
            {f"<strong>Details:</strong> {step.get('details', '')}<br>" if 'details' in step else ""}
        </div>
        """, unsafe_allow_html=True)
        
        if i < len(steps) - 1:
            st.markdown('<div class="pipeline-arrow">⬇️</div>', unsafe_allow_html=True)

def show_thinking_process(step_name: str, details: str):
    """Show thinking process like LLM reasoning."""
    st.markdown(f"""
    <div class="thinking-box">
        <strong>🤔 Processing: {step_name}</strong><br>
        {details}
    </div>
    """, unsafe_allow_html=True)

def main():
    """Main Streamlit application."""
    init_session_state()
    
    # Header
    st.markdown('<div class="main-header">🌤️ Weather Data Pipeline - Big Data Edition</div>', 
                unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.header("📊 System Status")
        
        # API Key Status (no sensitive info)
        has_openweather, has_noaa = check_api_keys()
        
        st.subheader("API Configuration")
        if has_openweather:
            st.success("✅ OpenWeatherMap Ready")
        else:
            st.error("❌ OpenWeatherMap Not Configured")
        
        if has_noaa:
            st.success("✅ NOAA Ready")
        else:
            st.info("ℹ️ NOAA Optional")
        
        st.divider()
        
        # Big Data Stats
        st.subheader("📈 Big Data Statistics")
        df, file_count, data_size = load_processed_data(limit=10000)  # Load sample for stats
        
        if not df.empty:
            st.metric("Total Records", f"{len(df):,}")
            st.metric("Cities", df['city'].nunique() if 'city' in df.columns else 0)
            st.metric("Parquet Files", file_count)
            st.metric("Data Size", f"{data_size:.2f} MB")
        else:
            st.info("No data yet")
        
        st.divider()
        
        # Navigation
        st.subheader("📑 Navigation")
        page = st.radio(
            "Select Page",
            ["🏠 Dashboard", "🔍 Visual Pipeline", "📥 Fetch Data", "🌐 Big Data Generator", 
             "📊 View Data", "⚡ Run Pipeline", "📈 Analytics", "📊 5-Year Statistics", "🔮 Weather Predictions"],
            label_visibility="collapsed"
        )
    
    # Main content based on selected page
    if page == "🏠 Dashboard":
        show_dashboard()
    elif page == "🔍 Visual Pipeline":
        show_visual_pipeline()
    elif page == "📥 Fetch Data":
        show_fetch_data()
    elif page == "🌐 Big Data Generator":
        show_big_data_generator()
    elif page == "📊 View Data":
        show_view_data()
    elif page == "⚡ Run Pipeline":
        show_run_pipeline()
    elif page == "📈 Analytics":
        show_analytics()
    elif page == "📊 5-Year Statistics":
        show_5year_statistics()
    elif page == "🔮 Weather Predictions":
        show_predictions()

def show_dashboard():
    """Display main dashboard."""
    st.header("📊 Dashboard")
    
    # Status cards
    col1, col2, col3, col4 = st.columns(4)
    
    df, file_count, data_size = load_processed_data(limit=10000)
    
    with col1:
        has_openweather, _ = check_api_keys()
        st.metric("Pipeline Status", "✅ Ready" if has_openweather else "⚠️ Config Needed")
    
    with col2:
        st.metric("Total Records", f"{len(df):,}" if not df.empty else "0")
    
    with col3:
        if not df.empty and 'city' in df.columns:
            st.metric("Cities", df['city'].nunique())
        else:
            st.metric("Cities", "0")
    
    with col4:
        st.metric("Data Size", f"{data_size:.2f} MB" if data_size > 0 else "0 MB")
    
    st.divider()
    
    # Pipeline Architecture Visualization
    st.subheader("🏗️ System Architecture")
    
    architecture_steps = [
        {
            "name": "Data Sources",
            "description": "Multiple APIs (OpenWeatherMap, NOAA) and synthetic data generation",
            "status": "✅ Active"
        },
        {
            "name": "Data Ingestion",
            "description": "Synchronous and asynchronous fetching with retry logic",
            "status": "✅ Active"
        },
        {
            "name": "Data Normalization",
            "description": "Convert diverse formats to canonical schema",
            "status": "✅ Active"
        },
        {
            "name": "Data Validation",
            "description": "Schema checks, range validation, deduplication",
            "status": "✅ Active"
        },
        {
            "name": "Data Storage",
            "description": "Parquet files (columnar format) for analytics",
            "status": "✅ Active"
        },
        {
            "name": "Streaming (Optional)",
            "description": "Kafka for real-time data distribution",
            "status": "✅ Available" if KAFKA_AVAILABLE else "⚠️ Not Configured"
        }
    ]
    
    visualize_pipeline_flow(architecture_steps)
    
    st.divider()
    
    # Recent Data Preview
    st.subheader("📋 Recent Data Preview")
    
    if not df.empty:
        # Sort by timestamp
        if 'timestamp_utc' in df.columns:
            df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
            df = df.sort_values('timestamp_utc', ascending=False)
        
        # Display recent records with Cincinnati marked
        display_cols = ['timestamp_utc', 'city', 'temp_C', 'humidity_pct', 
                       'pressure_hPa', 'wind_speed_mps', 'weather_main', 'source']
        available_cols = [col for col in display_cols if col in df.columns]
        
        # Create display dataframe with formatted city names
        display_df = df[available_cols].head(20).copy()
        if 'city' in display_df.columns:
            display_df['city'] = display_df['city'].apply(format_city_name)
        
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True
        )
        
        # Quick chart with Cincinnati highlighted
        if 'temp_C' in df.columns and 'city' in df.columns:
            st.subheader("🌡️ Temperature Distribution")
            # Create a copy for charting with formatted names
            chart_df = df.copy()
            chart_df['city_display'] = chart_df['city'].apply(format_city_name)
            fig = px.box(chart_df, x='city_display', y='temp_C', title="Temperature by City (⭐ Cincinnati highlighted)")
            st.plotly_chart(fig, use_container_width=True)
            
            # Highlight Cincinnati in the sidebar
            if 'Cincinnati' in df['city'].values:
                st.info("⭐ **Cincinnati** is your home city and is highlighted in the data!")
    else:
        st.info("👆 No data available. Use 'Big Data Generator' or 'Fetch Data' to collect data.")

def show_visual_pipeline():
    """Show visual pipeline flow with step-by-step processing."""
    st.header("🔍 Visual Pipeline Flow")
    st.info("This page shows what happens 'under the hood' when processing weather data, similar to how LLMs show their reasoning process.")
    
    # Interactive pipeline demonstration
    st.subheader("🎯 Interactive Pipeline Demo")
    
    demo_city = st.text_input("Enter a city to process", value="Kathmandu")
    
    if st.button("🚀 Run Visual Pipeline Demo", type="primary"):
        has_openweather, _ = check_api_keys()
        
        if not has_openweather:
            st.error("⚠️ OpenWeatherMap API key required. Configure it in your .env file.")
            return
        
        # Define pipeline steps
        pipeline_steps = [
            {
                "name": "1. Data Source Selection",
                "description": "Selecting data source (OpenWeatherMap API)",
                "status": "In Progress"
            },
            {
                "name": "2. API Request",
                "description": f"Sending HTTP GET request to OpenWeatherMap API for {demo_city}",
                "status": "In Progress"
            },
            {
                "name": "3. Response Parsing",
                "description": "Parsing JSON response from API",
                "status": "Pending"
            },
            {
                "name": "4. Data Normalization",
                "description": "Converting API-specific format to canonical schema",
                "status": "Pending"
            },
            {
                "name": "5. Data Validation",
                "description": "Validating data quality (schema, ranges, types)",
                "status": "Pending"
            },
            {
                "name": "6. Storage",
                "description": "Saving to Parquet format for analytics",
                "status": "Pending"
            }
        ]
        
        # Process step by step
        status_placeholder = st.empty()
        progress_bar = st.progress(0)
        
        for i, step in enumerate(pipeline_steps):
            # Update step status
            step['status'] = "✅ Complete" if i > 0 else "🔄 Processing"
            if i > 0:
                pipeline_steps[i-1]['status'] = "✅ Complete"
            
            # Show current thinking
            show_thinking_process(
                step['name'],
                step['description']
            )
            
            # Visualize pipeline
            visualize_pipeline_flow(pipeline_steps, current_step=i)
            
            # Actually process
            if i == 1:  # API Request step
                try:
                    data = fetch(demo_city)
                    step['details'] = f"✅ Received {len(json.dumps(data))} bytes of data"
                except Exception as e:
                    step['details'] = f"❌ Error: {str(e)}"
                    st.error(f"Error: {e}")
                    break
            elif i == 2:  # Response Parsing
                step['details'] = "✅ JSON parsed successfully"
            elif i == 3:  # Normalization
                try:
                    normalized = normalize_openweather(data)
                    step['details'] = f"✅ Normalized to {len(normalized)} fields"
                except Exception as e:
                    step['details'] = f"❌ Error: {str(e)}"
                    break
            elif i == 4:  # Validation
                try:
                    is_valid, error_msg = validate_weather_record(normalized)
                    if is_valid:
                        step['details'] = "✅ Validation passed"
                    else:
                        step['details'] = f"⚠️ Validation warning: {error_msg}"
                except Exception as e:
                    step['details'] = f"❌ Error: {str(e)}"
            elif i == 5:  # Storage
                try:
                    result = run_pipeline(demo_city, validate=True)
                    if result:
                        step['details'] = f"✅ Saved to {result}"
                    else:
                        step['details'] = "⚠️ Storage skipped"
                except Exception as e:
                    step['details'] = f"❌ Error: {str(e)}"
            
            progress_bar.progress((i + 1) / len(pipeline_steps))
            time.sleep(0.5)  # Visual delay
        
        st.success("✅ Pipeline execution complete!")
        st.balloons()

def show_fetch_data():
    """Show data fetching interface."""
    st.header("📥 Fetch Weather Data")
    
    has_openweather, _ = check_api_keys()
    
    if not has_openweather:
        st.error("⚠️ OpenWeatherMap API key is required. Configure it in your .env file.")
        return
    
    # Fetch options
    st.subheader("Fetch Options")
    
    fetch_mode = st.radio(
        "Select Fetch Mode",
        ["Single City", "Multiple Cities"],
        horizontal=True
    )
    
    if fetch_mode == "Single City":
        city = st.text_input("City Name", value="Kathmandu")
        
        if st.button("🔍 Fetch Weather Data", type="primary"):
            with st.spinner(f"Fetching weather data for {city}..."):
                try:
                    data = fetch(city)
                    normalized = normalize_openweather(data)
                    is_valid, error_msg = validate_weather_record(normalized)
                    
                    if is_valid:
                        st.success(f"✅ Successfully fetched data for {city}!")
                        
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("Temperature", f"{normalized.get('temp_C', 'N/A'):.1f}°C" if normalized.get('temp_C') else "N/A")
                        with col2:
                            st.metric("Humidity", f"{normalized.get('humidity_pct', 'N/A'):.0f}%" if normalized.get('humidity_pct') else "N/A")
                        with col3:
                            st.metric("Pressure", f"{normalized.get('pressure_hPa', 'N/A'):.0f} hPa" if normalized.get('pressure_hPa') else "N/A")
                        with col4:
                            st.metric("Wind Speed", f"{normalized.get('wind_speed_mps', 'N/A'):.2f} m/s" if normalized.get('wind_speed_mps') else "N/A")
                    else:
                        st.error(f"❌ Validation failed: {error_msg}")
                
                except Exception as e:
                    st.error(f"❌ Error fetching data: {e}")
    
    else:  # Multiple Cities
        cities_input = st.text_area(
            "Enter Cities (one per line)",
            value="Kathmandu\nPokhara\nLalitpur",
            help="Enter city names, one per line"
        )
        
        cities = [c.strip() for c in cities_input.split('\n') if c.strip()]
        st.write(f"**Cities to fetch:** {len(cities)}")
        
        if st.button("🔍 Fetch All Cities", type="primary"):
            if not cities:
                st.warning("Please enter at least one city.")
                return
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            results = []
            
            for i, city in enumerate(cities):
                status_text.text(f"Fetching {city}... ({i+1}/{len(cities)})")
                try:
                    data = fetch(city)
                    normalized = normalize_openweather(data)
                    is_valid, _ = validate_weather_record(normalized)
                    
                    results.append({
                        'city': city,
                        'status': '✅ Success' if is_valid else '⚠️ Validation Failed',
                        'temp': normalized.get('temp_C'),
                        'humidity': normalized.get('humidity_pct')
                    })
                except Exception as e:
                    results.append({
                        'city': city,
                        'status': f'❌ Error: {str(e)[:50]}',
                        'temp': None,
                        'humidity': None
                    })
                
                progress_bar.progress((i + 1) / len(cities))
                time.sleep(0.5)
            
            status_text.text("✅ Fetching complete!")
            st.dataframe(pd.DataFrame(results), use_container_width=True, hide_index=True)

def show_big_data_generator():
    """Show big data generation interface."""
    st.header("🌐 Big Data Generator")
    st.info("🚀 Generate **BILLIONS** of weather records for large-scale big data testing and demonstration.")
    st.success("⭐ **Note:** Cincinnati (your home city!) is always included as the first city in generated datasets.")
    
    if not BIG_DATA_AVAILABLE:
        st.error("⚠️ Big data generator module not available. Please check data_generator.py")
        return
    
    # Presets section
    st.subheader("📋 Quick Presets")
    
    if get_big_data_presets:
        presets = get_big_data_presets()
        preset_names = list(presets.keys())
        
        selected_preset = st.selectbox(
            "Select a Preset (or use Custom below)",
            ["Custom"] + preset_names,
            help="Choose a preset configuration for common big data scenarios"
        )
        
        if selected_preset != "Custom":
            preset_config = presets[selected_preset]
            st.info(f"📊 {preset_config['description']}")
            preset_num_cities = preset_config['num_cities']
            preset_days = preset_config['days']
            preset_frequency = preset_config['frequency']
        else:
            preset_num_cities = 1000
            preset_days = 365
            preset_frequency = "hourly"
    else:
        selected_preset = "Custom"
        preset_num_cities = 1000
        preset_days = 365
        preset_frequency = "hourly"
    
    st.divider()
    st.subheader("⚙️ Generation Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        num_cities = st.number_input(
            "Number of Cities",
            min_value=1,
            max_value=100000,
            value=preset_num_cities if selected_preset != "Custom" else 1000,
            step=100,
            help="Number of cities to generate data for (up to 100,000)"
        )
        
        days = st.number_input(
            "Number of Days",
            min_value=1,
            max_value=10000,
            value=preset_days if selected_preset != "Custom" else 365,
            step=30,
            help="Number of days of historical data (up to 10,000 days = ~27 years)"
        )
    
    with col2:
        frequency = st.selectbox(
            "Data Frequency",
            ["hourly", "daily"],
            index=0 if preset_frequency == "hourly" else 1,
            help="How often to generate data points"
        )
    
    # Calculate expected records
    if frequency == "hourly":
        expected_records = num_cities * days * 24
    else:
        expected_records = num_cities * days
    
    # Format expected records
    if expected_records >= 1_000_000_000:
        records_display = f"{expected_records/1_000_000_000:.2f} billion"
    elif expected_records >= 1_000_000:
        records_display = f"{expected_records/1_000_000:.2f} million"
    else:
        records_display = f"{expected_records:,}"
    
    # Estimate size
    estimated_size_gb = (expected_records * 200) / (1024**3)  # Rough estimate: 200 bytes per record
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Expected Records", records_display)
    with col2:
        st.metric("Estimated Size", f"{estimated_size_gb:.2f} GB")
    with col3:
        estimated_time_min = max(1, expected_records / 1_000_000)  # Rough estimate: 1M records per minute
        st.metric("Est. Time", f"~{estimated_time_min:.0f} min")
    
    st.warning("⚠️ **Large datasets may take significant time and disk space. Ensure you have sufficient resources.**")
    
    if st.button("🚀 Generate Big Data", type="primary", use_container_width=True):
        output_dir = settings.processed_data_dir / "big_data"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Progress tracking
        progress_bar = st.progress(0)
        status_text = st.empty()
        stats_placeholder = st.empty()
        
        def progress_callback(current: int, total: int):
            """Update progress in Streamlit."""
            progress = current / total if total > 0 else 0
            progress_bar.progress(progress)
            status_text.text(f"Generated {current:,} / {total:,} records ({progress*100:.1f}%)")
            
            # Update stats
            if current % 100000 == 0 or current == total:
                stats_placeholder.markdown(f"""
                **Current Progress:**
                - Generated: {current:,} records
                - Remaining: {total - current:,} records
                - Progress: {progress*100:.1f}%
                """)
        
        try:
            # Show thinking process
            show_thinking_process(
                "Data Generation Started",
                f"Initializing streaming generator for {num_cities:,} cities over {days} days ({frequency} frequency)...\n"
                f"Using memory-efficient batch processing to handle {expected_records:,} records."
            )
            
            # Generate data with streaming
            total_records, saved_files = generate_millions_of_records(
                num_cities=num_cities,
                days=days,
                frequency=frequency,
                output_dir=output_dir,
                progress_callback=progress_callback
            )
            
            progress_bar.progress(1.0)
            status_text.text(f"✅ Generation complete! Generated {total_records:,} records")
            
            # Calculate actual file sizes
            total_size_mb = sum(f.stat().st_size for f in saved_files) / (1024 * 1024)
            total_size_gb = total_size_mb / 1024
            
            st.success(f"✅ **Successfully generated {total_records:,} records!**")
            st.success(f"✅ **Saved to {len(saved_files):,} Parquet files**")
            
            # Update stats
            st.session_state.big_data_stats = {
                'total_records': total_records,
                'total_cities': num_cities,
                'total_files': len(saved_files),
                'data_size_mb': total_size_mb,
                'data_size_gb': total_size_gb
            }
            
            # Show summary
            st.subheader("📊 Generation Summary")
            col1, col2, col3, col4, col5 = st.columns(5)
            with col1:
                st.metric("Total Records", f"{total_records:,}")
            with col2:
                st.metric("Cities", f"{num_cities:,}")
            with col3:
                st.metric("Files Created", f"{len(saved_files):,}")
            with col4:
                st.metric("Data Size", f"{total_size_gb:.2f} GB")
            with col5:
                st.metric("Avg Records/File", f"{total_records//len(saved_files):,}" if saved_files else "0")
            
            # Show file locations
            with st.expander("📁 View Generated Files"):
                st.write(f"**Output Directory:** `{output_dir}`")
                st.write(f"**Total Files:** {len(saved_files)}")
                if len(saved_files) <= 50:
                    for i, filepath in enumerate(saved_files[:50], 1):
                        file_size_mb = filepath.stat().st_size / (1024 * 1024)
                        st.write(f"{i}. {filepath.name} ({file_size_mb:.2f} MB)")
                else:
                    st.write(f"Showing first 50 of {len(saved_files)} files:")
                    for i, filepath in enumerate(saved_files[:50], 1):
                        file_size_mb = filepath.stat().st_size / (1024 * 1024)
                        st.write(f"{i}. {filepath.name} ({file_size_mb:.2f} MB)")
                    st.write(f"... and {len(saved_files) - 50} more files")
            
            st.balloons()
            st.info("💡 **Tip:** Use the 'View Data' page to explore your generated data!")
            
        except Exception as e:
            st.error(f"❌ Error generating data: {e}")
            st.exception(e)
            progress_bar.progress(0)

def show_view_data():
    """Show data viewing interface."""
    st.header("📊 View Processed Data")
    
    df, file_count, data_size = load_processed_data(limit=100000)  # Load up to 100k for viewing
    
    if df.empty:
        st.info("No processed data available. Use 'Big Data Generator' or 'Fetch Data' to collect data.")
        return
    
    st.success(f"✅ Loaded {len(df):,} records from {file_count} files ({data_size:.2f} MB)")
    
    # Filters
    st.subheader("🔍 Filters")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if 'city' in df.columns:
            city_list = sorted(df['city'].unique().tolist())[:100]  # Limit to 100 cities
            # Format cities for display with star for Cincinnati
            city_display = ['All'] + [format_city_name(c) for c in city_list]
            city_mapping = {format_city_name(c): c for c in city_list}
            city_mapping['All'] = 'All'
            selected_city_display = st.selectbox("City", city_display)
            selected_city = city_mapping[selected_city_display]
        else:
            selected_city = 'All'
    
    with col2:
        if 'source' in df.columns:
            sources = ['All'] + sorted(df['source'].unique().tolist())
            selected_source = st.selectbox("Data Source", sources)
        else:
            selected_source = 'All'
    
    with col3:
        if 'timestamp_utc' in df.columns:
            df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
            date_range = st.date_input(
                "Date Range",
                value=(df['timestamp_utc'].min().date(), df['timestamp_utc'].max().date()),
                min_value=df['timestamp_utc'].min().date(),
                max_value=df['timestamp_utc'].max().date()
            )
        else:
            date_range = None
    
    # Apply filters
    filtered_df = df.copy()
    if selected_city != 'All' and 'city' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['city'] == selected_city]
    if selected_source != 'All' and 'source' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['source'] == selected_source]
    if date_range and 'timestamp_utc' in filtered_df.columns:
        if isinstance(date_range, tuple) and len(date_range) == 2:
            filtered_df = filtered_df[
                (filtered_df['timestamp_utc'].dt.date >= date_range[0]) &
                (filtered_df['timestamp_utc'].dt.date <= date_range[1])
            ]
    
    st.metric("Filtered Records", f"{len(filtered_df):,}")
    
    # Data table
    st.subheader("📋 Data Table")
    
    if not filtered_df.empty:
        numeric_cols = filtered_df.select_dtypes(include=['float64', 'int64']).columns.tolist()
        text_cols = ['city', 'country', 'weather_main', 'weather_description', 'source', 'timestamp_utc']
        available_cols = [col for col in text_cols + numeric_cols if col in filtered_df.columns]
        
        selected_cols = st.multiselect(
            "Select Columns to Display",
            available_cols,
            default=available_cols[:10] if len(available_cols) > 10 else available_cols
        )
        
        if selected_cols:
            # Format city names for display
            display_filtered_df = filtered_df[selected_cols].head(1000).copy()
            if 'city' in display_filtered_df.columns:
                display_filtered_df['city'] = display_filtered_df['city'].apply(format_city_name)
            
            st.dataframe(
                display_filtered_df,
                use_container_width=True,
                hide_index=True
            )
            
            # Highlight if Cincinnati data is shown
            if 'city' in filtered_df.columns and 'Cincinnati' in filtered_df['city'].values:
                st.info("⭐ **Cincinnati** data is included in the results above!")
            
            # Download button
            csv = filtered_df[selected_cols].to_csv(index=False)
            st.download_button(
                "📥 Download as CSV",
                csv,
                f"weather_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                "text/csv"
            )

def show_run_pipeline():
    """Show pipeline execution interface."""
    st.header("⚡ Run Data Pipeline")
    
    has_openweather, _ = check_api_keys()
    
    if not has_openweather:
        st.error("⚠️ OpenWeatherMap API key is required. Configure it in your .env file.")
        return
    
    st.info("This will run the complete end-to-end pipeline: Fetch → Normalize → Validate → Store")
    
    cities_input = st.text_area(
        "Cities to Process",
        value="Kathmandu\nPokhara\nLalitpur",
        help="Enter city names, one per line"
    )
    
    cities = [c.strip() for c in cities_input.split('\n') if c.strip()]
    
    col1, col2 = st.columns(2)
    with col1:
        use_kafka = st.checkbox(
            "Use Kafka Streaming", 
            value=False, 
            help="Send data to Kafka topic",
            disabled=not KAFKA_AVAILABLE
        )
    with col2:
        validate_data = st.checkbox("Validate Data", value=True)
    
    if st.button("🚀 Run Pipeline", type="primary", use_container_width=True):
        if not cities:
            st.warning("Please enter at least one city.")
            return
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        results = []
        
        for i, city in enumerate(cities):
            status_text.text(f"Processing {city}... ({i+1}/{len(cities)})")
            try:
                if use_kafka and KAFKA_AVAILABLE and run_pipeline_kafka:
                    result = run_pipeline_kafka(city, validate=validate_data, use_kafka=True)
                else:
                    result = run_pipeline(city, validate=validate_data)
                
                if result:
                    results.append({
                        'city': city,
                        'status': '✅ Success',
                        'file': str(result)
                    })
                else:
                    results.append({
                        'city': city,
                        'status': '❌ Failed',
                        'file': None
                    })
            except Exception as e:
                results.append({
                    'city': city,
                    'status': f'❌ Error: {str(e)[:50]}',
                    'file': None
                })
            
            progress_bar.progress((i + 1) / len(cities))
            time.sleep(0.5)
        
        status_text.text("✅ Pipeline execution complete!")
        st.dataframe(pd.DataFrame(results), use_container_width=True, hide_index=True)
        st.rerun()

def show_analytics():
    """Show analytics and visualizations."""
    st.header("📈 Analytics & Visualizations")
    
    df, file_count, data_size = load_processed_data(limit=50000)  # Load sample for analytics
    
    if df.empty:
        st.info("No data available for analytics. Generate or fetch data first.")
        return
    
    if 'timestamp_utc' in df.columns:
        df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
    
    st.success(f"Analyzing {len(df):,} records")
    
    tab1, tab2, tab3, tab4 = st.tabs(["🌡️ Temperature", "💧 Humidity", "🌬️ Wind", "📊 Overview"])
    
    with tab1:
        if 'temp_C' in df.columns and 'city' in df.columns:
            # Format city names for display
            chart_df = df.copy()
            chart_df['city_display'] = chart_df['city'].apply(format_city_name)
            fig = px.box(chart_df, x='city_display', y='temp_C', title="Temperature Distribution by City (⭐ Cincinnati highlighted)")
            st.plotly_chart(fig, use_container_width=True)
            
            if 'timestamp_utc' in df.columns:
                fig = px.line(
                    chart_df.sort_values('timestamp_utc'),
                    x='timestamp_utc',
                    y='temp_C',
                    color='city_display',
                    title="Temperature Over Time (⭐ Cincinnati highlighted)"
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Show Cincinnati info if present
            if 'Cincinnati' in df['city'].values:
                cinci_data = df[df['city'] == 'Cincinnati']
                if not cinci_data.empty:
                    st.success(f"⭐ **Cincinnati** (Your City!): {len(cinci_data)} records, Avg Temp: {cinci_data['temp_C'].mean():.1f}°C")
    
    with tab2:
        if 'humidity_pct' in df.columns and 'city' in df.columns:
            humidity_df = df.groupby('city')['humidity_pct'].mean().reset_index()
            humidity_df['city_display'] = humidity_df['city'].apply(format_city_name)
            fig = px.bar(
                humidity_df,
                x='city_display',
                y='humidity_pct',
                title="Average Humidity by City (⭐ Cincinnati highlighted)"
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        if 'wind_speed_mps' in df.columns:
            fig = px.scatter(
                df,
                x='wind_speed_mps',
                y='city' if 'city' in df.columns else 'temp_C',
                size='wind_speed_mps',
                title="Wind Speed Analysis"
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with tab4:
        if 'temp_C' in df.columns:
            st.write("**Temperature Statistics:**")
            st.write(df['temp_C'].describe())
        
        if 'source' in df.columns:
            source_counts = df['source'].value_counts()
            fig = px.pie(values=source_counts.values, names=source_counts.index, title="Data by Source")
            st.plotly_chart(fig, use_container_width=True)

def show_5year_statistics():
    """Show 5-year statistics generation and viewing."""
    st.header("📊 5-Year Historical Data & Statistics")
    st.info("Generate 5 years of historical weather data for all cities and view comprehensive statistics.")
    
    tab1, tab2, tab3 = st.tabs(["🚀 Generate 5-Year Data", "📈 View Statistics", "⭐ Cincinnati Analysis"])
    
    with tab1:
        st.subheader("Generate 5 Years of Historical Data")
        st.success("⭐ **Cincinnati** will be included as the first city!")
        
        col1, col2 = st.columns(2)
        
        with col1:
            num_cities = st.number_input(
                "Number of Cities",
                min_value=1,
                max_value=100,
                value=30,
                help="Number of American cities (Cincinnati always first)"
            )
            
            frequency = st.selectbox(
                "Data Frequency",
                ["hourly", "daily"],
                index=0,
                help="Hourly = more detailed, Daily = faster generation"
            )
        
        with col2:
            years = st.number_input(
                "Years of Data",
                min_value=1,
                max_value=10,
                value=5,
                help="Number of years of historical data"
            )
            
            if frequency == "hourly":
                expected_records = num_cities * years * 365 * 24
            else:
                expected_records = num_cities * years * 365
            
            st.metric("Expected Records", f"{expected_records:,}")
            estimated_size_gb = (expected_records * 200) / (1024**3)
            st.metric("Estimated Size", f"{estimated_size_gb:.2f} GB")
        
        if st.button("🚀 Generate 5-Year Data", type="primary", use_container_width=True):
            output_dir = settings.processed_data_dir / "5year_historical"
            output_dir.mkdir(parents=True, exist_ok=True)
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            stats_placeholder = st.empty()
            
            def progress_callback(current: int, total: int):
                progress = current / total if total > 0 else 0
                progress_bar.progress(progress)
                status_text.text(f"Generated {current:,} / {total:,} records ({progress*100:.1f}%)")
            
            try:
                show_thinking_process(
                    "5-Year Data Generation",
                    f"Generating {years} years of {frequency} data for {num_cities} cities..."
                )
                
                total_records, saved_files = generate_millions_of_records(
                    num_cities=num_cities,
                    days=years * 365,
                    frequency=frequency,
                    output_dir=output_dir,
                    progress_callback=progress_callback
                )
                
                progress_bar.progress(1.0)
                status_text.text(f"✅ Generation complete! Generated {total_records:,} records")
                
                st.success(f"✅ **Successfully generated {total_records:,} records!**")
                st.success(f"✅ **Saved to {len(saved_files):,} Parquet files**")
                
                # Generate statistics automatically
                st.info("📊 Generating statistics...")
                df = load_all_data(output_dir)
                
                if not df.empty:
                    stats_file = generate_statistics_report(df, output_dir / "statistics")
                    st.success(f"✅ Statistics generated: {stats_file.name}")
                    st.balloons()
                else:
                    st.warning("⚠️ No data loaded for statistics")
                
            except Exception as e:
                st.error(f"❌ Error: {e}")
                st.exception(e)
    
    with tab2:
        st.subheader("View Statistics")
        
        # Load statistics
        stats_dir = settings.processed_data_dir / "5year_historical" / "statistics"
        
        if not stats_dir.exists():
            st.info("No statistics available. Generate 5-year data first.")
            return
        
        # Find latest statistics file
        stats_files = sorted(stats_dir.glob("statistics_*.json"), reverse=True)
        
        if not stats_files:
            st.info("No statistics files found. Generate 5-year data first.")
            return
        
        selected_file = st.selectbox(
            "Select Statistics File",
            [f.name for f in stats_files],
            help="Choose a statistics file to view"
        )
        
        if selected_file:
            import json
            stats_file = stats_dir / selected_file
            
            with open(stats_file, 'r') as f:
                stats = json.load(f)
            
            # Overall statistics
            if '_overall' in stats:
                overall = stats['_overall']
                st.subheader("📊 Overall Statistics")
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Cities", overall.get('total_cities', 0))
                with col2:
                    st.metric("Total Records", f"{overall.get('total_records', 0):,}")
                with col3:
                    if 'temperature' in overall:
                        st.metric("Global Avg Temp", f"{overall['temperature'].get('global_mean', 0):.2f}°C")
                with col4:
                    if 'date_range' in overall:
                        start = overall['date_range'].get('start', 'N/A')
                        if start != 'N/A':
                            start_date = start[:10] if len(start) > 10 else start
                            st.metric("Start Date", start_date)
            
            st.divider()
            
            # City statistics
            st.subheader("🏙️ City Statistics")
            
            cities = [c for c in stats.keys() if c != '_overall']
            selected_city = st.selectbox(
                "Select City",
                cities,
                format_func=lambda x: format_city_name(x)
            )
            
            if selected_city in stats:
                city_stats = stats[selected_city]
                
                st.markdown(f"### {format_city_name(selected_city)} - 5 Year Statistics")
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Total Records", f"{city_stats.get('total_records', 0):,}")
                    if 'date_range' in city_stats:
                        span_days = city_stats['date_range'].get('span_days', 0)
                        st.metric("Data Span", f"{span_days:,} days ({span_days/365:.1f} years)")
                
                with col2:
                    if 'temperature' in city_stats:
                        temp = city_stats['temperature']
                        st.metric("Avg Temperature", f"{temp.get('mean', 0):.2f}°C")
                        st.metric("Temp Range", f"{temp.get('min', 0):.1f}°C - {temp.get('max', 0):.1f}°C")
                
                with col3:
                    if 'humidity' in city_stats:
                        hum = city_stats['humidity']
                        st.metric("Avg Humidity", f"{hum.get('mean', 0):.1f}%")
                    if 'wind' in city_stats:
                        wind = city_stats['wind']
                        st.metric("Avg Wind Speed", f"{wind.get('mean_speed', 0):.2f} m/s")
                
                # Seasonal statistics
                if 'seasonal_temperature' in city_stats:
                    st.subheader("🌍 Seasonal Averages")
                    seasonal_data = city_stats['seasonal_temperature']
                    
                    seasons = list(seasonal_data.keys())
                    temps = [seasonal_data[s]['mean'] for s in seasons]
                    
                    fig = px.bar(
                        x=seasons,
                        y=temps,
                        title=f"Seasonal Average Temperature - {format_city_name(selected_city)}",
                        labels={'x': 'Season', 'y': 'Temperature (°C)'},
                        color=temps,
                        color_continuous_scale='RdYlBu_r'
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # Yearly trends
                if 'yearly_temperature' in city_stats:
                    st.subheader("📅 Yearly Temperature Trends")
                    yearly_data = city_stats['yearly_temperature']
                    
                    years = sorted([int(y) for y in yearly_data.keys()])
                    means = [yearly_data[str(y)]['mean'] for y in years]
                    mins = [yearly_data[str(y)]['min'] for y in years]
                    maxs = [yearly_data[str(y)]['max'] for y in years]
                    
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(x=years, y=means, name='Average', line=dict(width=3)))
                    fig.add_trace(go.Scatter(x=years, y=mins, name='Minimum', line=dict(dash='dash')))
                    fig.add_trace(go.Scatter(x=years, y=maxs, name='Maximum', line=dict(dash='dash')))
                    fig.update_layout(
                        title=f"5-Year Temperature Trends - {format_city_name(selected_city)}",
                        xaxis_title="Year",
                        yaxis_title="Temperature (°C)"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # Weather conditions
                if 'weather_conditions' in city_stats:
                    st.subheader("☁️ Weather Conditions Distribution")
                    weather_dist = city_stats['weather_conditions']['distribution']
                    
                    fig = px.pie(
                        values=list(weather_dist.values()),
                        names=list(weather_dist.keys()),
                        title=f"Weather Conditions - {format_city_name(selected_city)}"
                    )
                    st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        st.subheader("⭐ Cincinnati - 5 Year Analysis")
        st.success("**Cincinnati** is your home city! Here's a detailed 5-year analysis.")
        
        # Load data
        data_dir = settings.processed_data_dir / "5year_historical"
        df = load_all_data(data_dir)
        
        if df.empty:
            st.info("No 5-year data available. Generate it first using the 'Generate 5-Year Data' tab.")
        else:
            cinci_data = df[df['city'] == 'Cincinnati'].copy()
            
            if cinci_data.empty:
                st.warning("Cincinnati data not found in 5-year dataset. Generate data with at least 1 city.")
            else:
                # Calculate statistics
                cinci_stats = calculate_city_statistics(df, "Cincinnati")
                
                if cinci_stats:
                    st.markdown("### 📊 Cincinnati Statistics Summary")
                    
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric("Total Records", f"{cinci_stats.get('total_records', 0):,}")
                    with col2:
                        if 'temperature' in cinci_stats:
                            st.metric("Avg Temperature", f"{cinci_stats['temperature'].get('mean', 0):.2f}°C")
                    with col3:
                        if 'temperature' in cinci_stats:
                            st.metric("Record High", f"{cinci_stats['temperature'].get('max', 0):.2f}°C")
                    with col4:
                        if 'temperature' in cinci_stats:
                            st.metric("Record Low", f"{cinci_stats['temperature'].get('min', 0):.2f}°C")
                    
                    # Detailed charts
                    if 'timestamp_utc' in cinci_data.columns:
                        cinci_data['timestamp_utc'] = pd.to_datetime(cinci_data['timestamp_utc'])
                        cinci_data['year'] = cinci_data['timestamp_utc'].dt.year
                        cinci_data['month'] = cinci_data['timestamp_utc'].dt.month
                        
                        # Temperature over time
                        st.subheader("🌡️ Temperature Over 5 Years")
                        fig = px.line(
                            cinci_data.sort_values('timestamp_utc'),
                            x='timestamp_utc',
                            y='temp_C',
                            title="Cincinnati Temperature - 5 Year History",
                            labels={'temp_C': 'Temperature (°C)', 'timestamp_utc': 'Date'}
                        )
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Monthly averages
                        st.subheader("📅 Monthly Averages")
                        monthly_avg = cinci_data.groupby('month')['temp_C'].mean()
                        fig = px.bar(
                            x=monthly_avg.index,
                            y=monthly_avg.values,
                            title="Average Temperature by Month - Cincinnati",
                            labels={'x': 'Month', 'y': 'Temperature (°C)'},
                            color=monthly_avg.values,
                            color_continuous_scale='RdYlBu_r'
                        )
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Yearly comparison
                        st.subheader("📊 Year-by-Year Comparison")
                        yearly_avg = cinci_data.groupby('year')['temp_C'].agg(['mean', 'min', 'max'])
                        fig = go.Figure()
                        fig.add_trace(go.Bar(x=yearly_avg.index, y=yearly_avg['mean'], name='Average'))
                        fig.add_trace(go.Bar(x=yearly_avg.index, y=yearly_avg['min'], name='Minimum'))
                        fig.add_trace(go.Bar(x=yearly_avg.index, y=yearly_avg['max'], name='Maximum'))
                        fig.update_layout(
                            title="Cincinnati Temperature by Year",
                            xaxis_title="Year",
                            yaxis_title="Temperature (°C)",
                            barmode='group'
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    
                    # Show detailed statistics
                    with st.expander("📋 Detailed Statistics"):
                        st.json(cinci_stats)

def show_predictions():
    """Show weather prediction interface."""
    st.header("🔮 Weather Predictions")
    st.info("Use machine learning models to predict future weather conditions based on 5-year historical data.")
    
    if not PREDICTIONS_AVAILABLE:
        st.error("⚠️ Predictions module not available. Install scikit-learn: `pip install scikit-learn`")
        return
    
    # Check if 5-year data exists
    data_dir = settings.processed_data_dir / "5year_historical"
    
    if not data_dir.exists():
        st.warning("⚠️ No 5-year historical data found. Generate it first using the '📊 5-Year Statistics' page.")
        return
    
    # Load available cities
    df = load_all_data(data_dir)
    if df.empty:
        st.warning("⚠️ No data found in 5-year historical directory.")
        return
    
    cities = sorted(df['city'].unique())
    
    tab1, tab2 = st.tabs(["🎯 Train & Predict", "📊 Model Comparison"])
    
    with tab1:
        st.subheader("Train Model and Generate Predictions")
        
        col1, col2 = st.columns(2)
        
        with col1:
            selected_city = st.selectbox(
                "Select City",
                cities,
                format_func=lambda x: format_city_name(x),
                help="City to train model for and generate predictions"
            )
            
            model_type = st.selectbox(
                "Model Type",
                ["random_forest", "gradient_boosting", "linear"],
                help="Machine learning model to use"
            )
        
        with col2:
            hours_ahead = st.number_input(
                "Hours to Predict Ahead",
                min_value=1,
                max_value=168,  # 1 week
                value=24,
                help="Number of hours into the future to predict"
            )
            
            if st.button("🚀 Train & Predict", type="primary", use_container_width=True):
                with st.spinner("Training model and generating predictions..."):
                    try:
                        result = train_and_predict(
                            selected_city,
                            data_dir,
                            model_type=model_type,
                            hours_ahead=hours_ahead
                        )
                        
                        st.success("✅ Model trained and predictions generated!")
                        
                        # Show metrics
                        st.subheader("📊 Model Performance")
                        metrics = result['metrics']
                        
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("Train MAE", f"{metrics['train_mae']:.2f}°C")
                        with col2:
                            st.metric("Test MAE", f"{metrics['test_mae']:.2f}°C")
                        with col3:
                            st.metric("Train R²", f"{metrics['train_r2']:.3f}")
                        with col4:
                            st.metric("Test R²", f"{metrics['test_r2']:.3f}")
                        
                        st.info(f"📈 Model trained on {metrics['n_samples']:,} samples with {metrics['n_features']} features")
                        
                        # Show predictions
                        st.subheader(f"🔮 Predictions for {format_city_name(selected_city)} (Next {hours_ahead} Hours)")
                        predictions_df = result['predictions']
                        
                        # Display table
                        st.dataframe(
                            predictions_df[['timestamp_utc', 'predicted_temp_C', 'hours_ahead']].rename(columns={
                                'timestamp_utc': 'Time',
                                'predicted_temp_C': 'Predicted Temperature (°C)',
                                'hours_ahead': 'Hours Ahead'
                            }),
                            use_container_width=True,
                            hide_index=True
                        )
                        
                        # Plot predictions
                        fig = px.line(
                            predictions_df,
                            x='timestamp_utc',
                            y='predicted_temp_C',
                            title=f"Temperature Predictions - {format_city_name(selected_city)}",
                            labels={'timestamp_utc': 'Time', 'predicted_temp_C': 'Predicted Temperature (°C)'},
                            markers=True
                        )
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Show next 24 hours summary
                        if hours_ahead >= 24:
                            next_24h = predictions_df[predictions_df['hours_ahead'] <= 24]
                            st.subheader("📅 Next 24 Hours Summary")
                            
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("Average Temp", f"{next_24h['predicted_temp_C'].mean():.1f}°C")
                            with col2:
                                st.metric("Min Temp", f"{next_24h['predicted_temp_C'].min():.1f}°C")
                            with col3:
                                st.metric("Max Temp", f"{next_24h['predicted_temp_C'].max():.1f}°C")
                        
                    except Exception as e:
                        st.error(f"❌ Error: {e}")
                        st.exception(e)
    
    with tab2:
        st.subheader("Compare Different Models")
        st.info("Train multiple models and compare their performance.")
        
        selected_city_compare = st.selectbox(
            "Select City for Comparison",
            cities,
            format_func=lambda x: format_city_name(x),
            key="compare_city"
        )
        
        if st.button("🔬 Compare Models", type="primary"):
            model_types = ["random_forest", "gradient_boosting", "linear"]
            results = []
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            for i, model_type in enumerate(model_types):
                status_text.text(f"Training {model_type}...")
                try:
                    result = train_and_predict(
                        selected_city_compare,
                        data_dir,
                        model_type=model_type,
                        hours_ahead=24
                    )
                    results.append({
                        'model': model_type,
                        'test_mae': result['metrics']['test_mae'],
                        'test_r2': result['metrics']['test_r2'],
                        'train_mae': result['metrics']['train_mae'],
                        'train_r2': result['metrics']['train_r2']
                    })
                except Exception as e:
                    st.warning(f"Error training {model_type}: {e}")
                
                progress_bar.progress((i + 1) / len(model_types))
            
            if results:
                comparison_df = pd.DataFrame(results)
                
                st.subheader("📊 Model Comparison Results")
                st.dataframe(comparison_df, use_container_width=True, hide_index=True)
                
                # Visualize comparison
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=comparison_df['model'],
                    y=comparison_df['test_mae'],
                    name='Test MAE (°C)',
                    marker_color='lightblue'
                ))
                fig.add_trace(go.Bar(
                    x=comparison_df['model'],
                    y=comparison_df['test_r2'],
                    name='Test R²',
                    marker_color='lightgreen',
                    yaxis='y2'
                ))
                fig.update_layout(
                    title=f"Model Comparison - {format_city_name(selected_city_compare)}",
                    xaxis_title="Model Type",
                    yaxis_title="Test MAE (°C)",
                    yaxis2=dict(title="Test R²", overlaying='y', side='right'),
                    barmode='group'
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Best model
                best_model = comparison_df.loc[comparison_df['test_r2'].idxmax()]
                st.success(f"🏆 Best Model: **{best_model['model']}** (R² = {best_model['test_r2']:.3f}, MAE = {best_model['test_mae']:.2f}°C)")

if __name__ == "__main__":
    main()
