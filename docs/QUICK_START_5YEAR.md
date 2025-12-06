# Quick Start: 5-Year Historical Data & Statistics

## 🚀 Generate 5 Years of Data + Statistics

### Option 1: One-Command Script (Recommended)
```bash
python3 scripts/generate_5year_stats.py
```

This will:
- Generate 5 years of hourly data for 30 American cities (including ⭐ Cincinnati!)
- Create ~1.3 million records
- Automatically generate comprehensive statistics
- Save statistics reports (JSON + text)

### Option 2: Web Interface
```bash
streamlit run web/streamlit_app.py
```

Then navigate to: **"📊 5-Year Statistics"** page

### Option 3: CLI with Preset
```bash
python3 scripts/generate_big_data_cli.py --preset "5 Years Historical (Recommended)"
```

## 📊 What Statistics Are Generated?

For each city:
- **Temperature**: Mean, median, min, max, std dev, quartiles
- **Humidity**: Mean, min, max
- **Pressure**: Mean, min, max
- **Wind**: Average speed, max speed, direction
- **Weather Conditions**: Distribution and most common
- **Monthly Averages**: Temperature by month
- **Yearly Trends**: Temperature trends over 5 years
- **Seasonal Patterns**: Winter, Spring, Summer, Fall averages

## 📁 Output Files

- **Data**: `data/processed/5year_historical/`
- **Statistics JSON**: `data/processed/5year_historical/statistics/statistics_YYYYMMDD_HHMMSS.json`
- **Statistics Report**: `data/processed/5year_historical/statistics/statistics_summary_YYYYMMDD_HHMMSS.txt`

## ⭐ Cincinnati Special Analysis

The web interface includes a dedicated "⭐ Cincinnati Analysis" tab with:
- 5-year temperature trends
- Monthly averages
- Year-by-year comparison
- Detailed statistics

## 📈 View Statistics

### In Web Interface:
1. Go to "📊 5-Year Statistics" page
2. Click "📈 View Statistics" tab
3. Select a statistics file
4. Choose a city to view detailed stats

### In Python:
```python
from src.statistics import load_all_data, calculate_city_statistics
from src.config import settings

# Load data
df = load_all_data(settings.processed_data_dir / "5year_historical")

# Get Cincinnati statistics
cinci_stats = calculate_city_statistics(df, "Cincinnati")
print(f"Average Temperature: {cinci_stats['temperature']['mean']:.2f}°C")
```

