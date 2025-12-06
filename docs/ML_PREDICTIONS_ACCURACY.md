# ML Model Predictions & Accuracy

## 🔮 What We're Predicting

### Primary Prediction: **Temperature (°C)**

Our ML models predict **future temperature** in Celsius degrees for weather forecasting.

**Prediction Details:**
- **Target Variable**: `temp_C` (Temperature in Celsius)
- **Forecast Horizon**: 1-168 hours ahead (up to 1 week)
- **Frequency**: Hourly predictions
- **Based On**: 5 years of historical weather data
- **Model Types**: Random Forest, Gradient Boosting, Linear Regression

### What the Model Uses (Features)

The model uses **11-14 features** to make predictions:

1. **Time-Based Features** (6 features):
   - Hour of day (sin/cos encoding)
   - Day of year (sin/cos encoding)
   - Month (sin/cos encoding)

2. **Historical Temperature Patterns** (5 features):
   - Previous hour temperature (`temp_lag_1`)
   - Same time yesterday (`temp_lag_24`)
   - Same time last week (`temp_lag_168`)
   - 24-hour rolling mean
   - 24-hour rolling standard deviation

3. **Current Weather Conditions** (3-4 features):
   - Humidity percentage
   - Atmospheric pressure (hPa)
   - Wind speed (m/s)

## 📊 Accuracy Metrics

We measure accuracy using two key metrics:

### 1. MAE (Mean Absolute Error)
- **What it means**: Average prediction error in °C
- **Interpretation**: Lower is better
- **Example**: MAE of 2.5°C means predictions are typically within 2.5°C of actual temperature
- **Typical Range**: 1-5°C for weather forecasting

### 2. R² (R-squared / Coefficient of Determination)
- **What it means**: How well the model explains temperature variation
- **Range**: 0 to 1 (can be negative for very poor models)
- **Interpretation**: 
  - **0.9+**: Excellent (model explains 90%+ of variation)
  - **0.7-0.9**: Good (model explains 70-90% of variation)
  - **0.5-0.7**: Moderate (model explains 50-70% of variation)
  - **<0.5**: Poor (model explains less than 50% of variation)
- **Typical Range**: 0.7-0.95 for weather forecasting

## 🎯 Expected Accuracy

Based on the model architecture and typical weather forecasting:

### Random Forest Model (Recommended)
- **Expected MAE**: 1.5-3.5°C
- **Expected R²**: 0.80-0.95
- **Best for**: Non-linear patterns, complex relationships

### Gradient Boosting Model
- **Expected MAE**: 1.5-3.5°C
- **Expected R²**: 0.75-0.90
- **Best for**: Sequential patterns, time series

### Linear Regression Model
- **Expected MAE**: 2.5-5.0°C
- **Expected R²**: 0.60-0.80
- **Best for**: Baseline comparison, simple trends

## 📈 Model Training Details

- **Training Data**: 80% of historical data
- **Test Data**: 20% of historical data
- **Data Split**: Time-series split (no shuffling to preserve temporal order)
- **Training Samples**: Typically 1,000,000+ records (5 years × 30 cities × 24 hours/day)
- **Features**: 11-14 features per sample

## 🔍 How to Check Actual Accuracy

### Method 1: Web Interface
```bash
streamlit run web/streamlit_app.py
# Go to "🔮 Weather Predictions" page
# Select city and model type
# Click "Train & Predict"
# View accuracy metrics displayed
```

### Method 2: Python Script
```python
from src.predictions import train_and_predict
from src.config import settings

data_dir = settings.processed_data_dir / "5year_historical"
result = train_and_predict(
    city="Cincinnati",
    data_dir=data_dir,
    model_type="random_forest",
    hours_ahead=24
)

metrics = result['metrics']
print(f"Test MAE: {metrics['test_mae']:.3f}°C")
print(f"Test R²: {metrics['test_r2']:.3f}")
```

## 💡 Factors Affecting Accuracy

1. **Data Quality**: More historical data = better accuracy
2. **City Location**: Some cities have more predictable weather patterns
3. **Time Horizon**: Shorter predictions (1-24h) are more accurate than longer (48-168h)
4. **Season**: Predictions may be more accurate in stable seasons
5. **Model Type**: Random Forest typically performs best

## 📊 Example Output

```
🔮 PREDICTION: Temperature (°C)
   Model: Random Forest
   City: Cincinnati

📊 ACCURACY METRICS:
   Train MAE: 1.234°C
   Test MAE:  1.456°C
   Train R²:  0.923
   Test R²:   0.901

📈 INTERPRETATION:
   • MAE: Predictions are typically within 1.456°C of actual temperature
   • R²: Model explains 90.1% of temperature variation

✅ Model trained on 1,314,030 samples
✅ Using 14 features
```

## 🎯 Accuracy Benchmarks

For comparison, professional weather forecasting services:
- **Short-term (1-24h)**: MAE ~1-2°C, R² ~0.85-0.95
- **Medium-term (24-72h)**: MAE ~2-4°C, R² ~0.70-0.85
- **Long-term (72-168h)**: MAE ~3-6°C, R² ~0.60-0.75

Our models aim to achieve similar or better performance using historical data patterns.

