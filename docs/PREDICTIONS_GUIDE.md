# Weather Predictions Guide

## 🔮 What Predictions Are We Doing?

This project implements **machine learning-based weather forecasting** using historical weather data. Currently, we predict:

### Primary Prediction: Temperature (°C)
- **Short-term forecasts**: 1-168 hours ahead (up to 1 week)
- **Based on**: 5 years of historical weather data
- **Uses**: Time patterns, historical trends, recent weather patterns

### How It Works

1. **Feature Engineering**: 
   - Time-based features (hour, day of year, month)
   - Cyclical encoding (sin/cos for time patterns)
   - Lag features (previous temperature values)
   - Rolling statistics (moving averages)

2. **Model Training**:
   - Trains on 5 years of historical data
   - Uses 80% for training, 20% for testing
   - Multiple model types available

3. **Prediction**:
   - Uses recent weather data (last 7 days)
   - Generates forecasts for future hours
   - Provides confidence metrics

## 📊 Model Types

### 1. Random Forest (Recommended)
- **Best for**: Non-linear patterns, complex relationships
- **Accuracy**: Typically highest R² score
- **Speed**: Moderate training time

### 2. Gradient Boosting
- **Best for**: Sequential patterns, time series
- **Accuracy**: Very good, often close to Random Forest
- **Speed**: Slower training

### 3. Linear Regression
- **Best for**: Simple trends, baseline comparison
- **Accuracy**: Lower but faster
- **Speed**: Fastest

## 🎯 Usage Examples

### Web Interface (Easiest)
```bash
streamlit run web/streamlit_app.py
# Go to "🔮 Weather Predictions" page
```

### Python API
```python
from src.predictions import train_and_predict
from src.config import settings

# Train and predict for Cincinnati
data_dir = settings.processed_data_dir / "5year_historical"
result = train_and_predict(
    city="Cincinnati",
    data_dir=data_dir,
    model_type="random_forest",
    hours_ahead=48
)

# View results
print(f"Model R²: {result['metrics']['test_r2']:.3f}")
print(f"Predictions: {result['predictions']}")
```

## 📈 Model Performance Metrics

- **MAE (Mean Absolute Error)**: Average prediction error in °C (lower is better)
- **R² (R-squared)**: How well the model fits the data (0-1, higher is better)
- **Train vs Test**: Shows if model is overfitting

## 🔮 Future Predictions (Planned)

- **Humidity predictions**
- **Pressure predictions**
- **Wind speed/direction**
- **Weather condition (rain, snow, etc.)**
- **Long-term forecasts (weeks/months)**
- **Multi-city predictions**
- **Ensemble models** (combining multiple models)

## 📋 Requirements

```bash
pip install scikit-learn>=1.3.0
```

## 💡 Tips

1. **More data = Better predictions**: Use 5-year historical data for best results
2. **Recent data matters**: Model uses last 7 days for context
3. **City-specific**: Each city has its own weather patterns
4. **Compare models**: Use the comparison tab to find the best model for each city

