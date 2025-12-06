"""
Weather Prediction Models
Uses historical weather data to predict future weather conditions.
"""
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta, timezone
import warnings
warnings.filterwarnings('ignore')

try:
    from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
    from sklearn.linear_model import LinearRegression
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
    from sklearn.preprocessing import StandardScaler
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    print("⚠️  scikit-learn not installed. Install with: pip install scikit-learn")

from .statistics import load_all_data
from .config import settings


class WeatherPredictor:
    """
    Weather prediction model using historical data.
    Supports multiple prediction types and models.
    """
    
    def __init__(self, model_type: str = "random_forest"):
        """
        Initialize weather predictor.
        
        Args:
            model_type: Type of model ('random_forest', 'gradient_boosting', 'linear')
        """
        if not SKLEARN_AVAILABLE:
            raise ImportError("scikit-learn is required for predictions. Install with: pip install scikit-learn")
        
        self.model_type = model_type
        self.model = None
        self.scaler = StandardScaler()
        self.feature_columns = []
        self.target_column = None
        self.is_trained = False
        
    def _create_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create features from raw weather data for prediction.
        
        Args:
            df: DataFrame with weather data
            
        Returns:
            DataFrame with engineered features
        """
        df = df.copy()
        
        # Ensure timestamp is datetime
        if 'timestamp_utc' in df.columns:
            df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
        
        # Time-based features
        if 'timestamp_utc' in df.columns:
            df['hour'] = df['timestamp_utc'].dt.hour
            df['day_of_year'] = df['timestamp_utc'].dt.dayofyear
            df['month'] = df['timestamp_utc'].dt.month
            df['day_of_week'] = df['timestamp_utc'].dt.dayofweek
            
            # Cyclical encoding for time features
            df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
            df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
            df['day_sin'] = np.sin(2 * np.pi * df['day_of_year'] / 365)
            df['day_cos'] = np.cos(2 * np.pi * df['day_of_year'] / 365)
            df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
            df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
        
        # Lag features (previous values)
        if 'temp_C' in df.columns:
            df['temp_lag_1'] = df['temp_C'].shift(1)
            df['temp_lag_24'] = df['temp_C'].shift(24)  # Same time yesterday
            df['temp_lag_168'] = df['temp_C'].shift(168)  # Same time last week
        
        # Rolling statistics
        if 'temp_C' in df.columns:
            df['temp_rolling_mean_24'] = df['temp_C'].rolling(window=24, min_periods=1).mean()
            df['temp_rolling_std_24'] = df['temp_C'].rolling(window=24, min_periods=1).std()
        
        # Fill NaN values created by lag features
        df = df.fillna(method='bfill').fillna(method='ffill').fillna(0)
        
        return df
    
    def train(self, df: pd.DataFrame, target: str = "temp_C", city: Optional[str] = None):
        """
        Train the prediction model on historical data.
        
        Args:
            df: Historical weather data
            target: Target variable to predict (default: 'temp_C')
            city: Optional city to filter data for
        """
        if not SKLEARN_AVAILABLE:
            raise ImportError("scikit-learn is required")
        
        # Filter by city if specified
        if city:
            df = df[df['city'] == city].copy()
            if df.empty:
                raise ValueError(f"No data found for city: {city}")
        
        # Create features
        df_features = self._create_features(df)
        
        # Select feature columns
        feature_cols = [
            'hour_sin', 'hour_cos', 'day_sin', 'day_cos', 'month_sin', 'month_cos',
            'temp_lag_1', 'temp_lag_24', 'temp_lag_168',
            'temp_rolling_mean_24', 'temp_rolling_std_24'
        ]
        
        # Add available weather features
        if 'humidity_pct' in df_features.columns:
            feature_cols.append('humidity_pct')
        if 'pressure_hPa' in df_features.columns:
            feature_cols.append('pressure_hPa')
        if 'wind_speed_mps' in df_features.columns:
            feature_cols.append('wind_speed_mps')
        
        # Filter to available columns
        feature_cols = [col for col in feature_cols if col in df_features.columns]
        
        if not feature_cols:
            raise ValueError("No valid feature columns found")
        
        # Prepare data
        X = df_features[feature_cols].values
        y = df_features[target].values
        
        # Remove any NaN or inf values
        mask = ~(np.isnan(X).any(axis=1) | np.isnan(y) | np.isinf(X).any(axis=1) | np.isinf(y))
        X = X[mask]
        y = y[mask]
        
        if len(X) == 0:
            raise ValueError("No valid training data after cleaning")
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, shuffle=False
        )
        
        # Scale features
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        # Train model
        if self.model_type == "random_forest":
            self.model = RandomForestRegressor(
                n_estimators=100,
                max_depth=10,
                random_state=42,
                n_jobs=-1
            )
        elif self.model_type == "gradient_boosting":
            self.model = GradientBoostingRegressor(
                n_estimators=100,
                max_depth=5,
                random_state=42
            )
        elif self.model_type == "linear":
            self.model = LinearRegression()
        else:
            raise ValueError(f"Unknown model type: {self.model_type}")
        
        self.model.fit(X_train_scaled, y_train)
        
        # Evaluate
        y_pred_train = self.model.predict(X_train_scaled)
        y_pred_test = self.model.predict(X_test_scaled)
        
        train_mae = mean_absolute_error(y_train, y_pred_train)
        test_mae = mean_absolute_error(y_test, y_pred_test)
        train_r2 = r2_score(y_train, y_pred_train)
        test_r2 = r2_score(y_test, y_pred_test)
        
        self.feature_columns = feature_cols
        self.target_column = target
        self.is_trained = True
        
        return {
            'train_mae': train_mae,
            'test_mae': test_mae,
            'train_r2': train_r2,
            'test_r2': test_r2,
            'n_samples': len(X),
            'n_features': len(feature_cols)
        }
    
    def predict(self, df: pd.DataFrame, hours_ahead: int = 24) -> pd.DataFrame:
        """
        Predict future weather conditions.
        
        Args:
            df: Recent historical data (last 7 days recommended)
            hours_ahead: Number of hours to predict ahead
            
        Returns:
            DataFrame with predictions
        """
        if not self.is_trained:
            raise ValueError("Model must be trained before prediction")
        
        df = df.copy()
        df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
        df = df.sort_values('timestamp_utc')
        
        # Create features
        df_features = self._create_features(df)
        
        # Get last row as starting point
        last_row = df_features.iloc[-1:].copy()
        
        predictions = []
        current_time = df['timestamp_utc'].iloc[-1]
        
        for hour in range(1, hours_ahead + 1):
            # Create features for this prediction
            pred_time = current_time + timedelta(hours=hour)
            
            # Update time features
            last_row['timestamp_utc'] = pred_time
            last_row['hour'] = pred_time.hour
            last_row['day_of_year'] = pred_time.timetuple().tm_yday
            last_row['month'] = pred_time.month
            last_row['day_of_week'] = pred_time.weekday()
            
            # Update cyclical features
            last_row['hour_sin'] = np.sin(2 * np.pi * last_row['hour'] / 24)
            last_row['hour_cos'] = np.cos(2 * np.pi * last_row['hour'] / 24)
            last_row['day_sin'] = np.sin(2 * np.pi * last_row['day_of_year'] / 365)
            last_row['day_cos'] = np.cos(2 * np.pi * last_row['day_of_year'] / 365)
            last_row['month_sin'] = np.sin(2 * np.pi * last_row['month'] / 12)
            last_row['month_cos'] = np.cos(2 * np.pi * last_row['month'] / 12)
            
            # Prepare features
            X = last_row[self.feature_columns].values
            
            # Handle NaN
            X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
            
            # Scale and predict
            X_scaled = self.scaler.transform(X)
            prediction = self.model.predict(X_scaled)[0]
            
            # Update lag features for next prediction
            if 'temp_lag_1' in self.feature_columns:
                last_row['temp_lag_1'] = prediction
                last_row['temp_C'] = prediction  # Update current temp
            
            predictions.append({
                'timestamp_utc': pred_time,
                'predicted_' + self.target_column: prediction,
                'hours_ahead': hour
            })
        
        return pd.DataFrame(predictions)
    
    def predict_for_city(self, city: str, data_dir: Path, hours_ahead: int = 24) -> pd.DataFrame:
        """
        Load data and predict for a specific city.
        
        Args:
            city: City name
            data_dir: Directory containing historical data
            hours_ahead: Number of hours to predict
            
        Returns:
            DataFrame with predictions
        """
        # Load recent data (last 7 days)
        df = load_all_data(data_dir)
        if df.empty:
            raise ValueError(f"No data found in {data_dir}")
        
        df = df[df['city'] == city].copy()
        if df.empty:
            raise ValueError(f"No data found for city: {city}")
        
        df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
        df = df.sort_values('timestamp_utc')
        
        # Get last 7 days
        last_date = df['timestamp_utc'].max()
        start_date = last_date - timedelta(days=7)
        df_recent = df[df['timestamp_utc'] >= start_date].copy()
        
        if len(df_recent) < 24:
            raise ValueError(f"Insufficient recent data for {city}. Need at least 24 hours.")
        
        return self.predict(df_recent, hours_ahead=hours_ahead)


def train_and_predict(
    city: str,
    data_dir: Path,
    model_type: str = "random_forest",
    hours_ahead: int = 24
) -> Dict[str, Any]:
    """
    Train model and generate predictions for a city.
    
    Args:
        city: City name
        data_dir: Directory with historical data
        model_type: Model type ('random_forest', 'gradient_boosting', 'linear')
        hours_ahead: Hours to predict ahead
        
    Returns:
        Dictionary with model metrics and predictions
    """
    # Load data
    df = load_all_data(data_dir)
    if df.empty:
        raise ValueError(f"No data found in {data_dir}")
    
    # Train model
    predictor = WeatherPredictor(model_type=model_type)
    metrics = predictor.train(df, target="temp_C", city=city)
    
    # Generate predictions
    predictions = predictor.predict_for_city(city, data_dir, hours_ahead=hours_ahead)
    
    return {
        'city': city,
        'model_type': model_type,
        'metrics': metrics,
        'predictions': predictions
    }


if __name__ == "__main__":
    # Example usage
    data_dir = settings.processed_data_dir / "5year_historical"
    
    if data_dir.exists():
        print("Training model for Cincinnati...")
        result = train_and_predict("Cincinnati", data_dir, model_type="random_forest", hours_ahead=48)
        
        print(f"\nModel Performance:")
        print(f"  Train MAE: {result['metrics']['train_mae']:.2f}°C")
        print(f"  Test MAE: {result['metrics']['test_mae']:.2f}°C")
        print(f"  Train R²: {result['metrics']['train_r2']:.3f}")
        print(f"  Test R²: {result['metrics']['test_r2']:.3f}")
        
        print(f"\nPredictions for next 48 hours:")
        print(result['predictions'].head(10))
    else:
        print(f"Data directory not found: {data_dir}")
        print("Generate 5-year data first using: python3 scripts/generate_5year_stats.py")

