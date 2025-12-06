# Data Sources - Where We Get Weather Data

## Current Implementation: **API-Based (Not Scraping)**

### ✅ Primary Data Source: OpenWeatherMap API

**What we're using:**
- **Source**: OpenWeatherMap REST API
- **Method**: HTTP GET requests (API calls, not web scraping)
- **Endpoint**: `https://api.openweathermap.org/data/2.5/weather`
- **Authentication**: API key authentication
- **Data Format**: JSON responses

**How it works:**
```python
# From simple_end_to_end.py, fetch_openweather.py, fetch_many.py
url = "https://api.openweathermap.org/data/2.5/weather"
params = {
    "q": city,           # City name
    "appid": api_key,    # API key
    "units": "metric"    # Units
}
response = requests.get(url, params=params)
data = response.json()  # Structured JSON data
```

**Advantages:**
- ✅ Structured, reliable data
- ✅ Official API (legal, reliable)
- ✅ No HTML parsing needed
- ✅ Rate limits clearly defined
- ✅ Consistent data format
- ✅ Real-time data

**What we get:**
- Temperature, humidity, pressure
- Wind speed and direction
- Weather conditions (clouds, rain, etc.)
- Coordinates (lat/lon)
- Timestamps

### ✅ Secondary Data Source: NOAA API (Available but not actively used)

**Status**: Configured and ready, but not in the main pipeline yet

**What's available:**
- NOAA NCEI API (historical climate data)
- National Weather Service API (current forecasts)
- Requires API key: `(configured in .env)` (configured in .env)

**Implementation**: 
- Normalization function exists (`normalize_noaa()`)
- Not integrated into main pipeline yet

### ⚠️ Web Scraping: Example Only (Not Used)

**Status**: Code exists as an example, but **NOT used in the actual pipeline**

**File**: `playwright_example.py`

**What it does:**
- Demonstrates how to scrape JavaScript-rendered websites
- Uses Playwright to render pages
- Parses HTML with BeautifulSoup
- **This is just an example/template**

**Why not used:**
- APIs are more reliable and legal
- No need to parse HTML
- APIs provide structured data
- Scraping can violate Terms of Service
- APIs are faster and more efficient

## Data Flow

```
┌─────────────────────┐
│ OpenWeatherMap API  │  ← REST API (HTTP GET)
│  (REST Endpoint)    │     Not web scraping!
└──────────┬──────────┘
           │
           │ JSON Response
           ▼
┌─────────────────────┐
│   Our Pipeline      │
│  (fetch functions)  │
└──────────┬──────────┘
           │
           ├──► Normalize
           ├──► Validate
           ├──► Kafka (optional)
           └──► Parquet Files
```

## Comparison: API vs Scraping

| Aspect | API (What We Use) | Web Scraping (Not Used) |
|--------|-------------------|-------------------------|
| **Method** | HTTP GET to REST endpoint | Parse HTML from web pages |
| **Data Format** | Structured JSON | Unstructured HTML |
| **Reliability** | High (official API) | Low (HTML can change) |
| **Legal** | ✅ Legal (API terms) | ⚠️ May violate ToS |
| **Rate Limits** | Clear and documented | Unclear, risk of blocking |
| **Maintenance** | Low (API stable) | High (HTML changes) |
| **Speed** | Fast | Slower (page rendering) |
| **Error Handling** | Standard HTTP codes | Complex parsing errors |

## What We're Actually Doing

### ✅ API Integration (Current)
1. **Make HTTP request** to OpenWeatherMap API
2. **Receive JSON response** (structured data)
3. **Parse JSON** (not HTML)
4. **Normalize** to our schema
5. **Store** in Parquet/Kafka

### ❌ NOT Web Scraping
- We don't fetch HTML pages
- We don't parse HTML
- We don't use browser automation (except in example)
- We don't extract data from web pages

## Example: What an API Call Looks Like

```bash
# This is what we do (API call):
curl "https://api.openweathermap.org/data/2.5/weather?q=Kathmandu&appid=YOUR_KEY&units=metric"

# Response (JSON):
{
  "coord": {"lon": 85.324, "lat": 27.7172},
  "weather": [{"main": "Clouds", "description": "overcast clouds"}],
  "main": {
    "temp": 8.12,
    "pressure": 1019,
    "humidity": 100
  },
  ...
}
```

## If You Want to Add Scraping

The `playwright_example.py` file shows how to scrape, but you'd need to:

1. **Find a website** that allows scraping (check Terms of Service)
2. **Identify the HTML structure** (CSS selectors)
3. **Integrate into pipeline** (modify `simple_end_to_end.py`)
4. **Handle errors** (HTML can change)
5. **Respect rate limits** (don't overload servers)

**Recommendation**: Stick with APIs when possible - they're more reliable and legal.

## Summary

**Current Data Source**: 
- ✅ **OpenWeatherMap REST API** (primary)
- ✅ **NOAA API** (configured, ready to use)
- ❌ **Web Scraping** (example only, not used)

**Method**: API calls (HTTP GET requests), not web scraping

**Why**: APIs are more reliable, legal, and efficient than scraping

