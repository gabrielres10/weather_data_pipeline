# Weather Data Pipeline

A complete data pipeline that extracts weather data from OpenWeatherMap API, processes it with Apache Spark, and stores results in PostgreSQL, orchestrated with Apache Airflow.

## 🌟 Features

### Part 1: Data Extraction ✅
- **Multi-city weather extraction**: Fetches current weather data for 5 major cities
  - New York, USA
  - London, UK  
  - Tokyo, Japan
  - Sydney, Australia
  - São Paulo, Brazil
- **Robust retry logic**: Automatic retry with exponential backoff for failed API calls
- **Comprehensive logging**: Detailed logging with colored console output and file logging
- **Raw data staging**: Stores JSON responses with metadata in staging directory
- **Error handling**: Graceful handling of API errors, network issues, and validation

### Upcoming Parts
- **Part 2**: Data Processing with Apache Spark
- **Part 3**: Data Storage in PostgreSQL
- **Part 4**: Orchestration with Apache Airflow

## 📁 Project Structure

```
weather_data_pipeline/
├── extract_weather.py          # Main extraction script
├── requirements.txt            # Python dependencies
├── .env.example               # Environment variables template
├── README.md                  # This file
├── db_setup.sql              # Database setup scripts
├── staging/                   # Raw data storage (auto-created)
│   └── raw/                  # JSON files from API
└── logs/                     # Application logs (auto-created)
```

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- OpenWeatherMap API key (free at https://openweathermap.org/api)

### Installation

1. **Clone the repository**:
```bash
git clone <repository-url>
cd weather_data_pipeline
```

2. **Create and activate virtual environment**:
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install dependencies**:
```bash
pip install -r requirements.txt
```

4. **Configure API key**:
```bash
# Copy the example environment file
cp .env.example .env

# Edit .env file and add your API key
nano .env  # or use your preferred editor
```

Replace `your_api_key_here` with your actual OpenWeatherMap API key.

### Usage

**Run the weather extraction**:
```bash
python extract_weather.py
```

The script will:
- ✅ Validate your API key
- 🌍 Fetch weather data for all 5 cities
- 💾 Save raw JSON responses to `staging/raw/`
- 📊 Generate detailed logs in `logs/`
- 📈 Create an extraction summary

## 📊 Output

### Raw Data Files
- Location: `staging/raw/`
- Format: `{city_name}_{timestamp}.json`
- Content: Enhanced JSON with metadata and raw API response

### Logs
- Location: `logs/`
- Format: `extract_weather_{date}.log`
- Features: Colored console output, detailed file logging

### Example Output
```
2024-01-15 10:30:15 [INFO] ============================================================
2024-01-15 10:30:15 [INFO] Starting weather data extraction process
2024-01-15 10:30:15 [INFO] ============================================================
2024-01-15 10:30:15 [INFO] API key configured successfully
2024-01-15 10:30:15 [INFO] Cities to process: 5
2024-01-15 10:30:16 [INFO] ✓ Success: New York, US - 15.2°C, clear sky (1.23s)
2024-01-15 10:30:17 [INFO] ✓ Success: London, GB - 8.1°C, cloudy (0.98s)
...
2024-01-15 10:30:20 [INFO] Success rate: 100.0%
```

## 🔧 Configuration

### Environment Variables
- `OPENWEATHERMAP_API_KEY`: Your API key (required)
- `LOG_LEVEL`: Logging level (optional, default: INFO)

### Customization
- **Cities**: Edit the `CITIES` list in `extract_weather.py`
- **Retry Logic**: Modify retry parameters in the `@retry` decorator
- **Staging Directory**: Change `STAGING_DIR` path
- **API Parameters**: Adjust request parameters (units, language, etc.)

## 🛠️ Technical Details

### Retry Strategy
- **Max attempts**: 5
- **Wait strategy**: Exponential backoff (2s to 30s)
- **Retry conditions**: Network errors, API errors, timeouts

### Error Handling
- ✅ Invalid API key detection
- ✅ City not found handling
- ✅ Network timeout management  
- ✅ Connection error recovery
- ✅ Graceful failure logging

### Data Validation
- ✅ Response format validation
- ✅ Required fields checking
- ✅ Temperature data verification

## 🧪 Testing

Run a quick test:
```bash
# Test with debug logging
LOG_LEVEL=DEBUG python extract_weather.py
```

## 📈 Monitoring

Check extraction status:
```bash
# View recent logs
tail -f logs/extract_weather_$(date +%Y%m%d).log

# Check staging directory
ls -la staging/raw/

# View summary files
ls -la staging/raw/extraction_summary_*.json
```

## 🚨 Troubleshooting

### Common Issues

**API Key Error**:
```
ERROR: OPENWEATHERMAP_API_KEY is not configured properly
```
**Solution**: Set your API key in `.env` file

**Network Timeout**:
```
ERROR: Timeout occurred while fetching data
```
**Solution**: Check internet connection, script will auto-retry

**City Not Found**:
```
ERROR: City not found: {city_name}
```
**Solution**: Verify city name spelling in CITIES list

## 📋 Requirements

See `requirements.txt` for complete dependency list:
- `requests>=2.31` - HTTP client
- `tenacity>=8.2.0` - Retry logic  
- `python-dotenv>=1.0.0` - Environment variables
- Additional dependencies for future pipeline parts

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🌐 API Reference

- [OpenWeatherMap Current Weather API](https://openweathermap.org/current)
- [API Key Registration](https://openweathermap.org/api)
