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

### Part 2: Data Processing with Spark ✅
- **Apache Spark processing**: Scalable data transformation using PySpark
- **Temperature conversion**: Automatic conversion from Kelvin to Celsius and Fahrenheit
- **Data quality handling**: Robust filtering of missing or malformed data
- **Comprehensive transformations**: Extracts city, country, temperature, humidity, pressure, weather description, timestamps
- **Statistical analysis**: Calculates averages, extremes, and weather condition distributions
- **Parquet output**: Efficient columnar storage with date partitioning
- **Detailed reporting**: JSON summaries with comprehensive weather statistics

### Upcoming Parts
- **Part 3**: Data Storage in PostgreSQL
- **Part 4**: Orchestration with Apache Airflow

## 📁 Project Structure

```
weather_data_pipeline/
├── extract_weather.py          # Weather data extraction script
├── spark_process.py            # Apache Spark processing job
├── run_full_pipeline.py        # Complete pipeline runner
├── test_spark_process.py       # Spark processor testing utility
├── requirements.txt            # Python dependencies
├── .env.example               # Environment variables template
├── README.md                  # This file
├── db_setup.sql              # Database setup scripts
├── staging/                   # Data storage (auto-created)
│   ├── raw/                  # Raw JSON files from API
│   └── processed/            # Processed data
│       ├── weather_parquet/  # Parquet files (date partitioned)
│       └── summaries/        # Processing summaries (JSON)
└── logs/                     # Application logs (auto-created)
```

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Java 8+ (required for Apache Spark)
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

#### Option 1: Run Complete Pipeline
```bash
# Run both extraction and processing
python run_full_pipeline.py
```

#### Option 2: Run Individual Steps

**Step 1: Extract weather data**:
```bash
python extract_weather.py
```

**Step 2: Process with Spark**:
```bash
python spark_process.py
```

**With custom paths**:
```bash
python spark_process.py --input staging/raw --output staging/processed --verbose
```

The pipeline will:
- ✅ Validate your API key
- 🌍 Fetch weather data for all 5 cities
- 💾 Save raw JSON responses to `staging/raw/`
- ⚡ Process data with Apache Spark
- 📊 Generate Parquet files with date partitioning
- 📈 Create detailed processing summaries
- 📝 Generate comprehensive logs

## 📊 Output

### Raw Data Files (Part 1)
- **Location**: `staging/raw/`
- **Format**: `{city_name}_{timestamp}.json`
- **Content**: Enhanced JSON with metadata and raw API response

### Processed Data Files (Part 2)
- **Parquet Files**: `staging/processed/weather_parquet/`
  - Date-partitioned columnar format
  - Optimized for analytics queries
  - Contains: city, country, temperatures (C/F), humidity, pressure, weather description, timestamps
- **Summary Reports**: `staging/processed/summaries/`
  - Comprehensive JSON reports with statistics
  - Temperature analytics (avg, min, max, stddev)
  - Extreme weather identification
  - Weather condition distributions

### Logs
- **Location**: `logs/`
- **Format**: `extract_weather_{date}.log`
- **Features**: Colored console output, detailed file logging

### Example Output

#### Extraction (Part 1)
```
============================================================
Starting weather data extraction process
============================================================
2024-01-15 10:30:15 [INFO] API key configured successfully
2024-01-15 10:30:15 [INFO] Cities to process: 5
2024-01-15 10:30:16 [INFO] ✓ Success: New York, US - 15.2°C, clear sky (1.23s)
2024-01-15 10:30:17 [INFO] ✓ Success: London, GB - 8.1°C, cloudy (0.98s)
...
2024-01-15 10:30:20 [INFO] Success rate: 100.0%
```

#### Spark Processing (Part 2)
```
============================================================
Starting Spark weather data processing
============================================================
Found 6 JSON files to process
Detected enhanced JSON format with metadata
Initial records read: 5
Valid records for processing: 5
✓ Successfully wrote Parquet files

============================================================
PROCESSING SUMMARY
============================================================
Records processed: 5
Average temperature: 14.6°C
Temperature range: 8.1°C to 22.5°C
Hottest city: Tokyo, JP (22.5°C)
Coldest city: London, GB (8.1°C)
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

### Part 1: Data Extraction
#### Retry Strategy
- **Max attempts**: 5
- **Wait strategy**: Exponential backoff (2s to 30s)  
- **Retry conditions**: Network errors, API errors, timeouts

#### Error Handling
- ✅ Invalid API key detection
- ✅ City not found handling
- ✅ Network timeout management  
- ✅ Connection error recovery
- ✅ Graceful failure logging

#### Data Validation
- ✅ Response format validation
- ✅ Required fields checking
- ✅ Temperature data verification

### Part 2: Spark Processing
#### Data Transformations
- **Temperature conversion**: Smart detection of Kelvin vs Celsius input
- **Unit standardization**: Converts to both Celsius and Fahrenheit
- **Field extraction**: City, country, weather metrics, descriptions
- **Timestamp handling**: Processing and measurement timestamps

#### Data Quality
- **Missing data handling**: Graceful filtering of incomplete records
- **Range validation**: Temperature sanity checks (-100°C to 60°C)
- **Type casting**: Proper data type conversion for all fields
- **Duplicate handling**: Natural deduplication through processing

#### Output Optimization
- **Parquet format**: Efficient columnar storage
- **Date partitioning**: Organized by measurement date
- **Compression**: Built-in Parquet compression
- **Schema evolution**: Supports future field additions

#### Performance Features
- **Adaptive query execution**: Spark 3.x optimizations
- **Partition coalescing**: Reduces small file problems
- **Lazy evaluation**: Optimized execution plans
- **Memory management**: Configurable Spark settings

## 🧪 Testing

### Test Extraction
```bash
# Test extraction with debug logging
LOG_LEVEL=DEBUG python extract_weather.py
```

### Test Spark Processing
```bash
# Test Spark processor with sample data
python test_spark_process.py

# Test processing with existing data
python spark_process.py --verbose
```

### Test Complete Pipeline
```bash
# Run full pipeline test
python run_full_pipeline.py
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
