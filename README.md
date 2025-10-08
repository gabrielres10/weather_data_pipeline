# Weather Data Pipeline

A complete weather data ETL pipeline that extracts data from OpenWeatherMap API, processes it with Apache Spark, stores it in PostgreSQL, and orchestrates everything with Apache Airflow - all running in Docker containers.

## 🚀 Quick Start (5 minutes)

### Prerequisites
**Only 2 things you need:**
1. **Docker & Docker Compose** installed on your machine
2. **Free OpenWeatherMap API key** (get it [here](https://openweathermap.org/api))

### Step 1: Clone and Setup
```bash
git clone https://github.com/gabrielres10/weather_data_pipeline.git
cd weather_data_pipeline
```

### Step 2: Configure API Key
```bash
# Create .env file with your API key
echo "OPENWEATHERMAP_API_KEY=your_api_key_here" > .env
```
> Replace `your_api_key_here` with your actual API key from OpenWeatherMap

### Step 3: Start Everything
```bash
# Build and start all services (this will take a few minutes the first time)
docker-compose build
docker-compose up -d

# Fix file permissions for Docker
chmod +x fix-permissions.sh
./fix-permissions.sh
```

### Step 4: Access Airflow and Run Pipeline
1. **Open Airflow UI**: http://localhost:8080
2. **Login**: 
   - Username: `airflow`
   - Password: `airflow`
3. **Find the DAG**: Look for `weather_data_pipeline` in the DAGs list
4. **Run it**: Click the "Trigger DAG" button ▶️

**That's it!** The complete pipeline will run automatically.

## 🎯 What the Pipeline Does

### Architecture Overview
```
OpenWeatherMap API → Spark Processing → PostgreSQL → Data Quality Checks
      ↓                    ↓               ↓              ↓
   Raw JSON          Parquet Files    Structured DB   Validation Reports
```

### Pipeline Steps (All Automated)
1. **🌐 API Check**: Validates OpenWeatherMap API access
2. **📥 Data Extraction**: Fetches weather for 5 cities (NYC, London, Tokyo, Sydney, São Paulo)
3. **⚡ Spark Processing**: Transforms data with PySpark (includes Java 17 + Spark 3.5)
4. **💾 PostgreSQL Load**: Stores processed data with upsert logic
5. **📊 Summary Generation**: Creates daily statistics and reports
6. **✅ Quality Validation**: Checks data completeness and accuracy

### What You Get
- **Scheduled Runs**: Daily at 6 AM UTC (configurable)
- **Real-time Monitoring**: Airflow UI shows all task statuses
- **Data Storage**: PostgreSQL with raw weather data + daily summaries
- **Error Handling**: Automatic retries and detailed logging
- **Data Quality**: Built-in validation checks

## �️ Technical Stack

- **🐍 Python 3.12**: Core language
- **⚡ Apache Spark 3.5**: Data processing (with Java 17)
- **🗄️ PostgreSQL 13**: Data storage
- **🌬️ Apache Airflow 3.0**: Workflow orchestration
- **🐳 Docker Compose**: Container orchestration
- **🌤️ OpenWeatherMap API**: Weather data source

## �📁 Project Structure

```
weather_data_pipeline/
├── 🐳 docker-compose.yaml     # Container orchestration
├── 🐳 Dockerfile             # Custom Airflow image with Java
├── 🔧 fix-permissions.sh     # Docker permission fix script
├── 🌍 .env                   # Environment variables (you create this)
├── 📊 db_setup.sql           # Database schema setup
├── 📋 requirements.txt       # Python dependencies
├── 🗂️ dags/                  # Airflow DAGs
│   ├── weather_pipeline_dag.py    # Main orchestration DAG
│   └── scripts/              # Pipeline scripts
│       ├── extract_weather.py     # Data extraction
│       ├── spark_process.py       # Spark processing
│       └── load_to_postgres.py    # Database loading
├── 📁 staging/               # Data storage (auto-created)
│   ├── raw/                  # Raw JSON from API
│   └── processed/            # Parquet files + summaries
├── 📝 logs/                  # Application logs (auto-created)
└── 📖 README.md              # This guide
```

## � Data Flow & Output

### Raw Data (Extraction)
```bash
staging/raw/
├── New_York_20251008T120000Z.json     # NYC weather data
├── London_20251008T120000Z.json       # London weather data
├── Tokyo_20251008T120000Z.json        # Tokyo weather data
├── Sydney_20251008T120000Z.json       # Sydney weather data
├── Sao_Paulo_20251008T120000Z.json    # São Paulo weather data
└── extraction_summary_20251008T120000Z.json  # Extraction metadata
```

### Processed Data (Spark)
```bash
staging/processed/
├── weather_parquet/          # Columnar data for analytics
│   └── measurement_date=2025-10-08/
│       └── part-00000.parquet
└── summaries/                # Daily statistics
    └── daily_summary_2025-10-08.json
```

### Database Tables (PostgreSQL)
```sql
-- Raw weather measurements
weatherdb.weather_raw (
    id, city_name, country, temperature_celsius, 
    humidity, pressure, weather_description, measurement_time
)

-- Daily aggregated summaries  
weatherdb.daily_weather_summary (
    date, avg_temp_celsius, min_temp_celsius, max_temp_celsius,
    avg_humidity, avg_pressure, total_measurements
)
```

## 🔧 Advanced Configuration

### Custom Environment Variables (.env)
```bash
# Required
OPENWEATHERMAP_API_KEY=your_key_here

# Optional Overrides
POSTGRES_HOST=postgres
POSTGRES_DB=weatherdb
LOG_LEVEL=INFO
AIRFLOW_UID=50000
```

### Custom Docker Setup
```bash
# Rebuild after changes
docker-compose build --no-cache

# View logs
docker-compose logs -f airflow-scheduler

# Connect to database directly
docker exec -it weather_data_pipeline-postgres-1 psql -U airflow -d weatherdb
```

## � Troubleshooting

### Common Issues & Solutions

| Issue | Symptoms | Solution |
|-------|----------|----------|
| **Missing API Key** | `OPENWEATHERMAP_API_KEY is not configured properly` | Create `.env` file with your API key |
| **Permission Errors** | `Permission denied: staging/raw/` | Run `./fix-permissions.sh` |
| **Docker Build Fails** | Java/Spark installation errors | Ensure Docker has enough memory (4GB+) |
| **Services Won't Start** | Containers keep restarting | Check `docker-compose logs` for errors |
| **DAG Not Visible** | Pipeline doesn't appear in Airflow | Wait 30s for DAG processor to scan files |
| **Tasks Failing** | Red task boxes in Airflow | Click task → View Logs for detailed error |

### Health Checks
```bash
# Check all services are running
docker-compose ps

# View real-time logs
docker-compose logs -f

# Test database connection
docker exec weather_data_pipeline-postgres-1 psql -U airflow -d weatherdb -c "SELECT COUNT(*) FROM weather_raw;"

# Test Spark with Java
docker exec weather_data_pipeline-airflow-scheduler-1 python -c "from pyspark.sql import SparkSession; print('✅ Spark OK')"
```

### Performance Tuning
```bash
# For low-memory systems, reduce Spark resources
export SPARK_DRIVER_MEMORY=1g
export SPARK_EXECUTOR_MEMORY=1g

# For faster builds, use Docker BuildKit
export DOCKER_BUILDKIT=1
docker-compose build
```

### Manual Development Mode (Optional)

If you prefer running scripts individually without Docker:

**Prerequisites:**
- Python 3.9+
- Java 17+ (for Spark)
- PostgreSQL running locally

**Setup:**
```bash
# Install Python dependencies
pip install -r requirements.txt

# Set up database
psql -U postgres -c "CREATE DATABASE weatherdb;"
psql -U postgres -d weatherdb -f db_setup.sql

# Configure environment
export OPENWEATHERMAP_API_KEY=your_key_here
export POSTGRES_CONNECTION="postgresql://postgres:password@localhost:5432/weatherdb"
```

**Run individual scripts:**
```bash
# Extract weather data
python dags/scripts/extract_weather.py

# Process with Spark  
python dags/scripts/spark_process.py

# Load to database
python dags/scripts/load_to_postgres.py
```

## 📈 Monitoring & Analytics

### Airflow UI Features
- **Graph View**: Visual pipeline dependencies  
- **Tree View**: Historical run status
- **Gantt Chart**: Task timing analysis
- **Task Logs**: Detailed execution logs
- **Variables**: Runtime configuration

### Database Queries
```sql
-- Check latest data
SELECT city_name, temperature_celsius, measurement_time 
FROM weather_raw 
ORDER BY measurement_time DESC LIMIT 10;

-- Daily averages
SELECT date, avg_temp_celsius, total_measurements 
FROM daily_weather_summary 
ORDER BY date DESC;

-- Temperature trends
SELECT city_name, AVG(temperature_celsius) as avg_temp
FROM weather_raw 
WHERE measurement_time >= NOW() - INTERVAL '7 days'
GROUP BY city_name;
```

### Data Analysis
```python
# Connect with pandas
import pandas as pd
import psycopg

conn = psycopg.connect("postgresql://airflow:airflow@localhost:5432/weatherdb")
df = pd.read_sql("SELECT * FROM weather_raw", conn)

# Quick analysis
print(df.describe())
print(df.groupby('city_name')['temperature_celsius'].mean())
```

## 🔧 Customization

### Add More Cities
Edit `dags/scripts/extract_weather.py`:
```python
CITIES = [
    "New York,US",
    "London,GB", 
    "Tokyo,JP",
    "Sydney,AU",
    "Sao Paulo,BR",
    "Paris,FR",        # Add more cities
    "Berlin,DE"
]
```

### Change Schedule
Edit `dags/weather_pipeline_dag.py`:
```python
dag = DAG(
    'weather_data_pipeline',
    schedule='0 12 * * *',  # Change to run at noon UTC
    # schedule='@hourly',   # Or run every hour
    ...
)
```

### Modify Data Processing
Edit `dags/scripts/spark_process.py` to add custom transformations:
```python
# Example: Add temperature categories
df_with_categories = df.withColumn(
    "temp_category",
    when(col("temperature_celsius") < 0, "freezing")
    .when(col("temperature_celsius") < 20, "cold")
    .otherwise("warm")
)
```

## 🏗️ Architecture Deep Dive

### Container Architecture
```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Airflow       │    │   PostgreSQL     │    │   Data          │
│   Scheduler     │◄──►│   Database       │◄──►│   Volumes       │
│   (+ Java +     │    │   (weatherdb)    │    │   (staging/)    │
│    Spark)       │    │                  │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         ▲
         │
┌─────────────────┐    ┌──────────────────┐
│   Airflow       │    │   Airflow        │
│   Webserver     │    │   DAG Processor  │
│   (Port 8080)   │    │   (File Watcher) │
└─────────────────┘    └──────────────────┘
```

### Data Flow Details
1. **API Extraction** → Raw JSON files with timestamps
2. **Spark Processing** → Schema validation + transformations
3. **Parquet Storage** → Columnar format with date partitioning
4. **PostgreSQL Load** → Structured tables with upsert logic
5. **Quality Checks** → Data validation and alerting

## 📚 Learning Resources

### Understanding the Technologies
- **Airflow**: [Official Tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/index.html)
- **Spark**: [PySpark Guide](https://spark.apache.org/docs/latest/api/python/)
- **Docker**: [Docker Compose Docs](https://docs.docker.com/compose/)
- **PostgreSQL**: [Tutorial](https://www.postgresql.org/docs/current/tutorial.html)

### Extending the Pipeline
- Add more data sources (weather APIs, sensors)
- Implement machine learning predictions
- Create data visualization dashboards
- Add real-time streaming capabilities
- Export to cloud data warehouses

## 🤝 Contributing

**Found a bug or want to improve something?**

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Make your changes
4. Test with: `docker-compose up --build`
5. Submit a pull request

**Ideas for contributions:**
- Additional weather data sources
- More sophisticated data quality checks
- Performance optimizations
- Additional output formats (CSV, JSON)
- Integration with visualization tools

## 📄 License & Credits

**License**: MIT License - see LICENSE file for details

**Built with:**
- 🌤️ OpenWeatherMap API for weather data
- ⚡ Apache Spark for data processing
- 🌬️ Apache Airflow for orchestration  
- 🐘 PostgreSQL for data storage
- 🐳 Docker for containerization

---

### ⭐ Quick Commands Reference

```bash
# Start pipeline
docker-compose up -d

# Stop pipeline  
docker-compose down

# View logs
docker-compose logs -f

# Rebuild after changes
docker-compose build --no-cache

# Fix permissions
./fix-permissions.sh

# Connect to database
docker exec -it weather_data_pipeline-postgres-1 psql -U airflow -d weatherdb

# Access Airflow
open http://localhost:8080
```

**🎉 That's it! You now have a complete production-ready weather data pipeline!**
