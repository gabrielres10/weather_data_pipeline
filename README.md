# Weather Data Pipeline

A comprehensive ETL (Extract, Transform, Load) pipeline implementation that demonstrates modern data engineering practices. The system extracts me## Troubleshooting and Diagnostics

### Common Issues and Resolutions

| Issue Category | Error Symptoms | Recommended Solution |
|-------|----------|----------|
| **Authentication** | `OPENWEATHERMAP_API_KEY is not configured properly` | Verify `.env` file contains valid API key configuration |
| **File System Permissions** | `Permission denied: staging/raw/` | Execute `./fix-permissions.sh` to configure container access |
| **Container Build Process** | Java/Spark installation failures | Allocate minimum 4GB memory to Docker daemon |
| **Service Initialization** | Containers in restart loop | Examine service logs using `docker-compose logs` |
| **DAG Registration** | Pipeline absent from Airflow interface | Allow 30-second delay for DAG processor file scanning |
| **Task Execution** | Task failure indicators in interface | Access detailed error logs through task-specific log viewer | data from the OpenWeatherMap API, processes it using Apache Spark for scalable data transformation, stores results in PostgreSQL for persistent storage, and orchestrates the entire workflow using Apache Airflow within a containerized Docker environment.

## Quick Start Guide

### System Requirements
The following prerequisites are required for deployment:
1. **Docker Engine and Docker Compose** - Container orchestration platform
2. **OpenWeatherMap API Key** - Free registration available at https://openweathermap.org/api

### Installation and Configuration

**Step 1: Repository Setup**
```bash
git clone https://github.com/gabrielres10/weather_data_pipeline.git
cd weather_data_pipeline
```

**Step 2: Environment Configuration**
```bash
# Configure environment variables
echo "OPENWEATHERMAP_API_KEY=your_api_key_here" > .env
```
Note: Replace `your_api_key_here` with your registered OpenWeatherMap API key.

**Step 3: Container Deployment**
```bash
# Build custom Docker images and initialize services
docker-compose build
docker-compose up -d

# Configure file system permissions for container access
chmod +x fix-permissions.sh
./fix-permissions.sh
```

**Step 4: Pipeline Execution**
1. Access the Airflow web interface at http://localhost:8080
2. Authenticate using the default credentials:
   - Username: `airflow`
   - Password: `airflow`
3. Locate the `weather_data_pipeline` DAG in the interface
4. Execute the pipeline by triggering the DAG manually

The system will execute the complete data processing workflow automatically.

## Pipeline Architecture and Functionality

### System Architecture
```
OpenWeatherMap API → Spark Processing → PostgreSQL → Data Quality Validation
      ↓                    ↓               ↓              ↓
   Raw JSON          Parquet Files    Structured DB   Validation Reports
```

### Workflow Components
The pipeline implements a six-stage automated workflow:

1. **API Connectivity Validation**: Verifies OpenWeatherMap API accessibility and authentication
2. **Data Extraction**: Retrieves current meteorological data for five major metropolitan areas (New York, London, Tokyo, Sydney, São Paulo)
3. **Data Processing**: Transforms raw JSON data using Apache Spark with PySpark interface (Java 17 runtime with Spark 3.5)
4. **Data Persistence**: Loads processed data into PostgreSQL using upsert operations for data consistency
5. **Statistical Analysis**: Generates daily aggregated summaries and statistical reports
6. **Quality Assurance**: Performs automated data validation and completeness verification

### System Capabilities
- **Automated Scheduling**: Configurable execution schedule (default: daily at 06:00 UTC)
- **Monitoring Infrastructure**: Real-time task monitoring through Airflow web interface
- **Data Storage**: Dual-layer storage with raw data preservation and structured analytical tables
- **Error Recovery**: Comprehensive retry mechanisms with exponential backoff strategies
- **Data Quality Management**: Built-in validation framework with configurable thresholds

## Technology Stack

The system utilizes the following technologies and frameworks:

- **Python 3.12**: Primary development language for all pipeline components
- **Apache Spark 3.5**: Distributed data processing engine with Java 17 runtime environment
- **PostgreSQL 13**: Relational database management system for structured data storage
- **Apache Airflow 3.0**: Workflow orchestration and scheduling platform
- **Docker Compose**: Container orchestration for multi-service deployment
- **OpenWeatherMap API**: External data source for meteorological information

## Project Structure

```
weather_data_pipeline/
├── docker-compose.yaml     # Container orchestration
├── Dockerfile             # Custom Airflow image with Java
├── fix-permissions.sh     # Docker permission fix script
├── .env                   # Environment variables (you create this)
├── db_setup.sql           # Database schema setup
├── requirements.txt       # Python dependencies
├── dags/                  # Airflow DAGs
│   ├── weather_pipeline_dag.py    # Main orchestration DAG
│   └── scripts/              # Pipeline scripts
│       ├── extract_weather.py     # Data extraction
│       ├── spark_process.py       # Spark processing
│       └── load_to_postgres.py    # Database loading
├── staging/               # Data storage (auto-created)
│   ├── raw/                  # Raw JSON from API
│   └── processed/            # Parquet files + summaries
├── logs/                  # Application logs (auto-created)
└── README.md              # This guide
```

## Data Flow and Output Structure

### Raw Data Extraction Layer
```bash
staging/raw/
├── New_York_20251008T120000Z.json     # New York meteorological data
├── London_20251008T120000Z.json       # London meteorological data
├── Tokyo_20251008T120000Z.json        # Tokyo meteorological data
├── Sydney_20251008T120000Z.json       # Sydney meteorological data
├── Sao_Paulo_20251008T120000Z.json    # São Paulo meteorological data
└── extraction_summary_20251008T120000Z.json  # Extraction process metadata
```

### Processed Data Transformation Layer
```bash
staging/processed/
├── weather_parquet/          # Columnar storage format for analytical queries
│   └── measurement_date=2025-10-08/
│       └── part-00000.parquet
└── summaries/                # Aggregated statistical summaries
    └── daily_summary_2025-10-08.json
```

### Database Schema (PostgreSQL)
```sql
-- Primary weather measurements table
weatherdb.weather_raw (
    id, city_name, country, temperature_celsius, 
    humidity, pressure, weather_description, measurement_time
)

-- Aggregated daily summary table  
weatherdb.daily_weather_summary (
    date, avg_temp_celsius, min_temp_celsius, max_temp_celsius,
    avg_humidity, avg_pressure, total_measurements
)
```

## Advanced Configuration

### Environment Variable Configuration (.env)
```bash
# Required Configuration
OPENWEATHERMAP_API_KEY=your_key_here

# Optional System Overrides
POSTGRES_HOST=postgres
POSTGRES_DB=weatherdb
LOG_LEVEL=INFO
AIRFLOW_UID=50000
```

### Container Management Commands
```bash
# Rebuild containers after configuration changes
docker-compose build --no-cache

# Monitor service logs in real-time
docker-compose logs -f airflow-scheduler

# Direct database access for administrative tasks
docker exec -it weather_data_pipeline-postgres-1 psql -U airflow -d weatherdb
```

## Troubleshooting

### Common Issues & Solutions

| Issue | Symptoms | Solution |
|-------|----------|----------|
| **Missing API Key** | `OPENWEATHERMAP_API_KEY is not configured properly` | Create `.env` file with your API key |
| **Permission Errors** | `Permission denied: staging/raw/` | Run `./fix-permissions.sh` |
| **Docker Build Fails** | Java/Spark installation errors | Ensure Docker has enough memory (4GB+) |
| **Services Won't Start** | Containers keep restarting | Check `docker-compose logs` for errors |
| **DAG Not Visible** | Pipeline doesn't appear in Airflow | Wait 30s for DAG processor to scan files |
| **Tasks Failing** | Red task boxes in Airflow | Click task → View Logs for detailed error |

### System Health Verification
```bash
# Verify container service status
docker-compose ps

# Monitor application logs continuously
docker-compose logs -f

# Validate database connectivity and data presence
docker exec weather_data_pipeline-postgres-1 psql -U airflow -d weatherdb -c "SELECT COUNT(*) FROM weather_raw;"

# Confirm Spark framework initialization
docker exec weather_data_pipeline-airflow-scheduler-1 python -c "from pyspark.sql import SparkSession; print('Spark Framework Operational')"
```

### Performance Optimization
```bash
# Resource allocation for memory-constrained environments
export SPARK_DRIVER_MEMORY=1g
export SPARK_EXECUTOR_MEMORY=1g

# Enable Docker BuildKit for improved build performance
export DOCKER_BUILDKIT=1
docker-compose build
```

### Development Environment Setup (Alternative)

For local development without containerization:

**System Requirements:**
- Python 3.9 or higher
- Java Development Kit 17 or higher (required for Spark operations)
- PostgreSQL database server (local installation)

**Environment Configuration:**
```bash
# Install Python package dependencies
pip install -r requirements.txt

# Initialize database schema
psql -U postgres -c "CREATE DATABASE weatherdb;"
psql -U postgres -d weatherdb -f db_setup.sql

# Configure runtime environment variables
export OPENWEATHERMAP_API_KEY=your_key_here
export POSTGRES_CONNECTION="postgresql://postgres:password@localhost:5432/weatherdb"
```

**Individual Component Execution:**
```bash
# Execute data extraction module
python dags/scripts/extract_weather.py

# Execute Spark data processing module
python dags/scripts/spark_process.py

# Execute database persistence module
python dags/scripts/load_to_postgres.py
```

## Monitoring and Data Analysis

### Airflow Interface Capabilities
- **Graph View**: Visual representation of task dependencies and workflow structure
- **Tree View**: Historical execution status across multiple pipeline runs
- **Gantt Chart**: Temporal analysis of task execution duration and scheduling
- **Task Logs**: Comprehensive logging output for individual task troubleshooting
- **Variables**: Runtime configuration management and parameter adjustment

### Database Query Examples
```sql
-- Retrieve most recent meteorological measurements
SELECT city_name, temperature_celsius, measurement_time 
FROM weather_raw 
ORDER BY measurement_time DESC LIMIT 10;

-- Access daily aggregated statistics
SELECT date, avg_temp_celsius, total_measurements 
FROM daily_weather_summary 
ORDER BY date DESC;

-- Analyze temperature trends across cities
SELECT city_name, AVG(temperature_celsius) as avg_temp
FROM weather_raw 
WHERE measurement_time >= NOW() - INTERVAL '7 days'
GROUP BY city_name;
```

### Programmatic Data Analysis
```python
# Establish database connection using pandas and psycopg
import pandas as pd
import psycopg

connection = psycopg.connect("postgresql://airflow:airflow@localhost:5432/weatherdb")
dataframe = pd.read_sql("SELECT * FROM weather_raw", connection)

# Generate descriptive statistics
print(dataframe.describe())
print(dataframe.groupby('city_name')['temperature_celsius'].mean())
```

## System Customization

### Expanding Geographic Coverage
Modify the city configuration in `dags/scripts/extract_weather.py`:
```python
CITIES = [
    "New York,US",
    "London,GB", 
    "Tokyo,JP",
    "Sydney,AU",
    "Sao Paulo,BR",
    "Paris,FR",        # Additional metropolitan areas
    "Berlin,DE"
]
```

### Schedule Configuration
Adjust execution frequency in `dags/weather_pipeline_dag.py`:
```python
dag = DAG(
    'weather_data_pipeline',
    schedule='0 12 * * *',  # Modified to execute at noon UTC
    # schedule='@hourly',   # Alternative: hourly execution
    ...
)
```

### Data Transformation Extensions
Enhance Spark processing in `dags/scripts/spark_process.py`:
```python
# Implementation example: Temperature classification system
df_with_categories = df.withColumn(
    "temp_category",
    when(col("temperature_celsius") < 0, "freezing")
    .when(col("temperature_celsius") < 20, "cold")
    .otherwise("warm")
)
```

## Architecture Design

### Container Service Architecture
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

### Data Processing Workflow
1. **API Data Extraction** → Timestamped JSON file generation
2. **Spark Data Processing** → Schema validation and data transformation
3. **Parquet File Storage** → Columnar format with temporal partitioning
4. **PostgreSQL Data Loading** → Structured table population with upsert operations
5. **Data Quality Validation** → Completeness verification and anomaly detection

## Technical References

### Technology Documentation
- **Apache Airflow**: [Official Documentation](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/index.html)
- **Apache Spark**: [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
- **Docker Compose**: [Configuration Reference](https://docs.docker.com/compose/)
- **PostgreSQL**: [Database Tutorial](https://www.postgresql.org/docs/current/tutorial.html)

### Extension Opportunities
- Integration of additional meteorological data sources and sensor networks
- Implementation of predictive analytics and machine learning models
- Development of real-time data visualization and dashboard interfaces
- Addition of streaming data processing capabilities for continuous monitoring
- Export functionality to cloud-based data warehouse solutions

## Contributing

### Development Process

1. Fork the repository to your GitHub account
2. Create a feature branch: `git checkout -b feature/descriptive-name`
3. Implement your modifications with appropriate testing
4. Validate changes using: `docker-compose up --build`
5. Submit a pull request with detailed description

### Contribution Areas
- Integration of additional meteorological data sources
- Enhancement of data quality validation mechanisms
- Performance optimization and resource utilization improvements
- Support for additional data export formats (CSV, JSON, XML)
- Development of data visualization and dashboard integrations

## License and Attribution

**License**: MIT License - refer to LICENSE file for complete terms

**Technology Stack Attribution:**
- OpenWeatherMap API for meteorological data sourcing
- Apache Spark for distributed data processing capabilities
- Apache Airflow for workflow orchestration and scheduling
- PostgreSQL for relational database management
- Docker for application containerization and deployment

---

### Command Reference

```bash
# Initialize pipeline services
docker-compose up -d

# Terminate pipeline services
docker-compose down

# Monitor application logs
docker-compose logs -f

# Rebuild containers after modifications
docker-compose build --no-cache

# Configure file system permissions
./fix-permissions.sh

# Access database interface
docker exec -it weather_data_pipeline-postgres-1 psql -U airflow -d weatherdb

# Access web-based monitoring interface
open http://localhost:8080
```

This implementation provides a comprehensive, production-ready weather data processing pipeline suitable for educational and professional applications.
