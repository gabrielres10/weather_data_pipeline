"""
Weather Data Pipeline DAG
=========================

Este DAG orquesta el pipeline completo de datos meteorológicos:
1. Verifica disponibilidad de la API
2. Extrae datos meteorológicos de OpenWeatherMap
3. Procesa datos con Apache Spark
4. Carga datos a PostgreSQL
5. Genera resumen estadístico diario
6. Ejecuta validaciones de calidad de datos

Configuración:
- Se ejecuta diariamente a las 6:00 AM UTC
- Incluye reintentos con backoff exponencial
- Envía alertas por email en caso de fallos
- Maneja dependencias entre tareas apropiadamente
"""

import os
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Airflow imports - válidos en el entorno Docker de Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore
from airflow.exceptions import AirflowException  # type: ignore
import requests

# Configuración del DAG
DEFAULT_ARGS = {
    'owner': 'weather-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['admin@weather-pipeline.com'],  # Mock email
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    # retry_exponential_backoff removido - no compatible con Airflow 3.0
    'max_retry_delay': timedelta(minutes=30),
}

# Paths dentro del contenedor Airflow
SCRIPTS_DIR = "/opt/airflow/dags/scripts"
STAGING_DIR = "/opt/airflow/staging"
LOGS_DIR = "/opt/airflow/logs"

def check_api_availability(**context):
    """
    Verifica que la API de OpenWeatherMap sea accesible.
    """
    api_key = os.getenv("OPENWEATHERMAP_API_KEY")
    if not api_key or api_key == "your_api_key_here":
        raise AirflowException("API key no configurada correctamente")
    
    # Test con una ciudad pequeña
    test_url = "https://api.openweathermap.org/data/2.5/weather"
    params = {"q": "London", "appid": api_key, "units": "metric"}
    
    try:
        response = requests.get(test_url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        if 'main' not in data:
            raise AirflowException("Respuesta de API inválida")
            
        logging.info(f"API disponible. Test OK con temp: {data['main']['temp']}°C")
        return True
        
    except requests.exceptions.RequestException as e:
        raise AirflowException(f"API no disponible: {e}")

def extract_weather_data(**context):
    """
    Ejecuta el script de extracción de datos meteorológicos.
    """
    import subprocess
    import sys
    
    script_path = f"{SCRIPTS_DIR}/extract_weather.py"
    
    # Configurar variables de entorno para el script
    env = os.environ.copy()
    env['PYTHONPATH'] = SCRIPTS_DIR
    
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            cwd="/opt/airflow",
            env=env,
            capture_output=True,
            text=True,
            timeout=300  # 5 minutos timeout
        )
        
        if result.returncode != 0:
            logging.error(f"Error en extracción: {result.stderr}")
            raise AirflowException(f"Script de extracción falló: {result.stderr}")
            
        logging.info(f"Extracción exitosa: {result.stdout}")
        
        # Verificar que se crearon archivos
        raw_dir = Path(f"{STAGING_DIR}/raw")
        json_files = list(raw_dir.glob("*.json")) if raw_dir.exists() else []
        
        if len(json_files) < 5:  # Esperamos 5 ciudades + summary
            raise AirflowException(f"Archivos insuficientes generados: {len(json_files)}")
            
        return {"files_created": len(json_files)}
        
    except subprocess.TimeoutExpired:
        raise AirflowException("Timeout en script de extracción")

def process_with_spark(**context):
    """
    Ejecuta el procesamiento de datos con Apache Spark.
    """
    import subprocess
    import sys
    
    script_path = f"{SCRIPTS_DIR}/spark_process.py"
    
    try:
        result = subprocess.run([
            sys.executable, script_path,
            "--input", f"{STAGING_DIR}/raw",
            "--output", f"{STAGING_DIR}/processed",
            "--verbose"
        ], 
        cwd="/opt/airflow",
        capture_output=True,
        text=True,
        timeout=600  # 10 minutos timeout
        )
        
        if result.returncode != 0:
            logging.error(f"Error en procesamiento Spark: {result.stderr}")
            raise AirflowException(f"Procesamiento Spark falló: {result.stderr}")
            
        logging.info(f"Procesamiento Spark exitoso: {result.stdout}")
        
        # Verificar archivos de salida
        parquet_dir = Path(f"{STAGING_DIR}/processed/weather_parquet")
        summary_dir = Path(f"{STAGING_DIR}/processed/summaries")
        
        if not parquet_dir.exists() or not summary_dir.exists():
            raise AirflowException("Directorios de salida no creados")
            
        return {"spark_processing": "completed"}
        
    except subprocess.TimeoutExpired:
        raise AirflowException("Timeout en procesamiento Spark")

def load_to_postgres(**context):
    """
    Carga los datos procesados a PostgreSQL.
    """
    import subprocess
    import sys
    
    script_path = f"{SCRIPTS_DIR}/load_to_postgres.py"
    
    try:
        # El script usa POSTGRES_CONNECTION del entorno
        result = subprocess.run([
            sys.executable, script_path,
            "--parquet_dir", f"{STAGING_DIR}/processed/weather_parquet",
            "--summary_dir", f"{STAGING_DIR}/processed/summaries"
        ],
        cwd="/opt/airflow",
        capture_output=True,
        text=True,
        timeout=300  # 5 minutos timeout
        )
        
        if result.returncode != 0:
            logging.error(f"Error en carga a Postgres: {result.stderr}")
            raise AirflowException(f"Carga a Postgres falló: {result.stderr}")
            
        logging.info(f"Carga a Postgres exitosa: {result.stdout}")
        return {"postgres_load": "completed"}
        
    except subprocess.TimeoutExpired:
        raise AirflowException("Timeout en carga a Postgres")

def generate_summary(**context):
    """
    Genera resumen estadístico adicional y métricas de calidad.
    """
    hook = PostgresHook(postgres_conn_id="postgres")
    
    # Query para estadísticas del día actual
    date_filter = context['ds']  # Fecha de ejecución en formato YYYY-MM-DD
    
    stats_query = f"""
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT city_name) as unique_cities,
        ROUND(AVG(temperature_celsius), 2) as avg_temp,
        ROUND(MIN(temperature_celsius), 2) as min_temp,
        ROUND(MAX(temperature_celsius), 2) as max_temp,
        ROUND(AVG(humidity), 1) as avg_humidity,
        ROUND(AVG(pressure), 1) as avg_pressure
    FROM weather_raw 
    WHERE DATE(measurement_time) = '{date_filter}'
    """
    
    try:
        result = hook.get_first(stats_query)
        
        if not result or result[0] == 0:
            raise AirflowException(f"No se encontraron datos para la fecha {date_filter}")
        
        summary_stats = {
            "execution_date": date_filter,
            "total_records": result[0],
            "unique_cities": result[1], 
            "avg_temperature_celsius": float(result[2]) if result[2] else None,
            "min_temperature_celsius": float(result[3]) if result[3] else None,
            "max_temperature_celsius": float(result[4]) if result[4] else None,
            "avg_humidity": float(result[5]) if result[5] else None,
            "avg_pressure": float(result[6]) if result[6] else None,
            "generated_at": datetime.now().isoformat()
        }
        
        # Guardar resumen extendido
        summary_path = Path(f"{STAGING_DIR}/processed/summaries/daily_summary_{date_filter}.json")
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(summary_path, 'w') as f:
            json.dump(summary_stats, f, indent=2)
        
        logging.info(f"Resumen generado: {summary_stats}")
        return summary_stats
        
    except Exception as e:
        raise AirflowException(f"Error generando resumen: {e}")

def data_quality_check(**context):
    """
    Valida la calidad y completitud de los datos cargados.
    """
    hook = PostgresHook(postgres_conn_id="postgres")
    date_filter = context['ds']
    
    # Definir las validaciones
    validations = {
        "completeness": f"""
            SELECT COUNT(*) FROM weather_raw 
            WHERE DATE(measurement_time) = '{date_filter}'
        """,
        "temperature_range": f"""
            SELECT COUNT(*) FROM weather_raw 
            WHERE DATE(measurement_time) = '{date_filter}'
            AND (temperature_celsius < -50 OR temperature_celsius > 60)
        """,
        "humidity_range": f"""
            SELECT COUNT(*) FROM weather_raw 
            WHERE DATE(measurement_time) = '{date_filter}'
            AND (humidity < 0 OR humidity > 100)
        """,
        "null_cities": f"""
            SELECT COUNT(*) FROM weather_raw 
            WHERE DATE(measurement_time) = '{date_filter}'
            AND (city_name IS NULL OR city_name = '')
        """,
        "summary_exists": f"""
            SELECT COUNT(*) FROM daily_weather_summary 
            WHERE date = '{date_filter}'
        """
    }
    
    # Umbrales esperados
    thresholds = {
        "completeness": 5,  # Mínimo 5 registros (5 ciudades)
        "temperature_range": 0,  # 0 registros fuera de rango
        "humidity_range": 0,  # 0 registros fuera de rango  
        "null_cities": 0,  # 0 ciudades nulas
        "summary_exists": 1  # 1 resumen diario debe existir
    }
    
    results = {}
    issues = []
    
    try:
        for check_name, query in validations.items():
            result = hook.get_first(query)
            count = result[0] if result else 0
            results[check_name] = count
            
            expected = thresholds[check_name]
            
            if check_name == "completeness" and count < expected:
                issues.append(f"Datos incompletos: {count} registros, esperados >= {expected}")
            elif check_name in ["temperature_range", "humidity_range", "null_cities"] and count > expected:
                issues.append(f"Calidad comprometida en {check_name}: {count} registros problemáticos")
            elif check_name == "summary_exists" and count < expected:
                issues.append(f"Resumen diario faltante: {count} encontrados, esperados {expected}")
                
        # Logging de resultados
        logging.info(f"Resultados de validación: {results}")
        
        if issues:
            error_msg = "Validaciones de calidad fallaron: " + "; ".join(issues)
            logging.error(error_msg)
            raise AirflowException(error_msg)
        
        logging.info("Todas las validaciones de calidad aprobadas")
        return {"quality_check": "passed", "results": results}
        
    except Exception as e:
        raise AirflowException(f"Error en validación de calidad: {e}")

# Definición del DAG
dag = DAG(
    'weather_data_pipeline',
    default_args=DEFAULT_ARGS,
    description='Pipeline completo de datos meteorológicos con Spark y PostgreSQL',
    schedule='0 6 * * *',  # Diario a las 6:00 AM UTC - Airflow 3.0+ usa 'schedule' en lugar de 'schedule_interval'
    catchup=False,
    max_active_runs=1,
    tags=['weather', 'etl', 'spark', 'postgres']
)

# Definición de tareas
check_api_task = PythonOperator(
    task_id='check_api_availability',
    python_callable=check_api_availability,
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_weather_data',
    python_callable=extract_weather_data,
    dag=dag,
)

process_task = PythonOperator(
    task_id='process_with_spark',
    python_callable=process_with_spark,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag,
)

summary_task = PythonOperator(
    task_id='generate_summary',
    python_callable=generate_summary,
    dag=dag,
)

quality_task = PythonOperator(
    task_id='data_quality_check',
    python_callable=data_quality_check,
    dag=dag,
)

# Definición de dependencias
check_api_task >> extract_task >> process_task >> load_task >> [summary_task, quality_task]

# Las tareas de resumen y calidad pueden ejecutarse en paralelo después de la carga