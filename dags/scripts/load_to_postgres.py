import os
import glob
import json
import argparse
import logging
from pathlib import Path
import pandas as pd
import psycopg
from dotenv import load_dotenv

load_dotenv()

DEFAULT_PROCESSED_PARQUET = "staging/processed/weather_parquet"
DEFAULT_SUMMARY_DIR = "staging/processed/summaries"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("loader")

REQUIRED_TABLES = {"weather_raw", "daily_weather_summary"}

def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = ANY(%s)
            """,
            (list(REQUIRED_TABLES),)
        )
        existing = {r[0] for r in cur.fetchall()}

    if REQUIRED_TABLES.issubset(existing):
        logger.info("Esquema ya presente (tablas %s)", ", ".join(sorted(existing)))
        return False

    script_path = Path(__file__).resolve().parent / "db_setup.sql"
    if not script_path.exists():
        raise FileNotFoundError(f"No se encontró db_setup.sql en {script_path}")

    logger.info("Faltan tablas (%s). Ejecutando script de esquema: %s",
                ", ".join(sorted(REQUIRED_TABLES - existing)), script_path)
    sql_text = script_path.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql_text)
    logger.info("Esquema creado / actualizado")
    return True

def infer_connection(args_pg: str | None) -> str:
    if args_pg:
        return args_pg
    env_conn = os.getenv("POSTGRES_CONNECTION")
    if env_conn:
        logger.info("Using POSTGRES_CONNECTION from environment")
        return env_conn
    raise SystemExit("No Postgres connection string provided. Use --pg o variable POSTGRES_CONNECTION")


def load_summary_json(conn, summary_file: str):
    with open(summary_file, "r", encoding="utf-8") as fh:
        s = json.load(fh)

    temp_stats = s.get("temperature_statistics", {})
    hottest_list = s.get("extreme_temperatures", {}).get("hottest_cities", [])
    coldest_list = s.get("extreme_temperatures", {}).get("coldest_cities", [])

    processing_ts = s.get("processing_timestamp") or s.get("processing_time_utc")
    if not processing_ts:
        raise ValueError("Summary file missing processing timestamp field")
    date = pd.to_datetime(processing_ts).date()

    avg_temp = temp_stats.get("avg_celsius")
    max_temp = temp_stats.get("max_celsius")
    min_temp = temp_stats.get("min_celsius")
    hottest_city = hottest_list[0]["city"] if hottest_list else None
    coldest_city = coldest_list[0]["city"] if coldest_list else None

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO daily_weather_summary (
              date, avg_temperature_celsius, max_temperature_celsius, min_temperature_celsius, hottest_city, coldest_city
            ) VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (date) DO UPDATE SET
              avg_temperature_celsius = EXCLUDED.avg_temperature_celsius,
              max_temperature_celsius = EXCLUDED.max_temperature_celsius,
              min_temperature_celsius = EXCLUDED.min_temperature_celsius,
              hottest_city = EXCLUDED.hottest_city,
              coldest_city = EXCLUDED.coldest_city,
              created_at = CURRENT_TIMESTAMP
            """,
            (date, avg_temp, max_temp, min_temp, hottest_city, coldest_city),
        )
    logger.info("Upserted summary for date %s", date)


def load_parquet_weather(conn, parquet_dir: str):
    path = Path(parquet_dir)
    if not path.exists():
        logger.warning("Parquet dir %s no existe, omitiendo carga weather_raw", parquet_dir)
        return 0

    files = list(path.glob("date=*/part-*.parquet"))
    if not files:
        logger.warning("No se encontraron archivos parquet en %s", parquet_dir)
        return 0

    df_list = [pd.read_parquet(f) for f in files]
    df = pd.concat(df_list, ignore_index=True)
    logger.info("Leídos %d registros desde parquet", len(df))

    expected_cols = {
        "city_name",
        "country_code",
        "temperature_celsius",
        "temperature_fahrenheit",
        "feels_like_celsius",
        "humidity",
        "pressure",
        "weather_description",
        "weather_main",
        "measurement_time",
        "processing_time",
    }
    missing = expected_cols - set(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas esperadas en parquet: {missing}")

    # Upsert por lotes
    records = df.to_dict(orient="records")
    upsert_sql = """
        INSERT INTO weather_raw (
          city_name, country_code, temperature_celsius, temperature_fahrenheit,
          humidity, pressure, weather_description, measurement_time, processing_time, unique_run_key
        ) VALUES (
          %(city_name)s, %(country_code)s, %(temperature_celsius)s, %(temperature_fahrenheit)s,
          %(humidity)s, %(pressure)s, %(weather_description)s, %(measurement_time)s, %(processing_time)s, %(unique_run_key)s
        )
        ON CONFLICT (city_name, measurement_time) DO UPDATE SET
          temperature_celsius = EXCLUDED.temperature_celsius,
          temperature_fahrenheit = EXCLUDED.temperature_fahrenheit,
          humidity = EXCLUDED.humidity,
          pressure = EXCLUDED.pressure,
          weather_description = EXCLUDED.weather_description,
          processing_time = EXCLUDED.processing_time
    """

    for r in records:
        mt = r.get("measurement_time")
        city = r.get("city_name")
        r["unique_run_key"] = f"{city}_{mt}" if mt and city else None

    total = 0
    with conn.cursor() as cur:
        for chunk_start in range(0, len(records), 500):
            chunk = records[chunk_start:chunk_start + 500]
            cur.executemany(upsert_sql, chunk)
            total += len(chunk)
    logger.info("Upserted %d registros en weather_raw", total)
    return total

def main():
    parser = argparse.ArgumentParser(description="Carga datos procesados y resumen a PostgreSQL")
    parser.add_argument("--pg", help="Cadena conexión Postgres (si no usar POSTGRES_CONNECTION)")
    parser.add_argument("--parquet_dir", default=DEFAULT_PROCESSED_PARQUET, help="Directorio parquet procesado")
    parser.add_argument("--summary_dir", default=DEFAULT_SUMMARY_DIR, help="Directorio de summaries JSON")
    parser.add_argument("--skip_raw", action="store_true", help="No cargar weather_raw (solo summary)")
    parser.add_argument("--skip_summary", action="store_true", help="No cargar daily_weather_summary")
    args = parser.parse_args()

    conn_str = infer_connection(args.pg)
    with psycopg.connect(conn_str) as conn:
        try:
            created = ensure_schema(conn)
            if created:
                conn.commit()
        except Exception as e:
            logger.exception("Error asegurando esquema: %s", e)
            raise SystemExit(1)

        if not args.skip_raw:
            try:
                load_parquet_weather(conn, args.parquet_dir)
            except Exception as e:
                logger.exception("Error cargando parquet: %s", e)
                raise SystemExit(1)

        if not args.skip_summary:
            files = sorted(glob.glob(os.path.join(args.summary_dir, "summary_*.json")))
            if not files:
                logger.warning("No summary files found in %s", args.summary_dir)
            else:
                latest = files[-1]
                logger.info("Loading summary %s", latest)
                try:
                    load_summary_json(conn, latest)
                except Exception as e:
                    logger.exception("Error cargando summary: %s", e)
                    raise SystemExit(1)

        conn.commit()
    logger.info("Proceso de carga completado")

if __name__ == "__main__":
    main()
