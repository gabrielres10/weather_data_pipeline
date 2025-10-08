#!/usr/bin/env python3
"""
spark_process.py
Apache Spark job for processing weather data from OpenWeatherMap API.

This script processes raw JSON weather data and performs the following transformations:
- Converts temperature units (Kelvin to Celsius and Fahrenheit)
- Extracts relevant fields (city, country, temperature, humidity, pressure, etc.)
- Adds processing timestamps
- Calculates comprehensive weather statistics
- Handles missing or malformed data gracefully
- Outputs processed data in Parquet format with date partitioning
- Generates detailed summary reports in JSON format

Requirements: pyspark, pyarrow
"""

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession, functions as F, types as T

def build_spark():
    """
    Create and configure Spark session for weather data processing.
    """
    spark = (SparkSession.builder
             .appName("WeatherDataProcessor")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .getOrCreate())
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def validate_input_path(input_path):
    """
    Validate that input path exists and contains JSON files.
    """
    input_dir = Path(input_path)
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory does not exist: {input_path}")
    
    json_files = list(input_dir.glob("*.json"))
    if not json_files:
        raise FileNotFoundError(f"No JSON files found in: {input_path}")
    
    print(f"Found {len(json_files)} JSON files to process")
    return json_files

def create_output_directories(output_path):
    """
    Create necessary output directories.
    """
    output_dir = Path(output_path)
    parquet_dir = output_dir / "weather_parquet"
    summaries_dir = output_dir / "summaries"
    
    parquet_dir.mkdir(parents=True, exist_ok=True)
    summaries_dir.mkdir(parents=True, exist_ok=True)
    
    return str(parquet_dir), str(summaries_dir)

def process_weather_data(spark, input_path, output_path):
    """
    Main processing function for weather data.
    """
    print("=" * 60)
    print("Starting Spark weather data processing")
    print("=" * 60)
    
    # Validate inputs and create outputs
    json_files = validate_input_path(input_path)
    parquet_dir, summaries_dir = create_output_directories(output_path)
    
    print(f"Input path: {input_path}")
    print(f"Output path: {output_path}")
    print(f"Processing {len(json_files)} files")
    
    # Read JSON files - handle both direct API responses and enhanced format
    try:
        # Try reading as enhanced format first (with metadata wrapper)
        df_raw = spark.read.option("multiLine", "true").json(f"{input_path}/*.json")
        
        # Check if we have the enhanced format with metadata
        if "raw_data" in df_raw.columns:
            print("Detected enhanced JSON format with metadata")
            df = df_raw.select("raw_data.*")
        else:
            print("Detected direct API response format")
            df = df_raw
            
    except Exception as e:
        print(f"Error reading JSON files: {e}")
        raise
    
    print(f"Initial records read: {df.count()}")
    
    # Show schema for debugging
    print("\nData schema:")
    df.printSchema()
    
    # Extract and transform data with comprehensive error handling
    processed = df.select(
        # City information
        F.coalesce(F.col("name"), F.lit("unknown")).alias("city_name"),
        F.coalesce(F.col("sys.country"), F.lit("")).alias("country_code"),
        
        # Temperature data (handle both Kelvin and Celsius from API)
        F.coalesce(F.col("main.temp"), F.lit(0.0)).cast("double").alias("temp_raw"),
        
        # Other weather metrics
        F.coalesce(F.col("main.humidity"), F.lit(0)).cast("int").alias("humidity"),
        F.coalesce(F.col("main.pressure"), F.lit(0)).cast("int").alias("pressure"),
        F.coalesce(F.col("main.feels_like"), F.lit(0.0)).cast("double").alias("feels_like_raw"),
        
        # Weather description
        F.coalesce(F.expr("weather[0].description"), F.lit("")).alias("weather_description"),
        F.coalesce(F.expr("weather[0].main"), F.lit("")).alias("weather_main"),
        
        # Timestamps
        F.from_unixtime(F.coalesce(F.col("dt"), F.lit(0))).cast("timestamp").alias("measurement_time"),
        F.current_timestamp().alias("processing_time")
    )
    
    # Data quality checks and temperature conversion
    # OpenWeatherMap returns Celsius when units=metric, Kelvin by default
    processed = processed.withColumn(
        "is_celsius", 
        F.when(F.col("temp_raw") < 100, True).otherwise(False)  # Heuristic: < 100 likely Celsius
    )
    
    # Convert temperatures appropriately
    processed = processed.withColumn(
        "temperature_celsius",
        F.when(F.col("is_celsius"), F.round(F.col("temp_raw"), 2))
         .otherwise(F.round(F.col("temp_raw") - 273.15, 2))
    ).withColumn(
        "temperature_fahrenheit", 
        F.round(F.col("temperature_celsius") * 9.0/5.0 + 32.0, 2)
    ).withColumn(
        "feels_like_celsius",
        F.when(F.col("is_celsius"), F.round(F.col("feels_like_raw"), 2))
         .otherwise(F.round(F.col("feels_like_raw") - 273.15, 2))
    )
    
    # Data quality filtering
    valid_data = processed.filter(
        (F.col("temperature_celsius").isNotNull()) &
        (F.col("temperature_celsius") > -100) &  # Reasonable temperature range
        (F.col("temperature_celsius") < 60) &
        (F.col("city_name") != "unknown")
    )
    
    invalid_count = processed.count() - valid_data.count()
    if invalid_count > 0:
        print(f"Filtered out {invalid_count} invalid records")
    
    print(f"Valid records for processing: {valid_data.count()}")
    
    # Select final columns
    final_df = valid_data.select(
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
        "processing_time"
    )
    
    # Add date partition column
    final_df = final_df.withColumn("date", F.to_date("measurement_time"))
    
    # Write processed data to Parquet
    print(f"\nWriting processed data to: {parquet_dir}")
    try:
        final_df.write.mode("overwrite").partitionBy("date").parquet(parquet_dir)
        print("✓ Successfully wrote Parquet files")
    except Exception as e:
        print(f"✗ Error writing Parquet files: {e}")
        raise
    
    return final_df

def calculate_statistics(df):
    """
    Calculate comprehensive weather statistics.
    """
    print("\nCalculating weather statistics...")
    
    # Basic temperature statistics
    temp_stats = df.agg(
        F.round(F.avg("temperature_celsius"), 2).alias("avg_temperature_celsius"),
        F.max("temperature_celsius").alias("max_temperature_celsius"),
        F.min("temperature_celsius").alias("min_temperature_celsius"),
        F.round(F.stddev("temperature_celsius"), 2).alias("stddev_temperature_celsius"),
        F.count("*").alias("total_records")
    ).collect()[0]
    
    # Find cities with extreme temperatures
    hottest_cities = (df.filter(F.col("temperature_celsius") == temp_stats["max_temperature_celsius"])
                       .select("city_name", "country_code", "temperature_celsius", "weather_description")
                       .collect())
    
    coldest_cities = (df.filter(F.col("temperature_celsius") == temp_stats["min_temperature_celsius"])
                       .select("city_name", "country_code", "temperature_celsius", "weather_description")
                       .collect())
    
    # Additional statistics
    humidity_stats = df.agg(
        F.round(F.avg("humidity"), 1).alias("avg_humidity"),
        F.max("humidity").alias("max_humidity"),
        F.min("humidity").alias("min_humidity")
    ).collect()[0]
    
    pressure_stats = df.agg(
        F.round(F.avg("pressure"), 1).alias("avg_pressure"),
        F.max("pressure").alias("max_pressure"),
        F.min("pressure").alias("min_pressure")
    ).collect()[0]
    
    # Weather condition distribution
    weather_distribution = (df.groupBy("weather_main")
                             .count()
                             .orderBy(F.desc("count"))
                             .collect())
    
    return {
        "temperature_statistics": {
            "avg_celsius": float(temp_stats["avg_temperature_celsius"]) if temp_stats["avg_temperature_celsius"] else None,
            "max_celsius": float(temp_stats["max_temperature_celsius"]) if temp_stats["max_temperature_celsius"] else None,
            "min_celsius": float(temp_stats["min_temperature_celsius"]) if temp_stats["min_temperature_celsius"] else None,
            "stddev_celsius": float(temp_stats["stddev_temperature_celsius"]) if temp_stats["stddev_temperature_celsius"] else None,
        },
        "extreme_temperatures": {
            "hottest_cities": [
                {
                    "city": row["city_name"],
                    "country": row["country_code"], 
                    "temperature_celsius": float(row["temperature_celsius"]),
                    "description": row["weather_description"]
                } for row in hottest_cities
            ],
            "coldest_cities": [
                {
                    "city": row["city_name"],
                    "country": row["country_code"],
                    "temperature_celsius": float(row["temperature_celsius"]),
                    "description": row["weather_description"]
                } for row in coldest_cities
            ]
        },
        "humidity_statistics": {
            "avg_humidity": float(humidity_stats["avg_humidity"]) if humidity_stats["avg_humidity"] else None,
            "max_humidity": int(humidity_stats["max_humidity"]) if humidity_stats["max_humidity"] else None,
            "min_humidity": int(humidity_stats["min_humidity"]) if humidity_stats["min_humidity"] else None,
        },
        "pressure_statistics": {
            "avg_pressure": float(pressure_stats["avg_pressure"]) if pressure_stats["avg_pressure"] else None,
            "max_pressure": int(pressure_stats["max_pressure"]) if pressure_stats["max_pressure"] else None,
            "min_pressure": int(pressure_stats["min_pressure"]) if pressure_stats["min_pressure"] else None,
        },
        "weather_conditions": [
            {"condition": row["weather_main"], "count": row["count"]} 
            for row in weather_distribution
        ],
        "total_records_processed": int(temp_stats["total_records"]),
        "processing_timestamp": datetime.now(timezone.utc).isoformat()
    }

def save_summary(summary_data, summaries_dir):
    """
    Save processing summary to JSON file.
    """
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    summary_file = Path(summaries_dir) / f"summary_{timestamp}.json"
    
    try:
        with open(summary_file, "w", encoding="utf-8") as f:
            json.dump(summary_data, f, indent=2, ensure_ascii=False)
        
        print(f"✓ Summary saved to: {summary_file}")
        return str(summary_file)
    except Exception as e:
        print(f"✗ Error saving summary: {e}")
        raise

def main(args):
    """
    Main processing pipeline.
    """
    spark = build_spark()
    
    try:
        # Process the data
        processed_df = process_weather_data(spark, args.input, args.output)
        
        # Calculate statistics
        statistics = calculate_statistics(processed_df)
        
        # Save summary
        summaries_dir = Path(args.output) / "summaries"
        summary_file = save_summary(statistics, summaries_dir)
        
        # Print summary
        print("\n" + "=" * 60)
        print("PROCESSING SUMMARY")
        print("=" * 60)
        
        temp_stats = statistics["temperature_statistics"]
        print(f"Records processed: {statistics['total_records_processed']}")
        print(f"Average temperature: {temp_stats['avg_celsius']:.1f}°C")
        print(f"Temperature range: {temp_stats['min_celsius']:.1f}°C to {temp_stats['max_celsius']:.1f}°C")
        
        # Show extreme cities
        hottest = statistics["extreme_temperatures"]["hottest_cities"]
        coldest = statistics["extreme_temperatures"]["coldest_cities"]
        
        if hottest:
            city = hottest[0]
            print(f"Hottest city: {city['city']}, {city['country']} ({city['temperature_celsius']:.1f}°C)")
        
        if coldest:
            city = coldest[0]
            print(f"Coldest city: {city['city']}, {city['country']} ({city['temperature_celsius']:.1f}°C)")
        
        print(f"\nDetailed summary: {summary_file}")
        print("=" * 60)
        
    except Exception as e:
        print(f"Processing failed: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process weather data with Apache Spark",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python spark_process.py
  python spark_process.py --input staging/raw --output staging/processed
  python spark_process.py --input /path/to/json/files --output /path/to/output
        """
    )
    parser.add_argument(
        "--input", 
        default="staging/raw", 
        help="Directory containing raw JSON weather files (default: staging/raw)"
    )
    parser.add_argument(
        "--output", 
        default="staging/processed", 
        help="Output directory for processed data (default: staging/processed)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output"
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        import logging
        logging.basicConfig(level=logging.INFO)
    
    try:
        main(args)
        print("\n🎉 Weather data processing completed successfully!")
    except Exception as e:
        print(f"\n❌ Processing failed: {e}")
        exit(1)
