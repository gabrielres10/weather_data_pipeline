-- enable uuid extension if wanted
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- weather_raw table
CREATE TABLE IF NOT EXISTS weather_raw (
  id SERIAL PRIMARY KEY,
  city_name VARCHAR(255) NOT NULL,
  country_code VARCHAR(10),
  temperature_celsius DECIMAL(6,2),
  temperature_fahrenheit DECIMAL(6,2),
  humidity INTEGER,
  pressure INTEGER,
  weather_description TEXT,
  measurement_time TIMESTAMP,
  processing_time TIMESTAMP,
  unique_run_key TEXT -- to manage duplicates by source filename or run id
);

-- daily_weather_summary
CREATE TABLE IF NOT EXISTS daily_weather_summary (
  id SERIAL PRIMARY KEY,
  date DATE NOT NULL UNIQUE,
  avg_temperature_celsius DECIMAL(6,2),
  max_temperature_celsius DECIMAL(6,2),
  min_temperature_celsius DECIMAL(6,2),
  hottest_city VARCHAR(255),
  coldest_city VARCHAR(255),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_weather_raw_city ON weather_raw(city_name);
CREATE INDEX IF NOT EXISTS idx_weather_raw_measurement_time ON weather_raw(measurement_time);
CREATE INDEX IF NOT EXISTS idx_summary_date ON daily_weather_summary(date);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'uq_weather_raw_run'
      AND conrelid = 'weather_raw'::regclass
  ) THEN
    ALTER TABLE weather_raw ADD CONSTRAINT uq_weather_raw_run UNIQUE (unique_run_key);
  END IF;
END $$;


-- Composite unique constraint for idempotent loads (city + measurement_time)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'uq_weather_city_time'
      AND conrelid = 'weather_raw'::regclass
  ) THEN
    ALTER TABLE weather_raw ADD CONSTRAINT uq_weather_city_time UNIQUE (city_name, measurement_time);
  END IF;
END $$;

