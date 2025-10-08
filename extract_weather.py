import os
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("OPENWEATHERMAP_API_KEY", "YOUR_API_KEY_HERE")
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
CITIES = [
    {"name": "New York", "country": "US"},
    {"name": "London", "country": "GB"},
    {"name": "Tokyo", "country": "JP"},
    {"name": "Sydney", "country": "AU"},
    {"name": "São Paulo", "country": "BR"},
]

# staging dir for raw json
STAGING_DIR = Path("staging/raw")
STAGING_DIR.mkdir(parents=True, exist_ok=True)

# logging configuration
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Create a custom formatter
class ColoredFormatter(logging.Formatter):
    """Custom formatter to add colors to console output"""
    
    grey = "\x1b[38;21m"
    blue = "\x1b[34m"
    yellow = "\x1b[33m"
    red = "\x1b[31m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    
    FORMATS = {
        logging.DEBUG: grey + "%(asctime)s [DEBUG] %(message)s" + reset,
        logging.INFO: blue + "%(asctime)s [INFO] %(message)s" + reset,
        logging.WARNING: yellow + "%(asctime)s [WARNING] %(message)s" + reset,
        logging.ERROR: red + "%(asctime)s [ERROR] %(message)s" + reset,
        logging.CRITICAL: bold_red + "%(asctime)s [CRITICAL] %(message)s" + reset
    }
    
    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, datefmt="%Y-%m-%d %H:%M:%S")
        return formatter.format(record)

# Configure root logger
logging.basicConfig(level=logging.DEBUG, handlers=[])

# Create logger
logger = logging.getLogger("extract_weather")
logger.setLevel(logging.DEBUG)

# Create file handler with detailed logging
log_filename = LOG_DIR / f"extract_weather_{datetime.now(timezone.utc).strftime('%Y%m%d')}.log"
file_handler = logging.FileHandler(log_filename, encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
file_format = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(name)s:%(lineno)d - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
file_handler.setFormatter(file_format)

# Create console handler with colored output
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(ColoredFormatter())

# Add handlers to logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Suppress urllib3 debug logs
logging.getLogger("urllib3").setLevel(logging.WARNING)


class APIError(Exception):
    pass


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30),
       retry=retry_if_exception_type((requests.RequestException, APIError)))
def fetch_city_weather(city_name: str, country_code: str) -> dict:
    """
    Fetch current weather data for a specific city from OpenWeatherMap API.
    
    Args:
        city_name: Name of the city
        country_code: ISO country code (e.g., 'US', 'GB')
    
    Returns:
        dict: JSON response from the API
    
    Raises:
        APIError: When API returns non-200 status code
        requests.RequestException: For network-related errors
    """
    params = {
        "q": f"{city_name},{country_code}",
        "appid": API_KEY,
        "units": "metric"  # Get temperature in Celsius
    }
    logger.info("Fetching weather data for %s, %s", city_name, country_code)
    logger.debug("API request parameters: %s", params)
    
    try:
        resp = requests.get(BASE_URL, params=params, timeout=15)
        logger.debug("API response status: %s for %s", resp.status_code, city_name)
        
        if resp.status_code == 401:
            logger.error("API key is invalid or expired")
            raise APIError(f"Invalid API key for {city_name}")
        elif resp.status_code == 404:
            logger.error("City not found: %s", city_name)
            raise APIError(f"City not found: {city_name}")
        elif resp.status_code != 200:
            logger.warning("Non-200 response for %s: %s %s", city_name, resp.status_code, resp.text)
            raise APIError(f"Status {resp.status_code} for {city_name}")
        
        data = resp.json()
        logger.info("Successfully fetched weather data for %s", city_name)
        return data
        
    except requests.exceptions.Timeout:
        logger.error("Timeout occurred while fetching data for %s", city_name)
        raise
    except requests.exceptions.ConnectionError:
        logger.error("Connection error occurred while fetching data for %s", city_name)
        raise
    except requests.exceptions.RequestException as e:
        logger.error("Request error for %s: %s", city_name, e)
        raise


def save_raw_json(city_name: str, data: dict):
    """
    Save raw JSON weather data to staging directory.
    
    Args:
        city_name: Name of the city
        data: Raw JSON data from API
    """
    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    # Clean city name for filename (handle special characters)
    renamed_city = (city_name.replace(" ", "_")
                         .replace("ã", "a")
                         .replace("ó", "o")
                         .replace("ñ", "n")
                         .replace("ü", "u")
                         .replace("é", "e"))
    
    filename = STAGING_DIR / f"{renamed_city}_{now}.json"
    
    try:
        # Add metadata to the saved file
        enhanced_data = {
            "metadata": {
                "city_name": city_name,
                "extraction_timestamp": now,
                "api_endpoint": BASE_URL,
                "file_version": "1.0"
            },
            "raw_data": data
        }
        
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(enhanced_data, f, ensure_ascii=False, indent=2)
        
        file_size = filename.stat().st_size
        logger.info("Successfully saved raw data for %s -> %s (%.2f KB)", 
                   city_name, filename, file_size / 1024)
        
    except IOError as e:
        logger.error("Failed to save data for %s: %s", city_name, e)
        raise


def main():
    """
    Main function to extract weather data for all cities.
    """
    logger.info("=" * 60)
    logger.info("Starting weather data extraction process")
    logger.info("=" * 60)
    
    # Validate API key
    if API_KEY == "YOUR_API_KEY_HERE" or not API_KEY:
        logger.error("OPENWEATHERMAP_API_KEY is not configured properly")
        logger.error("Please set it in .env file or as environment variable")
        logger.error("Get your free API key at: https://openweathermap.org/api")
        raise SystemExit(1)
    
    logger.info("API key configured successfully")
    logger.info("Staging directory: %s", STAGING_DIR.absolute())
    logger.info("Cities to process: %d", len(CITIES))
    
    results = []
    successful_extractions = 0
    failed_extractions = 0
    
    for i, city_info in enumerate(CITIES, 1):
        city_name = city_info["name"]
        country_code = city_info["country"]
        
        logger.info("-" * 40)
        logger.info("Processing city %d/%d: %s, %s", i, len(CITIES), city_name, country_code)
        
        try:
            # Fetch weather data
            start_time = datetime.now(timezone.utc)
            data = fetch_city_weather(city_name, country_code)
            fetch_duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            # Validate response data
            if not data or 'main' not in data:
                raise APIError(f"Invalid response data for {city_name}")
            
            # Save raw JSON
            save_raw_json(city_name, data)
            
            # Extract key information for logging
            temp = data.get('main', {}).get('temp', 'N/A')
            weather_desc = data.get('weather', [{}])[0].get('description', 'N/A')
            
            successful_extractions += 1
            results.append({
                "city": city_name,
                "country": country_code,
                "status": "success",
                "temperature": temp,
                "description": weather_desc,
                "fetch_time_seconds": round(fetch_duration, 2)
            })
            
            logger.info("[success] Success: %s, %s - %.1f°C, %s (%.2fs)", 
                       city_name, country_code, temp if temp != 'N/A' else 0, 
                       weather_desc, fetch_duration)
            
        except Exception as e:
            failed_extractions += 1
            error_msg = str(e)
            logger.exception("[error] Failed to extract data for %s, %s: %s", 
                           city_name, country_code, error_msg)
            
            results.append({
                "city": city_name,
                "country": country_code,
                "status": "failed",
                "error": error_msg,
                "error_type": type(e).__name__
            })
    
    # Final summary
    logger.info("=" * 60)
    logger.info("EXTRACTION SUMMARY")
    logger.info("=" * 60)
    logger.info("Total cities processed: %d", len(CITIES))
    logger.info("Successful extractions: %d", successful_extractions)
    logger.info("Failed extractions: %d", failed_extractions)
    logger.info("Success rate: %.1f%%", (successful_extractions / len(CITIES)) * 100)
    
    # Log detailed results
    logger.info("\nDetailed results:")
    for result in results:
        if result["status"] == "success":
            logger.info("  [success] %s, %s: %.1f°C - %s", 
                       result["city"], result["country"], 
                       result.get("temperature", 0), result.get("description", "N/A"))
        else:
            logger.info("  [error] %s, %s: %s", 
                       result["city"], result["country"], result.get("error", "Unknown error"))
    
    # Save summary to file
    summary_file = STAGING_DIR / f"extraction_summary_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    try:
        with open(summary_file, "w", encoding="utf-8") as f:
            json.dump({
                "extraction_timestamp": datetime.now(timezone.utc).isoformat(),
                "total_cities": len(CITIES),
                "successful_extractions": successful_extractions,
                "failed_extractions": failed_extractions,
                "success_rate_percent": round((successful_extractions / len(CITIES)) * 100, 1),
                "results": results
            }, f, ensure_ascii=False, indent=2)
        logger.info("Summary saved to: %s", summary_file)
    except Exception as e:
        logger.error("Failed to save summary file: %s", e)
    
    logger.info("=" * 60)
    logger.info("Weather data extraction completed")
    logger.info("=" * 60)
    
    # Exit with error code if any extraction failed
    if failed_extractions > 0:
        logger.warning("Some extractions failed. Check logs for details.")
        return 1
    
    return 0


if __name__ == "__main__":
    try:
        exit_code = main()
        raise SystemExit(exit_code)
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        raise SystemExit(130)  # Standard exit code for Ctrl+C
    except Exception as e:
        logger.exception("Unexpected error occurred: %s", e)
        raise SystemExit(1)
