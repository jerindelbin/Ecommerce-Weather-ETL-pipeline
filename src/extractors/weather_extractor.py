#Gets weather data from the api

import requests
import pandas as pd
from datetime import datetime
import logging

class WeatherExtractor:
    def __init__(self, api_key=None):
        self.api_key = api_key  # Not needed for this free API
        self.base_url = "https://api.open-meteo.com/v1/forecast"
        self.logger = logging.getLogger(__name__)
    
    def extract_weather_data(self, latitude=51.5074, longitude=-0.1278, days=7):
        """Get weather data - default is London coordinates"""
        try:
            start_time = datetime.now()
            
            # What data we want from the API
            params = {
                'latitude': latitude,
                'longitude': longitude,
                'daily': 'temperature_2m_max,temperature_2m_min,precipitation_sum',
                'timezone': 'auto',
                'past_days': days  # How many days back
            }
            
            self.logger.info(f"Fetching weather data for coordinates ({latitude}, {longitude})...")
            
            # Make the API call
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()  # Crash if API call failed
            
            data = response.json()  # Convert response to dictionary
            
            # Turn JSON into a table (DataFrame)
            df = pd.DataFrame({
                'date': data['daily']['time'],
                'temp_max': data['daily']['temperature_2m_max'],
                'temp_min': data['daily']['temperature_2m_min'],
                'precipitation': data['daily']['precipitation_sum']
            })
            
            # Calculate how long it took
            duration = (datetime.now() - start_time).total_seconds()
            self.logger.info(f"Extracted {len(df)} rows in {duration:.2f} seconds")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Weather API extraction failed: {str(e)}")
            raise  # Re-throw the error