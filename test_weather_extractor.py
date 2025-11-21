#Test file for testing the weather extractor

from src.extractors.weather_extractor import WeatherExtractor
import logging

#Turn on info messages so we can see what's happening
logging.basicConfig(level=logging.INFO)

#Create the weather extractor
extractor = WeatherExtractor()

#Get last 7 days of weather for London
df = extractor.extract_weather_data()

print("\nWeather data extraction successful")
print(f"\nData:\n{df}")