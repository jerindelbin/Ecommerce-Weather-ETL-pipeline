from src.extractors.postgres_extractor import PostgresExtractor
from src.extractors.weather_extractor import WeatherExtractor
from src.transformers.data_cleaner import DataCleaner
from src.transformers.schema_mapper import SchemaMapper
from dotenv import load_dotenv
import os
import logging

logging.basicConfig(level=logging.INFO)
load_dotenv()

# Extract and clean e-commerce data
conn_string = (
    f"postgresql://{os.getenv('SOURCE_DB_USER')}:{os.getenv('SOURCE_DB_PASSWORD')}@"
    f"{os.getenv('SOURCE_DB_HOST')}:{os.getenv('SOURCE_DB_PORT')}/{os.getenv('SOURCE_DB_NAME')}"
)

extractor = PostgresExtractor(conn_string)
ecom_df = extractor.extract("SELECT * FROM raw_transactions LIMIT 100")

cleaner = DataCleaner()
ecom_clean = cleaner.clean(ecom_df)

# Extract weather data
weather_extractor = WeatherExtractor()
weather_df = weather_extractor.extract_weather_data()

# Map schemas
mapper = SchemaMapper()
ecom_mapped = mapper.map_ecommerce_schema(ecom_clean)
weather_mapped = mapper.map_weather_schema(weather_df)

print("\nE-commerce schema mapped:")
print(f"Columns: {ecom_mapped.columns.tolist()}")
print(f"\nSample:\n{ecom_mapped.head()}")

print("\nWeather schema mapped:")
print(f"Columns: {weather_mapped.columns.tolist()}")
print(f"\nSample:\n{weather_mapped.head()}")