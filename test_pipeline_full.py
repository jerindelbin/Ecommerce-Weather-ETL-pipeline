from src.extractors.postgres_extractor import PostgresExtractor
from src.extractors.weather_extractor import WeatherExtractor
from src.transformers.data_cleaner import DataCleaner
from src.transformers.schema_mapper import SchemaMapper
from src.loaders.database_loader import DatabaseLoader
from dotenv import load_dotenv
import os
import logging

logging.basicConfig(level=logging.INFO)
load_dotenv()

# Connection strings
source_conn = (
    f"postgresql://{os.getenv('SOURCE_DB_USER')}:{os.getenv('SOURCE_DB_PASSWORD')}@"
    f"{os.getenv('SOURCE_DB_HOST')}:{os.getenv('SOURCE_DB_PORT')}/{os.getenv('SOURCE_DB_NAME')}"
)

target_conn = (
    f"postgresql://{os.getenv('TARGET_DB_USER')}:{os.getenv('TARGET_DB_PASSWORD')}@"
    f"{os.getenv('TARGET_DB_HOST')}:{os.getenv('TARGET_DB_PORT')}/{os.getenv('TARGET_DB_NAME')}"
)

print("\nRunning Full ETL Pipeline\n")

# EXTRACT
print("EXTRACTING DATA")
pg_extractor = PostgresExtractor(source_conn)
ecom_df = pg_extractor.extract("SELECT * FROM raw_transactions LIMIT 1000")

weather_extractor = WeatherExtractor()
weather_df = weather_extractor.extract_weather_data()

# TRANSFORM
print("\nTRANSFORMING DATA")
cleaner = DataCleaner()
ecom_clean = cleaner.clean(ecom_df, config={'outlier_columns': ['Quantity', 'UnitPrice']})

mapper = SchemaMapper()
ecom_mapped = mapper.map_ecommerce_schema(ecom_clean)
weather_mapped = mapper.map_weather_schema(weather_df)

# LOAD
print("\nLOADING DATA")
loader = DatabaseLoader(target_conn)

ecom_result = loader.load(ecom_mapped, 'ecommerce_transactions', if_exists='replace')
weather_result = loader.load(weather_mapped, 'weather_data', if_exists='replace')

# VERIFY
print("\nVERIFICATION")
ecom_count = loader.verify_load('ecommerce_transactions')
weather_count = loader.verify_load('weather_data')

print(f"\nPIPELINE COMPLETE")
print(f"E-commerce records loaded: {ecom_count}")
print(f"Weather records loaded: {weather_count}")
print(f"\nLoad results:")
print(f"  E-commerce: {ecom_result}")
print(f"  Weather: {weather_result}")