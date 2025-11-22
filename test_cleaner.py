from src.extractors.postgres_extractor import PostgresExtractor
from src.transformers.data_cleaner import DataCleaner
from dotenv import load_dotenv
import os
import logging

logging.basicConfig(level=logging.INFO)
load_dotenv()

# Extract sample data
conn_string = (
    f"postgresql://{os.getenv('SOURCE_DB_USER')}:{os.getenv('SOURCE_DB_PASSWORD')}@"
    f"{os.getenv('SOURCE_DB_HOST')}:{os.getenv('SOURCE_DB_PORT')}/{os.getenv('SOURCE_DB_NAME')}"
)

extractor = PostgresExtractor(conn_string)
df = extractor.extract("SELECT * FROM raw_transactions LIMIT 1000")

print(f"\nBefore cleaning: {df.shape}")
print(f"Null counts:\n{df.isnull().sum()}")

# Clean the data
cleaner = DataCleaner()
config = {
    'critical_columns': ['InvoiceNo', 'StockCode'],
    'outlier_columns': ['Quantity', 'UnitPrice']
}

cleaned_df = cleaner.clean(df, config)

print(f"\nAfter cleaning: {cleaned_df.shape}")
print(f"\nCleaning stats: {cleaner.get_cleaning_stats()}")
print(f"\nSample cleaned data:\n{cleaned_df.head()}")