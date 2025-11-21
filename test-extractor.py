#Testing the database extraction 

from src.extractors.postgres_extractor import PostgresExtractor
from dotenv import load_dotenv
import os
import logging

# Show info messages
logging.basicConfig(level=logging.INFO)
load_dotenv()

# Build connection string
conn_string = (
    f"postgresql://{os.getenv('SOURCE_DB_USER')}:{os.getenv('SOURCE_DB_PASSWORD')}@"
    f"{os.getenv('SOURCE_DB_HOST')}:{os.getenv('SOURCE_DB_PORT')}/{os.getenv('SOURCE_DB_NAME')}"
)

# Create extractor object
extractor = PostgresExtractor(conn_string)

# Get first 10 rows from database
query = "SELECT * FROM raw_transactions LIMIT 10"
df = extractor.extract(query)

print("\nExtraction successful")
print(f"\nFirst 5 rows:\n{df.head()}")
print(f"\nMetadata: {extractor.get_metadata(df)}")