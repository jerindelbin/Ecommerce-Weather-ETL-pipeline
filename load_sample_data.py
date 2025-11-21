#Loading the CSV into the database

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Get database credentials
load_dotenv()

# Connect to source database
source_engine = create_engine(
    f"postgresql://{os.getenv('SOURCE_DB_USER')}:{os.getenv('SOURCE_DB_PASSWORD')}@"
    f"{os.getenv('SOURCE_DB_HOST')}:{os.getenv('SOURCE_DB_PORT')}/{os.getenv('SOURCE_DB_NAME')}"
)

print("Loading CSV file")
df = pd.read_csv('data/raw/ecommerce_data.csv')

print(f"Loaded {len(df)} rows")
print(f"Columns: {df.columns.tolist()}")

print("\nLoading data into PostgreSQL")
# Create table called 'raw_transactions', replace if exists
df.to_sql('raw_transactions', source_engine, if_exists='replace', index=False)

print("Data loaded successfully")
print(f"Table 'raw_transactions' created with {len(df)} rows")