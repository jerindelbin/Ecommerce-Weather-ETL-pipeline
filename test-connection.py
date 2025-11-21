#For testing the 2 postgre instances (databases)

from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# Read passwords from .env file
load_dotenv()

# Build connection string for source database
source_engine = create_engine(
    f"postgresql://{os.getenv('SOURCE_DB_USER')}:{os.getenv('SOURCE_DB_PASSWORD')}@"
    f"{os.getenv('SOURCE_DB_HOST')}:{os.getenv('SOURCE_DB_PORT')}/{os.getenv('SOURCE_DB_NAME')}"
)

# Build connection string for target database
target_engine = create_engine(
    f"postgresql://{os.getenv('TARGET_DB_USER')}:{os.getenv('TARGET_DB_PASSWORD')}@"
    f"{os.getenv('TARGET_DB_HOST')}:{os.getenv('TARGET_DB_PORT')}/{os.getenv('TARGET_DB_NAME')}"
)

# Try connecting to both
try:
    # Test source database with simple query
    with source_engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        print("✅ Source database connected successfully!")
    
    # Test target database    
    with target_engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        print("✅ Target database connected successfully!")
        
except Exception as e:
    print(f"❌ Connection failed: {e}")