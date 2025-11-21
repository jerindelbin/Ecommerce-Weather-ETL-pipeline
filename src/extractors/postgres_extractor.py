#Getting data from PostgreSQL database

import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import logging

class PostgresExtractor:
    def __init__(self, connection_string):
        # Save database connection and setup logger
        self.engine = create_engine(connection_string)
        self.logger = logging.getLogger(__name__)
    
    def extract(self, query):
        """Run SQL query and return data as DataFrame"""
        try:
            start_time = datetime.now()
            # Log first 100 chars of query so we know what's running
            self.logger.info(f"Starting extraction with query: {query[:100]}...")
            
            # Execute query and load results into pandas DataFrame
            df = pd.read_sql(query, self.engine)
            
            # Calculate how long it took
            duration = (datetime.now() - start_time).total_seconds()
            self.logger.info(f"Extracted {len(df)} rows in {duration:.2f} seconds")
            
            return df
            
        except Exception as e:
            # If anything breaks, log it and crash
            self.logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def get_metadata(self, df):
        """Return basic info about the extracted data"""
        return {
            'row_count': len(df),  # How many rows
            'column_count': len(df.columns),  # How many columns
            'columns': df.columns.tolist(),  # Column names
            'extraction_time': datetime.now().isoformat()  # When extracted
        }