from sqlalchemy import create_engine
import pandas as pd
import logging
from datetime import datetime

class DatabaseLoader:
    def __init__(self, connection_string):
        self.engine = create_engine(connection_string)
        self.logger = logging.getLogger(__name__)
    
    def load(self, df, table_name, if_exists='append'):
        """Load data to database table"""
        try:
            start_time = datetime.now()
            self.logger.info(f"Loading {len(df)} rows to table '{table_name}'...")
            
            df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)
            
            duration = (datetime.now() - start_time).total_seconds()
            self.logger.info(f"Successfully loaded {len(df)} rows in {duration:.2f} seconds")
            
            return {
                'rows_loaded': len(df),
                'table_name': table_name,
                'duration_seconds': duration,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}")
            raise
    
    def verify_load(self, table_name):
        """Verify data was loaded successfully"""
        query = f"SELECT COUNT(*) as count FROM {table_name}"
        result = pd.read_sql(query, self.engine)
        count = result['count'].iloc[0]
        
        self.logger.info(f"Table '{table_name}' contains {count} rows")
        return count