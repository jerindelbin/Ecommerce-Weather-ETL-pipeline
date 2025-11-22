import pandas as pd
import logging
from datetime import datetime

class SchemaMapper:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def map_ecommerce_schema(self, df):
        """Map e-commerce data to target schema"""
        self.logger.info("Mapping e-commerce schema...")
        
        mapped_df = pd.DataFrame({
            'transaction_id': df['InvoiceNo'],
            'transaction_date': pd.to_datetime(df['InvoiceDate']),
            'customer_id': df['CustomerID'],
            'product_code': df['StockCode'],
            'product_description': df['Description'],
            'quantity': df['Quantity'],
            'unit_price': df['UnitPrice'],
            'total_price': df['Quantity'] * df['UnitPrice'],
            'country': df['Country'],
            'source': 'ecommerce_db'
        })
        
        return mapped_df
    
    def map_weather_schema(self, df):
        """Map weather data to target schema"""
        self.logger.info("Mapping weather schema...")
        
        mapped_df = pd.DataFrame({
            'date': pd.to_datetime(df['date']),
            'temp_max_c': df['temp_max'],
            'temp_min_c': df['temp_min'],
            'precipitation_mm': df['precipitation'],
            'source': 'weather_api'
        })
        
        return mapped_df
    
    def validate_schema(self, df, required_columns):
        """Validate that required columns exist"""
        missing = set(required_columns) - set(df.columns)
        
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
        
        self.logger.info(f"Schema validation passed. All required columns present.")
        return True