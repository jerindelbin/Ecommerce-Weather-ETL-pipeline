import pandas as pd
import logging
from datetime import datetime

class DataCleaner:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.cleaning_stats = {}
    
    def clean(self, df, config=None):
        """Main cleaning pipeline"""
        self.logger.info(f"Starting data cleaning. Input shape: {df.shape}")
        
        df = df.copy()
        original_rows = len(df)
        
        # Remove duplicates
        df = self.remove_duplicates(df)
        
        # Handle nulls
        df = self.handle_nulls(df, config)
        
        # Fix data types
        df = self.fix_data_types(df)
        
        # Remove outliers
        df = self.remove_outliers(df, config)
        
        # Standardize text
        df = self.standardize_text(df)
        
        final_rows = len(df)
        self.cleaning_stats['rows_removed'] = original_rows - final_rows
        self.cleaning_stats['removal_percentage'] = ((original_rows - final_rows) / original_rows) * 100
        
        self.logger.info(f"Cleaning complete. Output shape: {df.shape}")
        self.logger.info(f"Removed {self.cleaning_stats['rows_removed']} rows ({self.cleaning_stats['removal_percentage']:.2f}%)")
        
        return df
    
    def remove_duplicates(self, df):
        """Remove duplicate rows"""
        initial_count = len(df)
        df = df.drop_duplicates()
        removed = initial_count - len(df)
        
        self.cleaning_stats['duplicates_removed'] = removed
        self.logger.info(f"Removed {removed} duplicate rows")
        
        return df
    
    def handle_nulls(self, df, config=None):
        """Handle missing values"""
        null_counts = df.isnull().sum()
        
        for col in df.columns:
            null_pct = (null_counts[col] / len(df)) * 100
            
            if null_pct > 0:
                self.logger.info(f"Column '{col}' has {null_pct:.2f}% null values")
                
                # Drop rows if too many nulls in critical columns
                if config and col in config.get('critical_columns', []) and null_pct > 5:
                    df = df.dropna(subset=[col])
                    self.logger.info(f"Dropped rows with null in critical column '{col}'")
        
        return df
    
    def fix_data_types(self, df):
        """Fix common data type issues"""
        for col in df.columns:
            # Convert string numbers to numeric
            if df[col].dtype == 'object':
                try:
                    df[col] = pd.to_numeric(df[col], errors='ignore')
                except:
                    pass
        
        return df
    
    def remove_outliers(self, df, config=None):
        """Remove outliers from numeric columns using IQR method"""
        if not config or 'outlier_columns' not in config:
            return df
        
        for col in config.get('outlier_columns', []):
            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                initial_count = len(df)
                df = df[(df[col] >= lower_bound) & (df[col] <= upper_bound)]
                removed = initial_count - len(df)
                
                self.logger.info(f"Removed {removed} outliers from column '{col}'")
        
        return df
    
    def standardize_text(self, df):
        """Standardize text columns"""
        for col in df.columns:
            if df[col].dtype == 'object':
                # Strip whitespace
                df[col] = df[col].str.strip()
                
                # Lowercase for specific columns (like email, country)
                if 'country' in col.lower() or 'email' in col.lower():
                    df[col] = df[col].str.lower()
        
        return df
    
    def get_cleaning_stats(self):
        """Return cleaning statistics"""
        return self.cleaning_stats