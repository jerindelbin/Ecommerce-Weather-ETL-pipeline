import pandas as pd
import logging
from datetime import datetime

class DataQualityValidator:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.results = []
    
    def run_all_checks(self, df, config):
        """Run all quality checks"""
        self.logger.info("Starting quality validation...")
        self.results = []
        
        self.check_null_percentage(df, config.get('null_threshold', 0.1))
        self.check_duplicates(df)
        self.check_schema(df, config.get('required_columns', []))
        self.check_value_ranges(df, config.get('value_ranges', {}))
        self.check_row_count(df, config.get('min_rows', 0), config.get('max_rows', float('inf')))
        
        score = self.calculate_quality_score()
        self.logger.info(f"Quality score: {score}/100")
        
        return {
            'score': score,
            'checks': self.results,
            'timestamp': datetime.now().isoformat()
        }
    
    def check_null_percentage(self, df, threshold=0.1):
        """Check if null percentage is below threshold"""
        for col in df.columns:
            null_pct = df[col].isnull().sum() / len(df)
            passed = null_pct <= threshold
            
            self.results.append({
                'check': 'null_percentage',
                'column': col,
                'value': null_pct,
                'threshold': threshold,
                'passed': passed
            })
            
            if not passed:
                self.logger.warning(f"Column '{col}' has {null_pct*100:.2f}% nulls (threshold: {threshold*100}%)")
    
    def check_duplicates(self, df):
        """Check for duplicate rows"""
        dup_count = df.duplicated().sum()
        dup_pct = dup_count / len(df)
        passed = dup_count == 0
        
        self.results.append({
            'check': 'duplicates',
            'column': 'all',
            'value': dup_pct,
            'threshold': 0,
            'passed': passed
        })
        
        if not passed:
            self.logger.warning(f"Found {dup_count} duplicate rows ({dup_pct*100:.2f}%)")
    
    def check_schema(self, df, required_columns):
        """Check if all required columns exist"""
        missing = set(required_columns) - set(df.columns)
        passed = len(missing) == 0
        
        self.results.append({
            'check': 'schema',
            'column': 'all',
            'value': list(missing) if missing else None,
            'threshold': 'all_required',
            'passed': passed
        })
        
        if not passed:
            self.logger.error(f"Missing required columns: {missing}")
    
    def check_value_ranges(self, df, value_ranges):
        """Check if values are within expected ranges"""
        for col, (min_val, max_val) in value_ranges.items():
            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                out_of_range = ((df[col] < min_val) | (df[col] > max_val)).sum()
                passed = out_of_range == 0
                
                self.results.append({
                    'check': 'value_range',
                    'column': col,
                    'value': out_of_range,
                    'threshold': f"{min_val}-{max_val}",
                    'passed': passed
                })
                
                if not passed:
                    self.logger.warning(f"Column '{col}' has {out_of_range} values outside range [{min_val}, {max_val}]")
    
    def check_row_count(self, df, min_rows, max_rows):
        """Check if row count is within expected range"""
        row_count = len(df)
        passed = min_rows <= row_count <= max_rows
        
        self.results.append({
            'check': 'row_count',
            'column': 'all',
            'value': row_count,
            'threshold': f"{min_rows}-{max_rows}",
            'passed': passed
        })
        
        if not passed:
            self.logger.warning(f"Row count {row_count} outside expected range [{min_rows}, {max_rows}]")
    
    def calculate_quality_score(self):
        """Calculate overall quality score (0-100)"""
        if not self.results:
            return 0
        
        passed = sum(1 for r in self.results if r['passed'])
        total = len(self.results)
        
        return round((passed / total) * 100, 2)