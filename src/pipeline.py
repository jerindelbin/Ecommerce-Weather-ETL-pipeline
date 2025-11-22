from extractors.postgres_extractor import PostgresExtractor
from extractors.weather_extractor import WeatherExtractor
from transformers.data_cleaner import DataCleaner
from transformers.schema_mapper import SchemaMapper
from loaders.database_loader import DatabaseLoader
from quality.validators import DataQualityValidator
from dotenv import load_dotenv
import os
import logging
from datetime import datetime
import json

class ETLPipeline:
    def __init__(self):
        load_dotenv()
        self.setup_logging()
        self.setup_connections()
        
    def setup_logging(self):
        """Configure logging"""
        log_file = f"data/logs/pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def setup_connections(self):
        """Setup database connections"""
        self.source_conn = (
            f"postgresql://{os.getenv('SOURCE_DB_USER')}:{os.getenv('SOURCE_DB_PASSWORD')}@"
            f"{os.getenv('SOURCE_DB_HOST')}:{os.getenv('SOURCE_DB_PORT')}/{os.getenv('SOURCE_DB_NAME')}"
        )
        
        self.target_conn = (
            f"postgresql://{os.getenv('TARGET_DB_USER')}:{os.getenv('TARGET_DB_PASSWORD')}@"
            f"{os.getenv('TARGET_DB_HOST')}:{os.getenv('TARGET_DB_PORT')}/{os.getenv('TARGET_DB_NAME')}"
        )
    
    def run(self, run_id=None):
        """Execute the full ETL pipeline"""
        if not run_id:
            run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        self.logger.info(f"="*50)
        self.logger.info(f"Starting ETL Pipeline - Run ID: {run_id}")
        self.logger.info(f"="*50)
        
        pipeline_start = datetime.now()
        
        try:
            # EXTRACT
            self.logger.info("PHASE 1: EXTRACTION")
            ecom_df, weather_df = self.extract_data()
            
            # PRE-TRANSFORM QUALITY CHECK
            self.logger.info("PHASE 2: PRE-TRANSFORM QUALITY CHECK")
            pre_quality = self.validate_data(ecom_df, "pre_transform")
            
            # TRANSFORM
            self.logger.info("PHASE 3: TRANSFORMATION")
            ecom_clean, weather_clean = self.transform_data(ecom_df, weather_df)
            
            # POST-TRANSFORM QUALITY CHECK
            self.logger.info("PHASE 4: POST-TRANSFORM QUALITY CHECK")
            post_quality = self.validate_data(ecom_clean, "post_transform")
            
            # LOAD
            self.logger.info("PHASE 5: LOADING")
            load_results = self.load_data(ecom_clean, weather_clean)
            
            # SUMMARY
            duration = (datetime.now() - pipeline_start).total_seconds()
            self.log_summary(run_id, "SUCCESS", duration, pre_quality, post_quality, load_results)
            
            return {
                'status': 'SUCCESS',
                'run_id': run_id,
                'duration': duration,
                'quality_score': post_quality['score']
            }
            
        except Exception as e:
            duration = (datetime.now() - pipeline_start).total_seconds()
            self.logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
            self.log_summary(run_id, "FAILED", duration, error=str(e))
            raise
    
    def extract_data(self):
        """Extract data from all sources"""
        pg_extractor = PostgresExtractor(self.source_conn)
        ecom_df = pg_extractor.extract("SELECT * FROM raw_transactions")
        
        weather_extractor = WeatherExtractor()
        weather_df = weather_extractor.extract_weather_data(days=30)
        
        return ecom_df, weather_df
    
    def transform_data(self, ecom_df, weather_df):
        """Transform and clean data"""
        cleaner = DataCleaner()
        mapper = SchemaMapper()
        
        # Clean e-commerce data
        ecom_clean = cleaner.clean(ecom_df, config={
            'critical_columns': ['InvoiceNo', 'StockCode'],
            'outlier_columns': ['Quantity', 'UnitPrice']
        })
        
        # Map schemas
        ecom_mapped = mapper.map_ecommerce_schema(ecom_clean)
        weather_mapped = mapper.map_weather_schema(weather_df)
        
        return ecom_mapped, weather_mapped
    
    def validate_data(self, df, stage):
        """Run quality validation"""
        validator = DataQualityValidator()
        
        config = {
            'null_threshold': 0.05,
            'required_columns': ['transaction_id', 'transaction_date', 'customer_id'] if 'transaction_id' in df.columns else [],
            'value_ranges': {
                'quantity': (0, 10000),
                'unit_price': (0, 1000)
            } if 'quantity' in df.columns else {},
            'min_rows': 100
        }
        
        results = validator.run_all_checks(df, config)
        self.logger.info(f"{stage.upper()} Quality Score: {results['score']}/100")
        
        return results
    
    def load_data(self, ecom_df, weather_df):
        """Load data to target database"""
        loader = DatabaseLoader(self.target_conn)
        
        ecom_result = loader.load(ecom_df, 'ecommerce_transactions', if_exists='replace')
        weather_result = loader.load(weather_df, 'weather_data', if_exists='replace')
        
        return {
            'ecommerce': ecom_result,
            'weather': weather_result
        }
    
    def log_summary(self, run_id, status, duration, pre_quality=None, post_quality=None, load_results=None, error=None):
        """Log pipeline execution summary"""
        self.logger.info("="*50)
        self.logger.info("PIPELINE SUMMARY")
        self.logger.info("="*50)
        self.logger.info(f"Run ID: {run_id}")
        self.logger.info(f"Status: {status}")
        self.logger.info(f"Duration: {duration:.2f} seconds")
        
        if pre_quality:
            self.logger.info(f"Pre-transform Quality: {pre_quality['score']}/100")
        if post_quality:
            self.logger.info(f"Post-transform Quality: {post_quality['score']}/100")
        if load_results:
            self.logger.info(f"Records loaded: Ecommerce={load_results['ecommerce']['rows_loaded']}, Weather={load_results['weather']['rows_loaded']}")
        if error:
            self.logger.error(f"Error: {error}")
        
        self.logger.info("="*50)

if __name__ == "__main__":
    pipeline = ETLPipeline()
    result = pipeline.run()
    print(f"\nPipeline completed: {result}")