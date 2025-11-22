from src.extractors.postgres_extractor import PostgresExtractor
from src.transformers.data_cleaner import DataCleaner
from src.transformers.schema_mapper import SchemaMapper
from src.quality.validators import DataQualityValidator
from dotenv import load_dotenv
import os
import logging

logging.basicConfig(level=logging.INFO)
load_dotenv()

# Extract and transform data
conn_string = (
    f"postgresql://{os.getenv('SOURCE_DB_USER')}:{os.getenv('SOURCE_DB_PASSWORD')}@"
    f"{os.getenv('SOURCE_DB_HOST')}:{os.getenv('SOURCE_DB_PORT')}/{os.getenv('SOURCE_DB_NAME')}"
)

extractor = PostgresExtractor(conn_string)
df = extractor.extract("SELECT * FROM raw_transactions LIMIT 1000")

cleaner = DataCleaner()
clean_df = cleaner.clean(df)

mapper = SchemaMapper()
mapped_df = mapper.map_ecommerce_schema(clean_df)

# Run quality checks
validator = DataQualityValidator()

config = {
    'null_threshold': 0.05,
    'required_columns': ['transaction_id', 'transaction_date', 'customer_id'],
    'value_ranges': {
        'quantity': (0, 10000),
        'unit_price': (0, 1000)
    },
    'min_rows': 500,
    'max_rows': 2000
}

results = validator.run_all_checks(mapped_df, config)

print("\nQuality Validation Complete!")
print(f"Overall Score: {results['score']}/100")
print(f"\nChecks performed: {len(results['checks'])}")
print(f"Passed: {sum(1 for c in results['checks'] if c['passed'])}")
print(f"Failed: {sum(1 for c in results['checks'] if not c['passed'])}")
print(f"\nFailed checks:")
for check in results['checks']:
    if not check['passed']:
        print(f"  - {check['check']} on {check['column']}: {check['value']}")