BLANK = ''

# Dataset Environment variable names
MATCH_DATASET = 'VALIDATION_RESULTS_DATASET_ID'

# Validation dataset prefix
VALIDATION_PREFIX = 'validation'
VALIDATION_DATASET_FORMAT = 'validation_{}'
VALIDATION_DATASET_REGEX = 'validation_\d{8}'
VALIDATION_DATE_FORMAT = '%Y%m%d'

# Query to list all table information within a dataset
TABLE_INFO_QUERY = """
SELECT *
FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
"""

TABLE_NAME = 'table_name'
COLUMN_NAME = 'column_name'
