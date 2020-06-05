BLANK = ''

# Dataset Environment variable names
MATCH_DATASET = 'VALIDATION_RESULTS_DATASET_ID'

# HPO table info
LOOKUP_TABLES_DATASET_ID = 'lookup_tables'
HPO_SITE_ID_MAPPINGS_TABLE_ID = 'hpo_site_id_mappings'
HPO_ID_BUCKET_NAME_TABLE_ID = 'hpo_id_bucket_name'
HPO_ID_CONTACT_LIST_TABLE_ID = 'hpo_id_contact_list'

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
