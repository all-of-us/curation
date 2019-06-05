# bq_utils default values
SOCKET_TIMEOUT = 600000
BQ_DEFAULT_RETRY_COUNT = 10
# Maximum results returned by list_tables (API has a low default value)
LIST_TABLES_MAX_RESULTS = 2000
DATE_FORMAT = '%Y%m%d'
BLANK = ''

# API Dataset Job body fields
FRIENDLY_NAME = 'friendlyName'
DESCRIPTION = 'description'
DATASET_ID = 'datasetId'
PROJECT_ID = 'projectId'
DATASET_REFERENCE = 'datasetReference'
KIND = 'kind'
KIND_DATASET = 'bigquery#dataset'
BATCH = 'BATCH'
INTERACTIVE = 'INTERACTIVE'
PRIORITY_TAG = 'priority'

# Query response fields
PAGE_TOKEN = 'pageToken'
JOB_REFERENCE = 'jobReference'
JOB_ID = 'jobId'
ROWS = 'rows'
SCHEMA = 'schema'
FIELDS = 'fields'
DATASET_REF = 'datasetReference'
DATASET_ID = 'datasetId'

# BigQuery API expected strings
TRUE = 'true'
FALSE = 'false'

# Dataset Environment variable names
COMBINED_UNIDENTIFIED_DATASET = 'EHR_RDR_DEID_DATASET_ID'
MATCH_DATASET = 'VALIDATION_RESULTS_DATASET_ID'

# Dataset name formats
COMBINED_UNIDENTIFIED_DATASET_FORMAT = 'combined{}_deid'
VALIDATION_DATASET_FORMAT = 'validation_{}'
