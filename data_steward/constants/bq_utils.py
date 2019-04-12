# Query response fields
PAGE_TOKEN = 'pageToken'
JOB_REFERENCE = 'jobReference'
JOB_ID = 'jobId'
ROWS = 'rows'
SCHEMA = 'schema'
FIELDS = 'fields'

# bq_utils default values
SOCKET_TIMEOUT = 600000
BQ_DEFAULT_RETRY_COUNT = 10
# Maximum results returned by list_tables (API has a low default value)
LIST_TABLES_MAX_RESULTS = 2000


# API Dataset Job body fields
FRIENDLY_NAME = 'friendlyName'
DESCRIPTION = 'description'
DATASET_ID = 'datasetId'
PROJECT_ID = 'projectId'
DATASET_REFERENCE = 'datasetReference'
KIND = 'kind'
KIND_DATASET = 'bigquery#dataset'
