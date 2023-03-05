# bq_utils default values
SOCKET_TIMEOUT = 600000
BQ_DEFAULT_RETRY_COUNT = 10
MAX_POLL_INTERVAL = 500
# Maximum results returned by list_tables (API has a low default value)
LIST_TABLES_MAX_RESULTS = 10000
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

# API Dataset job body values
WRITE_TRUNCATE = 'WRITE_TRUNCATE'
WRITE_EMPTY = 'WRITE_EMPTY'
WRITE_APPEND = 'WRITE_APPEND'

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

# HPO table info
LOOKUP_TABLES_DATASET_ID = 'lookup_tables'
HPO_SITE_ID_MAPPINGS_TABLE_ID = 'hpo_site_id_mappings'
HPO_ID_BUCKET_NAME_TABLE_ID = 'hpo_id_bucket_name'

HPO_ID = 'HPO_ID'
ORG_ID = 'Org_ID'
SITE_NAME = 'Site_Name'
BUCKET_NAME = 'bucket_name'

# Dataset Environment variable names
MATCH_DATASET = 'VALIDATION_RESULTS_DATASET_ID'

# Dataset name formats
VALIDATION_DATASET_FORMAT = 'validation_{}'
VALIDATION_DATASET_REGEX = 'validation_\d{8}'
VALIDATION_DATE_FORMAT = '%Y%m%d'

INSERT_QUERY = """
INSERT INTO `{project_id}.{dataset_id}.{table_id}`
  ({columns})
VALUES {mapping_list}
"""

GET_HPO_CONTENTS_QUERY = """
SELECT *
FROM `{project_id}.{LOOKUP_TABLES_DATASET_ID}.{HPO_SITE_TABLE}`
"""
