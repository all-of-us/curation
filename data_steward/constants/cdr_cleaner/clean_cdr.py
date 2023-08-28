from enum import Enum, unique

# pre-deid datasets
EHR = 'ehr'
UNIONED = 'unioned'
RDR = 'rdr'
COMBINED = 'combined'
FITBIT = 'fitbit'
SYNTHETIC = 'synthetic'

# post deid Registered tier datasets
REGISTERED_TIER_DEID = 'registered_tier_deid'
REGISTERED_TIER_DEID_BASE = 'registered_tier_deid_base'
REGISTERED_TIER_DEID_CLEAN = 'registered_tier_deid_clean'
REGISTERED_TIER_FITBIT = 'registered_tier_fitbit'

# post deid Controlled tier datasets
CONTROLLED_TIER_DEID = 'controlled_tier_deid'
CONTROLLED_TIER_DEID_BASE = 'controlled_tier_deid_base'
CONTROLLED_TIER_DEID_CLEAN = 'controlled_tier_deid_clean'
CONTROLLED_TIER_FITBIT = 'controlled_tier_fitbit'

DATA_CONSISTENCY = 'data_consistency'
CRON_RETRACTION = 'cron_retraction'
BACKUP = 'backup'
STAGING = 'staging'
SANDBOX = 'sandbox'
CLEAN = 'clean'

PERSON_TABLE_NAME = 'person'

# Query dictionary keys
QUERY = 'query'
LEGACY_SQL = 'use_legacy_sql'
DESTINATION_TABLE = 'destination_table_id'
RETRY_COUNT = 'retry_count'
DISPOSITION = 'write_disposition'
DESTINATION_DATASET = 'destination_dataset_id'
BATCH = 'batch'
PROCEDURE_OCCURRENCE = 'procedure_occurrence'
QUALIFIER_SOURCE_VALUE = 'qualifier_source_value'

MODULE_NAME = 'module_name'
FUNCTION_NAME = 'function_name'
LINE_NO = 'line_no'
QUERY_FUNCTION = 'query_function'
SETUP_FUNCTION = 'setup_function'
DESTINATION = 'destination'

# Query dictionary default_values
MODULE_NAME_DEFAULT_VALUE = 'Unknown module'
FUNCTION_NAME_DEFAULT_VALUE = 'Unknown function'
LINE_NO_DEFAULT_VALUE = 'Unknown line number'


@unique
class DataStage(Enum):
    UNSPECIFIED = 'unspecified'
    EHR = EHR
    RDR = RDR
    UNIONED = UNIONED
    COMBINED = COMBINED
    FITBIT = FITBIT
    REGISTERED_TIER_DEID = REGISTERED_TIER_DEID
    REGISTERED_TIER_DEID_BASE = REGISTERED_TIER_DEID_BASE
    REGISTERED_TIER_DEID_CLEAN = REGISTERED_TIER_DEID_CLEAN
    REGISTERED_TIER_FITBIT = REGISTERED_TIER_FITBIT
    CONTROLLED_TIER_DEID = CONTROLLED_TIER_DEID
    CONTROLLED_TIER_DEID_BASE = CONTROLLED_TIER_DEID_BASE
    CONTROLLED_TIER_DEID_CLEAN = CONTROLLED_TIER_DEID_CLEAN
    CONTROLLED_TIER_FITBIT = CONTROLLED_TIER_FITBIT
    DATA_CONSISTENCY = DATA_CONSISTENCY
    CRON_RETRACTION = CRON_RETRACTION
    SYNTHETIC = SYNTHETIC

    def __str__(self):
        return self.value
