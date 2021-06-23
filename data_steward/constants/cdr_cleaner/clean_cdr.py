from enum import Enum, unique

EHR = 'ehr'
UNIONED = 'unioned'
RDR = 'rdr'
COMBINED = 'combined'
DEID_BASE = 'deid_base'
DEID_CLEAN = 'deid_clean'
FITBIT = 'fitbit'
FITBIT_DEID = 'fitbit_deid'

CONTROLLED_TIER_DEID = 'controlled_tier_deid'
CONTROLLED_TIER_DEID_BASE = 'controlled_tier_deid_base'
CONTROLLED_TIER_DEID_CLEAN = 'controlled_tier_deid_clean'
CONTROLLED_TIER_FITBIT = 'controlled_tier_fitbit'

REGISTERED_TIER_DEID = 'registered_tier_deid'

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
    DEID_BASE = DEID_BASE
    DEID_CLEAN = DEID_CLEAN
    FITBIT = FITBIT
    CONTROLLED_TIER_FITBIT = CONTROLLED_TIER_FITBIT
    FITBIT_DEID = FITBIT_DEID
    CONTROLLED_TIER_DEID = CONTROLLED_TIER_DEID
    CONTROLLED_TIER_DEID_BASE = CONTROLLED_TIER_DEID_BASE
    CONTROLLED_TIER_DEID_CLEAN = CONTROLLED_TIER_DEID_CLEAN
    REGISTERED_TIER_DEID = REGISTERED_TIER_DEID

    def __str__(self):
        return self.value
