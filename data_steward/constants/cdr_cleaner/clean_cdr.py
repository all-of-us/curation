from enum import Enum, unique

EHR = 'ehr'
UNIONED = 'unioned'
RDR = 'rdr'
COMBINED = 'combined'
DEID_BASE = 'deid_base'
DEID_CLEAN = 'deid_clean'
FITBIT = 'fitbit'

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

    def __str__(self):
        return self.value
