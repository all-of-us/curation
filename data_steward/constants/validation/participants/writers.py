# DRC match responses
MATCH = "match"
MISMATCH = "no_match"
MISSING = "missing"
YES = "yes"
NO = "no"

################################################################################
#  Participant Matching Validation Queries
# Select all results from table
VALIDATION_RESULTS_VALUES = 'SELECT * FROM `{project}.{dataset}.{table}`'

# Insert match values for the field into the site table
INSERT_MATCH_VALUES = (
    'INSERT `{project}.{dataset}.{table}` ({id_field}, {field}, {algorithm_field}) '
    'VALUES {values}'
)

# Merge values for the field in the site table
MERGE_UNIFY_SITE_RECORDS = (
    'MERGE `{project}.{dataset}.{table}` AS orig '
    'USING `{project}.{dataset}.{table}` AS updater '
    'ON orig.person_id = updater.person_id '
    'WHEN MATCHED AND orig.first_name IS NOT NULL AND orig.{field} IS NULL AND updater.{field} IS NOT NULL THEN '
    '  UPDATE SET orig.{field} = updater.{field}'
)

# Remove records that were previous merged into the unified participant record
# This is very long to be absolutely explicit
MERGE_DELETE_SPARSE_RECORDS = (
    'MERGE `{project}.{dataset}.{table}` AS orig '
    'USING `{project}.{dataset}.{table}` AS updater '
    'ON orig.person_id = updater.person_id '
    'WHEN MATCHED AND orig.{field_one} IS NULL AND orig.{field_two} IS NULL AND '
    'orig.{field_three} IS NULL AND orig.{field_four} IS NULL AND '
    'orig.{field_five} IS NULL AND orig.{field_six} IS NULL AND '
    'orig.{field_seven} IS NULL AND orig.{field_eight} IS NULL AND '
    'orig.{field_nine} IS NULL AND orig.{field_ten} IS NULL AND '
    'orig.{field_eleven} IS NULL AND orig.{field_twelve} IS NOT NULL THEN '
    '  DELETE '
    'WHEN MATCHED AND orig.{field_one} IS NULL AND orig.{field_two} IS NULL AND '
    'orig.{field_three} IS NULL AND orig.{field_four} IS NULL AND '
    'orig.{field_five} IS NULL AND orig.{field_six} IS NULL AND '
    'orig.{field_seven} IS NULL AND orig.{field_eight} IS NULL AND '
    'orig.{field_nine} IS NULL AND orig.{field_ten} IS NULL AND '
    'orig.{field_eleven} IS NOT NULL AND orig.{field_twelve} IS NULL THEN '
    '  DELETE '
    'WHEN MATCHED AND orig.{field_one} IS NULL AND orig.{field_two} IS NULL AND '
    'orig.{field_three} IS NULL AND orig.{field_four} IS NULL AND '
    'orig.{field_five} IS NULL AND orig.{field_six} IS NULL AND '
    'orig.{field_seven} IS NULL AND orig.{field_eight} IS NULL AND '
    'orig.{field_nine} IS NULL AND orig.{field_ten} IS NOT NULL AND '
    'orig.{field_eleven} IS NULL AND orig.{field_twelve} IS NULL THEN '
    '  DELETE '
    'WHEN MATCHED AND orig.{field_one} IS NULL AND orig.{field_two} IS NULL AND '
    'orig.{field_three} IS NULL AND orig.{field_four} IS NULL AND '
    'orig.{field_five} IS NULL AND orig.{field_six} IS NULL AND '
    'orig.{field_seven} IS NULL AND orig.{field_eight} IS NULL AND '
    'orig.{field_nine} IS NOT NULL AND orig.{field_ten} IS NULL AND '
    'orig.{field_eleven} IS NULL AND orig.{field_twelve} IS NULL THEN '
    '  DELETE '
    'WHEN MATCHED AND orig.{field_one} IS NULL AND orig.{field_two} IS NULL AND '
    'orig.{field_three} IS NULL AND orig.{field_four} IS NULL AND '
    'orig.{field_five} IS NULL AND orig.{field_six} IS NULL AND '
    'orig.{field_seven} IS NULL AND orig.{field_eight} IS NOT NULL AND '
    'orig.{field_nine} IS NULL AND orig.{field_ten} IS NULL AND '
    'orig.{field_eleven} IS NULL AND orig.{field_twelve} IS NULL THEN '
    '  DELETE '
    'WHEN MATCHED AND orig.{field_one} IS NULL AND orig.{field_two} IS NULL AND '
    'orig.{field_three} IS NULL AND orig.{field_four} IS NULL AND '
    'orig.{field_five} IS NULL AND orig.{field_six} IS NULL AND '
    'orig.{field_seven} IS NOT NULL AND orig.{field_eight} IS NULL AND '
    'orig.{field_nine} IS NULL AND orig.{field_ten} IS NULL AND '
    'orig.{field_eleven} IS NULL AND orig.{field_twelve} IS NULL THEN '
    '  DELETE '
    'WHEN MATCHED AND orig.{field_one} IS NULL AND orig.{field_two} IS NULL AND '
    'orig.{field_three} IS NULL AND orig.{field_four} IS NULL AND '
    'orig.{field_five} IS NULL AND orig.{field_six} IS NOT NULL AND '
    'orig.{field_seven} IS NULL AND orig.{field_eight} IS NULL AND '
    'orig.{field_nine} IS NULL AND orig.{field_ten} IS NULL AND '
    'orig.{field_eleven} IS NULL AND orig.{field_twelve} IS NULL THEN '
    '  DELETE '
    'WHEN MATCHED AND orig.{field_one} IS NULL AND orig.{field_two} IS NULL AND '
    'orig.{field_three} IS NULL AND orig.{field_four} IS NULL AND '
    'orig.{field_five} IS NOT NULL AND orig.{field_six} IS NULL AND '
    'orig.{field_seven} IS NULL AND orig.{field_eight} IS NULL AND '
    'orig.{field_nine} IS NULL AND orig.{field_ten} IS NULL AND '
    'orig.{field_eleven} IS NULL AND orig.{field_twelve} IS NULL THEN '
    '  DELETE '
    'WHEN MATCHED AND orig.{field_one} IS NULL AND orig.{field_two} IS NULL AND '
    'orig.{field_three} IS NULL AND orig.{field_four} IS NOT NULL AND '
    'orig.{field_five} IS NULL AND orig.{field_six} IS NULL AND '
    'orig.{field_seven} IS NULL AND orig.{field_eight} IS NULL AND '
    'orig.{field_nine} IS NULL AND orig.{field_ten} IS NULL AND '
    'orig.{field_eleven} IS NULL AND orig.{field_twelve} IS NULL THEN '
    '  DELETE '
    'WHEN MATCHED AND orig.{field_one} IS NULL AND orig.{field_two} IS NULL AND '
    'orig.{field_three} IS NOT NULL AND orig.{field_four} IS NULL AND '
    'orig.{field_five} IS NULL AND orig.{field_six} IS NULL AND '
    'orig.{field_seven} IS NULL AND orig.{field_eight} IS NULL AND '
    'orig.{field_nine} IS NULL AND orig.{field_ten} IS NULL AND '
    'orig.{field_eleven} IS NULL AND orig.{field_twelve} IS NULL THEN '
    '  DELETE '
    'WHEN MATCHED AND orig.{field_one} IS NULL AND orig.{field_two} IS NOT NULL '
    'AND orig.{field_three} IS NULL AND orig.{field_four} IS NULL AND '
    'orig.{field_five} IS NULL AND orig.{field_six} IS NULL AND '
    'orig.{field_seven} IS NULL AND orig.{field_eight} IS NULL AND '
    'orig.{field_nine} IS NULL AND orig.{field_ten} IS NULL AND '
    'orig.{field_eleven} IS NULL AND orig.{field_twelve} IS NULL THEN '
    '  DELETE '
    'WHEN MATCHED AND orig.{field_one} IS NOT NULL AND orig.{field_two} IS '
    'NULL AND orig.{field_three} IS NULL AND orig.{field_four} IS NULL AND '
    'orig.{field_five} IS NULL AND orig.{field_six} IS NULL AND '
    'orig.{field_seven} IS NULL AND orig.{field_eight} IS NULL AND '
    'orig.{field_nine} IS NULL AND orig.{field_ten} IS NULL AND '
    'orig.{field_eleven} IS NULL AND orig.{field_twelve} IS NULL THEN '
    '  DELETE '
)

# Remove records that were previous merged into the unified participant record
# This is very long to be absolutely explicit
MERGE_SET_MISSING_FIELDS = (
    'MERGE `{project}.{dataset}.{table}` AS orig '
    'USING `{project}.{dataset}.{table}` AS updater '
    'ON orig.person_id = updater.person_id '
    'WHEN MATCHED AND orig.{field_one} IS NULL THEN '
    '  UPDATE SET orig.{field_one} = \'{value}\' '
    'WHEN MATCHED AND orig.{field_two} IS NULL THEN '
    '  UPDATE SET orig.{field_two} = \'{value}\' '
    'WHEN MATCHED AND orig.{field_three} IS NULL THEN '
    '  UPDATE SET orig.{field_three} = \'{value}\' '
    'WHEN MATCHED AND orig.{field_four} IS NULL THEN '
    '  UPDATE SET orig.{field_four} = \'{value}\' '
    'WHEN MATCHED AND orig.{field_five} IS NULL THEN '
    '  UPDATE SET orig.{field_five} = \'{value}\' '
    'WHEN MATCHED AND orig.{field_six} IS NULL THEN '
    '  UPDATE SET orig.{field_six} = \'{value}\' '
    'WHEN MATCHED AND orig.{field_seven} IS NULL THEN '
    '  UPDATE SET orig.{field_seven} = \'{value}\' '
    'WHEN MATCHED AND orig.{field_eight} IS NULL THEN '
    '  UPDATE SET orig.{field_eight} = \'{value}\' '
    'WHEN MATCHED AND orig.{field_nine} IS NULL THEN '
    '  UPDATE SET orig.{field_nine} = \'{value}\' '
    'WHEN MATCHED AND orig.{field_ten} IS NULL THEN '
    '  UPDATE SET orig.{field_ten} = \'{value}\' '
    'WHEN MATCHED AND orig.{field_eleven} IS NULL THEN '
    '  UPDATE SET orig.{field_eleven} = \'{value}\' '
    'WHEN MATCHED AND orig.{field_twelve} IS NULL THEN '
    '  UPDATE SET orig.{field_twelve} = \'{value}\' '
)

# Table names
VALIDATION_TABLE_SUFFIX = '_identity_match'

# Field names
PERSON_ID_FIELD = 'person_id'
FIRST_NAME_FIELD = 'first_name'
MIDDLE_NAME_FIELD = 'middle_name'
LAST_NAME_FIELD = 'last_name'
EMAIL_FIELD = 'email'
PHONE_NUMBER_FIELD = 'phone_number'
SEX_FIELD = 'sex'
ZIP_CODE_FIELD = 'zip'
STATE_FIELD = 'state'
CITY_FIELD = 'city'
ADDRESS_ONE_FIELD = 'address_1'
ADDRESS_TWO_FIELD = 'address_2'
BIRTH_DATE_FIELD = 'birth_date'
ALGORITHM_FIELD = 'algorithm'
ADDRESS_MATCH_FIELD = 'address'

VALIDATION_FIELDS = [
    FIRST_NAME_FIELD, MIDDLE_NAME_FIELD, LAST_NAME_FIELD,
    EMAIL_FIELD, PHONE_NUMBER_FIELD, ZIP_CODE_FIELD, STATE_FIELD,
    CITY_FIELD, ADDRESS_ONE_FIELD, ADDRESS_TWO_FIELD,
    BIRTH_DATE_FIELD, SEX_FIELD
]
