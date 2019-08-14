from constants.bq_utils import (WRITE_APPEND, WRITE_TRUNCATE)
from constants.validation.participants.identity_match import (MATCH, MISMATCH, MISSING)

# DRC match responses
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

# Remove records that were previously merged into the unified participant record
# Relies on a first and last name existing
SELECT_FULL_RECORDS = (
    'SELECT * FROM `{project}.{dataset}.{table}` '
    'WHERE NOT '
    '({field_two} IS NULL AND {field_three} IS NULL AND '
    '{field_four} IS NULL AND {field_five} IS NULL AND {field_six} IS NULL AND '
    '{field_seven} IS NULL AND {field_eight} IS NULL AND {field_nine} IS NULL AND '
    '{field_ten} IS NULL AND {field_eleven} IS NULL AND {field_twelve} IS NULL) AND NOT '
    '({field_one} IS NULL AND {field_three} IS NULL AND '
    '{field_four} IS NULL AND {field_five} IS NULL AND {field_six} IS NULL AND '
    '{field_seven} IS NULL AND {field_eight} IS NULL AND {field_nine} IS NULL AND '
    '{field_ten} IS NULL AND {field_eleven} IS NULL AND {field_twelve} IS NULL) AND NOT '
    '({field_one} IS NULL AND {field_two} IS NULL AND '
    '{field_four} IS NULL AND {field_five} IS NULL AND {field_six} IS NULL AND '
    '{field_seven} IS NULL AND {field_eight} IS NULL AND {field_nine} IS NULL AND '
    '{field_ten} IS NULL AND {field_eleven} IS NULL AND {field_twelve} IS NULL) AND NOT '
    '({field_one} IS NULL AND {field_two} IS NULL AND '
    '{field_three} IS NULL AND {field_five} IS NULL AND {field_six} IS NULL AND '
    '{field_seven} IS NULL AND {field_eight} IS NULL AND {field_nine} IS NULL AND '
    '{field_ten} IS NULL AND {field_eleven} IS NULL AND {field_twelve} IS NULL) AND NOT '
    '({field_one} IS NULL AND {field_two} IS NULL AND '
    '{field_three} IS NULL AND {field_four} IS NULL AND {field_six} IS NULL AND '
    '{field_seven} IS NULL AND {field_eight} IS NULL AND {field_nine} IS NULL AND '
    '{field_ten} IS NULL AND {field_eleven} IS NULL AND {field_twelve} IS NULL) AND NOT '
    '({field_one} IS NULL AND {field_two} IS NULL AND '
    '{field_three} IS NULL AND {field_four} IS NULL AND {field_five} IS NULL AND '
    '{field_seven} IS NULL AND {field_eight} IS NULL AND {field_nine} IS NULL AND '
    '{field_ten} IS NULL AND {field_eleven} IS NULL AND {field_twelve} IS NULL) AND NOT '
    '({field_one} IS NULL AND {field_two} IS NULL AND '
    '{field_three} IS NULL AND {field_four} IS NULL AND {field_five} IS NULL AND '
    '{field_six} IS NULL AND {field_eight} IS NULL AND {field_nine} IS NULL AND '
    '{field_ten} IS NULL AND {field_eleven} IS NULL AND {field_twelve} IS NULL) AND NOT '
    '({field_one} IS NULL AND {field_two} IS NULL AND '
    '{field_three} IS NULL AND {field_four} IS NULL AND {field_five} IS NULL AND '
    '{field_six} IS NULL AND {field_seven} IS NULL AND {field_nine} IS NULL AND '
    '{field_ten} IS NULL AND {field_eleven} IS NULL AND {field_twelve} IS NULL) AND NOT '
    '({field_one} IS NULL AND {field_two} IS NULL AND '
    '{field_three} IS NULL AND {field_four} IS NULL AND {field_five} IS NULL AND '
    '{field_six} IS NULL AND {field_seven} IS NULL AND {field_eight} IS NULL AND '
    '{field_ten} IS NULL AND {field_eleven} IS NULL AND {field_twelve} IS NULL) AND NOT '
    '({field_one} IS NULL AND {field_two} IS NULL AND '
    '{field_three} IS NULL AND {field_four} IS NULL AND {field_five} IS NULL AND '
    '{field_six} IS NULL AND {field_seven} IS NULL AND {field_eight} IS NULL AND '
    '{field_nine} IS NULL AND {field_eleven} IS NULL AND {field_twelve} IS NULL) AND NOT '
    '({field_one} IS NULL AND {field_two} IS NULL AND '
    '{field_three} IS NULL AND {field_four} IS NULL AND {field_five} IS NULL AND '
    '{field_six} IS NULL AND {field_seven} IS NULL AND {field_eight} IS NULL AND '
    '{field_nine} IS NULL AND {field_ten} IS NULL AND {field_twelve} IS NULL) AND NOT '
    '({field_one} IS NULL AND {field_two} IS NULL AND '
    '{field_three} IS NULL AND {field_four} IS NULL AND {field_five} IS NULL AND '
    '{field_six} IS NULL AND {field_seven} IS NULL AND {field_eight} IS NULL AND '
    '{field_nine} IS NULL AND {field_ten} IS NULL AND {field_eleven} IS NULL) '
)

# Sets null values to the missing identifier
SELECT_SET_MISSING_VALUE = (
    'SELECT '
    '{person_id}, {algorithm}, '
    'IFNULL({field_one}, \'{value}\') as {field_one}, '
    'IFNULL({field_two}, \'{value}\') as {field_two}, '
    'IFNULL({field_three}, \'{value}\') as {field_three}, '
    'IFNULL({field_four}, \'{value}\') as {field_four}, '
    'IFNULL({field_five}, \'{value}\') as {field_five}, '
    'IFNULL({field_six}, \'{value}\') as {field_six}, '
    'IFNULL({field_seven}, \'{value}\') as {field_seven}, '
    'IFNULL({field_eight}, \'{value}\') as {field_eight}, '
    'IFNULL({field_nine}, \'{value}\') as {field_nine}, '
    'IFNULL({field_ten}, \'{value}\') as {field_ten}, '
    'IFNULL({field_eleven}, \'{value}\') as {field_eleven}, '
    'IFNULL({field_twelve}, \'{value}\') as {field_twelve} '
    'FROM `{project}.{dataset}.{table}`'
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
