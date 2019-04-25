# field lookup values
OBS_PII_NAME_FIRST = 1585596
OBS_PII_NAME_MIDDLE = 1585597
OBS_PII_NAME_LAST = 1585598
OBS_PII_EMAIL_ADDRESS = 1585260
OBS_PII_PHONE = 1585252
OBS_PII_STREET_ADDRESS_ONE = 1585246
OBS_PII_STREET_ADDRESS_TWO = 1585247
OBS_PII_STREET_ADDRESS_CITY = 1585248
OBS_PII_STREET_ADDRESS_STATE = 1585249
OBS_PII_STREET_ADDRESS_ZIP = 1585250
OBS_PII_CONSENT_PRIMARY_PHONE = None
OBS_EHR_BIRTH_DATETIME = 4083587
OBS_PII_BIRTH_DATETIME = 1585259

# response dictionary keys
EMAIL = 'email'
NAME = 'name'
FIRST_NAME = 'first'
MIDDLE_NAME = 'middle'
LAST_NAME = 'last'
ADDRESS = 'address'
STREET_ONE = 'street-one'
STREET_TWO = 'street-two'
CITY = 'city'
STATE = 'state'
ZIP = 'zip-code'
CONTACT_PHONE = 'contact-phone-number'
PRIMARY_PHONE = 'primary-phone'
BIRTHDATE = 'birthdate'

# DRC match responses
MATCH = "Match"
MISMATCH = "NoMatch"
MISSING = "Missing"
YES = "Yes"
NO = "No"

# Date format strings
FULL_DATETIME = '%Y-%m-%d %H:%M:%S%z'
DATE = '%Y-%m-%d'

# state abbreviations.  used to validate state abbreviations
STATE_ABBREVIATIONS = [
    'al', 'ak', 'az', 'ar', 'ca', 'co', 'ct', 'de', 'fl', 'ga', 'hi', 'id', 'il',
    'in', 'ia', 'ks', 'ky', 'la', 'me', 'md', 'ma', 'mi', 'mn', 'ms', 'mo', 'mt',
    'ne', 'nv', 'nh', 'nj', 'nm', 'ny', 'nc', 'nd', 'oh', 'ok', 'or', 'pa', 'ri',
    'sc', 'sd', 'tn', 'tx', 'ut', 'vt', 'va', 'wa', 'wv', 'wi', 'wy',
    # Commonwealth/Territory: 	Abbreviation:
    'as', 'dc', 'fm', 'gu', 'mh', 'mp', 'pw', 'pr', 'vi',
    # Military "State": 	Abbreviation:
    'aa', 'ae', 'ap',
]

ADDRESS_ABBREVIATIONS = {
    'aly': 'alley',
    'anx': 'annex',
    'apt': 'apartment',
    'ave': 'avenue',
    'bch': 'beach',
    'blvd': 'boulevard',
    'bnd': 'bend',
    'btm': 'bottom',
    'cir': 'circle',
    'ct': 'court',
    'co': 'county',
    'ctr': 'center',
    'dr': 'drive',
    'e': 'east',
    'expy': 'expressway',
    'hts': 'heights',
    'hwy': 'highway',
    'is': 'island',
    'jct': 'junction',
    'lk': 'lake',
    'ln': 'lane',
    'mtn': 'mountain',
    'n': 'north',
    'ne': 'northeast',
    'num': 'number',
    'nw': 'northwest',
    'pkwy': 'parkway',
    'pl': 'place',
    'plz': 'plaza',
    'po': 'post office',
    'rd': 'road',
    'rdg': 'ridge',
    'rr': 'rural route',
    'rm': 'room',
    's': 'south',
    'se': 'southeast',
    'sq': 'square',
    'st': 'street',
    'str': 'street',
    'sta': 'station',
    'ste': 'suite',
    'sw': 'southwest',
    'ter': 'terrace',
    'tpke': 'turnpike',
    'trl': 'trail',
    'vly': 'valley',
    'w': 'west',
    'way': 'way',
}

CITY_ABBREVIATIONS = {
    'st':  'saint',
    'afb': 'air force base',
}

################################################################################
#  Participant Matching Validation Queries
# Select all results from table
VALIDATION_RESULTS_VALUES = 'SELECT * FROM `{project}.{dataset}.{table}`'

# Select observation table attributes to validate
PPI_OBSERVATION_VALUES = (
    'SELECT person_id, observation_source_concept_id, value_as_string '
    'FROM `{project}.{dataset}.{table}` '
    'WHERE observation_source_concept_id={field_value} '
    'ORDER BY person_id'
)

# Select observation table attributes to validate
EHR_OBSERVATION_VALUES = (
    'SELECT person_id, observation_concept_id, value_as_string '
    'FROM `{project}.{data_set}.{table}` '
    'WHERE observation_concept_id={field_value} '
    'AND person_id IN ({person_id_csv}) '
    'ORDER BY person_id'
)

# Select observation table attributes to validate
ALL_PPI_OBSERVATION_VALUES = (
    'SELECT person_id, observation_source_concept_id, value_as_string '
    'FROM `{project}.{dataset}.{table}` '
    'WHERE observation_source_concept_id IN (' +
    ', '.join([str(OBS_PII_NAME_FIRST), str(OBS_PII_NAME_MIDDLE),
               str(OBS_PII_NAME_LAST), str(OBS_PII_EMAIL_ADDRESS),
               str(OBS_PII_PHONE), str(OBS_PII_STREET_ADDRESS_ONE),
               str(OBS_PII_STREET_ADDRESS_TWO), str(OBS_PII_STREET_ADDRESS_CITY),
               str(OBS_PII_STREET_ADDRESS_STATE), str(OBS_PII_STREET_ADDRESS_ZIP),
               str(OBS_PII_BIRTH_DATETIME)]) +
    ')'
)

# Select PII table values.
PII_VALUES = (
    'SELECT person_id, {field} '
    'FROM `{project}.{dataset}.{hpo_site_str}{table_suffix}`'
)

PII_LOCATION_VALUES = (
    'SELECT location_id, {field} '
    'FROM `{project}.{data_set}.location` '
    'WHERE location_id IN ({id_list})'
)

# Select EHR birth datetime
EHR_BIRTH_DATETIME_VALUES = (
    'SELECT person_id, observation_concept_id, value_as_string '
    'FROM `{project}.{data_set}.{table}` '
    'WHERE observation_concept_id={field} '
    'ORDER BY person_id'
)

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
    'orig.{field_eleven} IS NOT NULL THEN '
    '  DELETE '
    'WHEN MATCHED AND orig.{field_one} IS NULL AND orig.{field_two} IS NULL AND '
    'orig.{field_three} IS NULL AND orig.{field_four} IS NULL AND '
    'orig.{field_five} IS NULL AND orig.{field_six} IS NULL AND '
    'orig.{field_seven} IS NULL AND orig.{field_eight} IS NULL AND '
    'orig.{field_nine} IS NULL AND orig.{field_ten} IS NOT NULL AND '
    'orig.{field_eleven} IS NULL THEN '
    '  DELETE '
    'WHEN MATCHED AND orig.{field_one} IS NULL AND orig.{field_two} IS NULL AND '
    'orig.{field_three} IS NULL AND orig.{field_four} IS NULL AND '
    'orig.{field_five} IS NULL AND orig.{field_six} IS NULL AND '
    'orig.{field_seven} IS NULL AND orig.{field_eight} IS NULL AND '
    'orig.{field_nine} IS NOT NULL AND orig.{field_ten} IS NULL AND '
    'orig.{field_eleven} IS NULL THEN '
    '  DELETE '
    'WHEN MATCHED AND orig.{field_one} IS NULL AND orig.{field_two} IS NULL AND '
    'orig.{field_three} IS NULL AND orig.{field_four} IS NULL AND '
    'orig.{field_five} IS NULL AND orig.{field_six} IS NULL AND '
    'orig.{field_seven} IS NULL AND orig.{field_eight} IS NOT NULL AND '
    'orig.{field_nine} IS NULL AND orig.{field_ten} IS NULL AND '
    'orig.{field_eleven} IS NULL THEN '
    '  DELETE '
    'WHEN MATCHED AND orig.{field_one} IS NULL AND orig.{field_two} IS NULL AND '
    'orig.{field_three} IS NULL AND orig.{field_four} IS NULL AND '
    'orig.{field_five} IS NULL AND orig.{field_six} IS NULL AND '
    'orig.{field_seven} IS NOT NULL AND orig.{field_eight} IS NULL AND '
    'orig.{field_nine} IS NULL AND orig.{field_ten} IS NULL AND '
    'orig.{field_eleven} IS NULL THEN '
    '  DELETE '
    'WHEN MATCHED AND orig.{field_one} IS NULL AND orig.{field_two} IS NULL AND '
    'orig.{field_three} IS NULL AND orig.{field_four} IS NULL AND '
    'orig.{field_five} IS NULL AND orig.{field_six} IS NOT NULL AND '
    'orig.{field_seven} IS NULL AND orig.{field_eight} IS NULL AND '
    'orig.{field_nine} IS NULL AND orig.{field_ten} IS NULL AND '
    'orig.{field_eleven} IS NULL THEN '
    '  DELETE '
    'WHEN MATCHED AND orig.{field_one} IS NULL AND orig.{field_two} IS NULL AND '
    'orig.{field_three} IS NULL AND orig.{field_four} IS NULL AND '
    'orig.{field_five} IS NOT NULL AND orig.{field_six} IS NULL AND '
    'orig.{field_seven} IS NULL AND orig.{field_eight} IS NULL AND '
    'orig.{field_nine} IS NULL AND orig.{field_ten} IS NULL AND '
    'orig.{field_eleven} IS NULL THEN '
    '  DELETE '
    'WHEN MATCHED AND orig.{field_one} IS NULL AND orig.{field_two} IS NULL AND '
    'orig.{field_three} IS NULL AND orig.{field_four} IS NOT NULL AND '
    'orig.{field_five} IS NULL AND orig.{field_six} IS NULL AND '
    'orig.{field_seven} IS NULL AND orig.{field_eight} IS NULL AND '
    'orig.{field_nine} IS NULL AND orig.{field_ten} IS NULL AND '
    'orig.{field_eleven} IS NULL THEN '
    '  DELETE '
    'WHEN MATCHED AND orig.{field_one} IS NULL AND orig.{field_two} IS NULL AND '
    'orig.{field_three} IS NOT NULL AND orig.{field_four} IS NULL AND '
    'orig.{field_five} IS NULL AND orig.{field_six} IS NULL AND '
    'orig.{field_seven} IS NULL AND orig.{field_eight} IS NULL AND '
    'orig.{field_nine} IS NULL AND orig.{field_ten} IS NULL AND '
    'orig.{field_eleven} IS NULL THEN '
    '  DELETE '
    'WHEN MATCHED AND orig.{field_one} IS NULL AND orig.{field_two} IS NOT NULL '
    'AND orig.{field_three} IS NULL AND orig.{field_four} IS NULL AND '
    'orig.{field_five} IS NULL AND orig.{field_six} IS NULL AND '
    'orig.{field_seven} IS NULL AND orig.{field_eight} IS NULL AND '
    'orig.{field_nine} IS NULL AND orig.{field_ten} IS NULL AND '
    'orig.{field_eleven} IS NULL THEN '
    '  DELETE '
    'WHEN MATCHED AND orig.{field_one} IS NOT NULL AND orig.{field_two} IS '
    'NULL AND orig.{field_three} IS NULL AND orig.{field_four} IS NULL AND '
    'orig.{field_five} IS NULL AND orig.{field_six} IS NULL AND '
    'orig.{field_seven} IS NULL AND orig.{field_eight} IS NULL AND '
    'orig.{field_nine} IS NULL AND orig.{field_ten} IS NULL AND '
    'orig.{field_eleven} IS NULL THEN '
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
)

# Table names
OBSERVATION_TABLE = 'observation'
ID_MATCH_TABLE = 'id_match_table'
PII_EMAIL_TABLE = '_pii_email'
PII_PHONE_TABLE = '_pii_phone_number'
PII_ADDRESS_TABLE = '_pii_address'
PII_NAME_TABLE = '_pii_name'
VALIDATION_TABLE_SUFFIX = '_identity_match'

# Field names
OBS_CONCEPT_ID_FIELD = 'observation_concept_id'
OBS_SOURCE_CONCEPT_ID_FIELD = 'observation_source_concept_id'
PERSON_ID_FIELD = 'person_id'
STRING_VALUE_FIELD = 'value_as_string'
FIRST_NAME_FIELD = 'first_name'
MIDDLE_NAME_FIELD = 'middle_name'
LAST_NAME_FIELD = 'last_name'
EMAIL_FIELD = 'email'
PHONE_NUMBER_FIELD = 'phone_number'
ZIP_CODE_FIELD = 'zip'
STATE_FIELD = 'state'
CITY_FIELD = 'city'
ADDRESS_ONE_FIELD = 'address_1'
ADDRESS_TWO_FIELD = 'address_2'
LOCATION_ID_FIELD = 'location_id'
BIRTH_DATE_FIELD = 'birth_date'
ALGORITHM_FIELD = 'algorithm'
ADDRESS_MATCH_FIELD = 'address'

VALIDATION_FIELDS = [
    FIRST_NAME_FIELD, MIDDLE_NAME_FIELD, LAST_NAME_FIELD,
    EMAIL_FIELD, PHONE_NUMBER_FIELD, ZIP_CODE_FIELD, STATE_FIELD,
    CITY_FIELD, ADDRESS_ONE_FIELD, ADDRESS_TWO_FIELD,
    BIRTH_DATE_FIELD
]

# HPO dictionary keys
HPO_ID = 'hpo_id'

REPORT_TITLE = 'id-validation.csv'
