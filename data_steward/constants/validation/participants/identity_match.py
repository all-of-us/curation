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
    '#': 'number',
    'apt': 'apartment',
    'ave': 'avenue',
    'blvd': 'boulevard',
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
    'rd': 'road',
    'rdg': 'ridge',
    'rm': 'room',
    's': 'south',
    'se': 'southeast',
    'sq': 'square',
    'st': 'street',
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

################################################################################
#  Participant Matching Validation Queries
# Select observation table attributes to validate
PPI_OBSERVATION_VALUES = (
    'SELECT person_id, observation_source_concept_id, value_as_string '
    #'FROM `{project}.lrwb_combined{date_string}.{table}` '
    'FROM `{project}.{dataset}.{table}` '
    'WHERE observation_source_concept_id={field_value} '
    'ORDER BY person_id'
)

# Select observation table attributes to validate
EHR_OBSERVATION_VALUES = (
    'SELECT person_id, observation_concept_id, value_as_string '
    'FROM `{project}.lrwb_combined{date_string}.{table}` '
    'WHERE observation_concept_id={field_value} '
    'AND person_id IN ({person_id_csv}) '
    'ORDER BY person_id'
)

# Select observation table attributes to validate
ALL_PPI_OBSERVATION_VALUES = (
    'SELECT person_id, observation_source_concept_id, value_as_string '
    'FROM `{project}.lrwb_combined{date_string}.{table}` '
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
    'FROM `{project}.lrwb_combined{date_string}.location` '
    'WHERE location_id IN ({id_list})'
)

# Select EHR birth datetime
EHR_BIRTH_DATETIME_VALUES = (
    'SELECT person_id, observation_concept_id, value_as_string '
    'FROM `{project}.lrwb_combined{date_string}.{table}` '
    'WHERE observation_concept_id={field} '
    'ORDER BY person_id'
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
OBS_CONCEPT_ID = 'observation_concept_id'
OBS_SOURCE_CONCEPT_ID = 'observation_source_concept_id'
PERSON_ID = 'person_id'
STRING_VALUE = 'value_as_string'
FIRST_NAME = 'first_name'
MIDDLE_NAME = 'middle_name'
LAST_NAME = 'last_name'
EMAIL_FIELD = 'email'
PHONE_NUMBER_FIELD = 'phone_number'
ZIP_CODE_FIELD = 'zip'
STATE_FIELD = 'state'
CITY_FIELD = 'city'
ADDRESS_ONE_FIELD = 'address_1'
ADDRESS_TWO_FIELD = 'address_2'
LOCATION_ID_FIELD = 'location_id'

# HPO dictionary keys
HPO_ID = 'hpo_id'

