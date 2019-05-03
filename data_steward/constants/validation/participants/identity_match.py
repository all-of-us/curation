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
OBS_PII_BIRTH_DATETIME = 1585259
OBS_PII_SEX = 1585845
SEX_CONCEPT_IDS = {1585846: 'male', 1585847: 'female', 1585848: 'intersex'}

# DRC match responses
MATCH = "Match"
MISMATCH = "NoMatch"
MISSING = "Missing"

# Date format strings
DATE = '%Y-%m-%d'
DRC_DATE_FORMAT = '%Y%m%d'
DRC_DATE_REGEX = '\d{8}'

# Table names
OBSERVATION_TABLE = 'observation'
PERSON_TABLE = 'person'
ID_MATCH_TABLE = 'id_match_table'
PII_EMAIL_TABLE = '_pii_email'
PII_PHONE_TABLE = '_pii_phone_number'
PII_ADDRESS_TABLE = '_pii_address'
PII_NAME_TABLE = '_pii_name'
EHR_PERSON_TABLE_SUFFIX = '_person'
VALIDATION_TABLE_SUFFIX = '_identity_match'

# Field names
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
GENDER_FIELD = 'gender_concept_id'
BIRTH_DATETIME_FIELD = 'birth_datetime'

REPORT_TITLE = 'id-validation.csv'
REPORT_DIRECTORY = 'drc-validations-{date}'

DESTINATION_DATASET_DESCRIPTION = '{version} {rdr_dataset} + {ehr_dataset}'
