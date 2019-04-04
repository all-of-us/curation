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
MATCH = "matches"
MISMATCH = "did not match"

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
        'SELECT person_id, observation_concept_id, value_as_string '
        'FROM {dataset}.{table} '
        'WHERE observation_concept_id IN (' +
        ', '.join([str(OBS_PII_NAME_FIRST), str(OBS_PII_NAME_MIDDLE),
                   str(OBS_PII_NAME_LAST), str(OBS_PII_EMAIL_ADDRESS),
                   str(OBS_PII_PHONE), str(OBS_PII_STREET_ADDRESS_ONE),
                   str(OBS_PII_STREET_ADDRESS_TWO), str(OBS_PII_STREET_ADDRESS_CITY),
                   str(OBS_PII_STREET_ADDRESS_STATE), str(OBS_PII_STREET_ADDRESS_ZIP),
                   str(OBS_PII_BIRTH_DATETIME), str(OBS_EHR_BIRTH_DATETIME)]) +
        ') ORDER BY observation_concept_id, person_id'
)
# Select PII table email.
PII_EMAIL_VALUES = (
    'SELECT person_id, email '
    'FROM {dataset}.{hpo_site_str}_pii_email'
)
# Select PII table phone number.
PII_PHONE_NUMBER_VALUES = (
    'SELECT person_id, phone_number '
    'FROM {dataset}.{hpo_site_str}_pii_phone_number'
)
# Select PII table location_id.  OMOP location id.
PII_LOCATION_IDS = (
    'SELECT person_id, location_id '
    'FROM {dataset}.{hpo_site_str}_pii_address'
)
PII_LOCATION_VALUES = (
    'SELECT location_id, address_1, address_2, city, state, zip '
    'FROM {dataset}.location'
)

# Select PII table name
PII_NAME_VALUES = (
    'SELECT person_id, first_name, middle_name, last_name '
    'FROM {dataset}.{hpo_site_str}_pii_name'
)
