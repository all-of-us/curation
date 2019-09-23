from constants.validation.participants import identity_match as id_match

# Select observation table attributes to validate
PPI_OBSERVATION_VALUES = (
    'SELECT person_id, observation_source_concept_id, value_as_string '
    'FROM `{project}.{dataset}.{table}` '
    'WHERE observation_source_concept_id={field_value} '
    'ORDER BY person_id'
)

# Select values from ehr person table
EHR_PERSON_VALUES = (
    'SELECT person_id, {field} '
    'FROM `{project}.{dataset}.{table}` '
)

# Select observation table attributes to validate
ALL_PPI_OBSERVATION_VALUES = (
    'SELECT person_id, observation_source_concept_id, value_as_string '
    'FROM `{project}.{dataset}.{table}` '
    'WHERE observation_source_concept_id IN ({pii_list})'
)

PII_CODES_LIST = [
    str(id_match.OBS_PII_NAME_FIRST), str(id_match.OBS_PII_NAME_MIDDLE),
    str(id_match.OBS_PII_NAME_LAST), str(id_match.OBS_PII_EMAIL_ADDRESS),
    str(id_match.OBS_PII_PHONE), str(id_match.OBS_PII_STREET_ADDRESS_ONE),
    str(id_match.OBS_PII_STREET_ADDRESS_TWO), str(id_match.OBS_PII_STREET_ADDRESS_CITY),
    str(id_match.OBS_PII_STREET_ADDRESS_STATE), str(id_match.OBS_PII_STREET_ADDRESS_ZIP),
    str(id_match.OBS_PII_BIRTH_DATETIME), str(id_match.OBS_PII_SEX)
]

# Select PII table values.
PII_VALUES = (
    'SELECT person_id, {field} '
    'FROM `{project}.{dataset}.{hpo_site_str}{table_suffix}`'
)

PII_LOCATION_VALUES = (
    'SELECT location_id, {field} '
    'FROM `{project}.{dataset}.location` '
    'WHERE location_id IN ({id_list})'
)

# Table names
OBSERVATION_TABLE = 'observation'
ID_MATCH_TABLE = 'id_match_table'

# Field names
PERSON_ID_FIELD = 'person_id'
LOCATION_ID_FIELD = 'location_id'
STRING_VALUE_FIELD = 'value_as_string'

# HPO dictionary keys
HPO_ID = 'hpo_id'
