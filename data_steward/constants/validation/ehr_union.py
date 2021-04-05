import common
# This File consists of all the constants and sql queries from validation/ehr_union
VISIT_OCCURRENCE_ID = 'visit_occurrence_id'
CARE_SITE_ID = 'care_site_id'
PERSON_ID = 'person_id'
LOCATION_ID = 'location_id'

CONCEPT_CONSTANT_FACTOR = int(1e14)
HPO_CONSTANT_FACTOR = int(1e11)

# Starting factor to create ID space for person to observation mapped record
EHR_PERSON_TO_OBS_CONSTANT = 2 * common.ID_CONSTANT_FACTOR

# Starting factor to create ID spaces for each HPO (without RDR collisions)
EHR_ID_MULTIPLIER_START = 3

# person to observation (pto) constants
GENDER_CONCEPT_ID = 4135376
RACE_CONCEPT_ID = 4013886
DOB_CONCEPT_ID = 4083587
ETHNICITY_CONCEPT_ID = 4271761

GENDER_CONSTANT_FACTOR = 1 * CONCEPT_CONSTANT_FACTOR
RACE_CONSTANT_FACTOR = 2 * CONCEPT_CONSTANT_FACTOR
DOB_CONSTANT_FACTOR = 3 * CONCEPT_CONSTANT_FACTOR
ETHNICITY_CONSTANT_FACTOR = 4 * CONCEPT_CONSTANT_FACTOR
