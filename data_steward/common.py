import resources
import os

CDM_TABLES = resources.cdm_schemas().keys()
CDM_FILES = [table + '.csv' for table in CDM_TABLES]
ACHILLES_INDEX_FILES = resources.achilles_index_files()
ALL_ACHILLES_INDEX_FILES = [name.split(resources.resource_path + os.sep)[1].strip() for name in ACHILLES_INDEX_FILES]
DATASOURCES_JSON = os.path.join(resources.achilles_index_path, 'data/datasources.json')
PII_TABLES = ['pii_name', 'pii_email', 'pii_phone_number', 'pii_address', 'pii_mrn', 'participant_match']
AOU_REQUIRED = ['care_site', 'condition_occurrence', 'death', 'device_exposure', 'drug_exposure',
                'fact_relationship', 'location', 'measurement', 'note', 'observation', 'person',
                'procedure_occurrence', 'provider', 'specimen', 'visit_occurrence']
AOU_REQUIRED_FILES = [table + '.csv' for table in AOU_REQUIRED]
PII_FILES = [table + '.csv' for table in PII_TABLES]
SUBMISSION_FILES = AOU_REQUIRED_FILES + PII_FILES
RESULTS_HTML = 'results.html'
PROCESSED_TXT = 'processed.txt'
LOG_JSON = 'log.json'
ACHILLES_HEEL_REPORT = 'achillesheel'
PERSON_REPORT = 'person'
DATA_DENSITY_REPORT = 'datadensity'
ALL_REPORTS = [ACHILLES_HEEL_REPORT, PERSON_REPORT, DATA_DENSITY_REPORT]
ALL_REPORT_FILES = [report + '.json' for report in ALL_REPORTS]
IGNORE_LIST = [PROCESSED_TXT, RESULTS_HTML] + ALL_ACHILLES_INDEX_FILES
VOCABULARY_TABLES = ['concept', 'concept_ancestor', 'concept_class', 'concept_relationship', 'concept_synonym',
                     'domain', 'drug_strength', 'relationship', 'vocabulary']
REQUIRED_TABLES = ['person']
REQUIRED_FILES = [table + '.csv' for table in REQUIRED_TABLES]
ACHILLES_EXPORT_PREFIX_STRING = "curation_report/data/"
IGNORE_STRING_LIST = [ACHILLES_EXPORT_PREFIX_STRING]
ACHILLES_EXPORT_DATASOURCES_JSON = ACHILLES_EXPORT_PREFIX_STRING + 'datasources.json'
VOCABULARY = 'vocabulary'
CLINICAL = 'clinical'
ACHILLES = 'achilles'
CDM_COMPONENTS = [CLINICAL, VOCABULARY, ACHILLES]


# Validation Main Constant space
ACHILLES_HEEL_RESULTS_VALIDATION = '_achilles_heel_results'

# ID Spaces
#
# The following constants are added to values in all ID (or "primary key") fields to prevent
# collisions during union/combine phases

# Values for ID fields for each HPO are summed with a factor of ID_CONSTANT_FACTOR
ID_CONSTANT_FACTOR = 1000000000000000

CONCEPT_CONSTANT_FACTOR = 1000000000000

# Added to value in all ID fields in records coming from the RDR
RDR_ID_CONSTANT = ID_CONSTANT_FACTOR

# Starting factor to create ID space for person to observation mapped records
EHR_PERSON_TO_OBS_CONSTANT = 2 * ID_CONSTANT_FACTOR

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


PARTICIPANT_DIR = 'participant/'
IGNORE_DIRECTORIES = [PARTICIPANT_DIR]
