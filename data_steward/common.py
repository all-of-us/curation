import resources
import os

CDM_TABLES = resources.cdm_schemas().keys()
CDM_FILES = map(lambda t: t + '.csv', CDM_TABLES)
ACHILLES_INDEX_FILES = resources.achilles_index_files()
ALL_ACHILLES_INDEX_FILES = [name.split(resources.resource_path + os.sep)[1].strip() for name in ACHILLES_INDEX_FILES]
DATASOURCES_JSON = os.path.join(resources.achilles_index_path, 'data/datasources.json')
PII_TABLES = ['pii_name', 'pii_email', 'pii_phone_number', 'pii_address', 'pii_mrn']
AOU_REQUIRED = ['care_site', 'condition_occurrence', 'death', 'device_exposure', 'drug_exposure',
                'fact_relationship', 'location', 'measurement', 'note', 'observation', 'person',
                'procedure_occurrence', 'provider', 'specimen', 'visit_occurrence']
AOU_REQUIRED_FILES = map(lambda t: t + '.csv', AOU_REQUIRED)
PII_FILES = map(lambda t: t + '.csv', PII_TABLES)
SUBMISSION_FILES = AOU_REQUIRED_FILES + PII_FILES
RESULTS_HTML = 'results.html'
PROCESSED_TXT = 'processed.txt'
LOG_JSON = 'log.json'
ACHILLES_HEEL_REPORT = 'achillesheel'
PERSON_REPORT = 'person'
DATA_DENSITY_REPORT = 'datadensity'
ALL_REPORTS = [ACHILLES_HEEL_REPORT, PERSON_REPORT, DATA_DENSITY_REPORT]
ALL_REPORT_FILES = map(lambda s: s + '.json', ALL_REPORTS)
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

# ID Spaces
#
# The following constants are added to values in all ID (or "primary key") fields to prevent
# collisions during union/combine phases

# Values for ID fields for each HPO are summed with a factor of ID_CONSTANT_FACTOR
ID_CONSTANT_FACTOR = 1000000000000000

KEY_CONSTANT_FACTOR = 1000000000000

# Added to value in all ID fields in records coming from the RDR
RDR_ID_CONSTANT = ID_CONSTANT_FACTOR

# Starting factor to create ID space for person to observation mapped records
EHR_PERSON_TO_OBS_CONSTANT = 2 * ID_CONSTANT_FACTOR

# Starting factor to create ID spaces for each HPO (without RDR collisions)
EHR_ID_MULTIPLIER_START = 3

# person to observation (pto) constants

pto_concept_id = dict()
# concept ids
pto_concept_id['gender'] = 4135376
pto_concept_id['race'] = 4013886
pto_concept_id['dob'] = 4083587
pto_concept_id['ethnicity'] = 4271761

pto_concept_offset = dict()
# concept offsets
demographic_concepts = ['gender', 'race', 'dob', 'ethnicity']
for i, concept in enumerate(demographic_concepts):
    pto_concept_offset[concept] = (i+1) * KEY_CONSTANT_FACTOR


PARTICIPANT_DIR = 'participant/'
IGNORE_DIRECTORIES = [PARTICIPANT_DIR]
