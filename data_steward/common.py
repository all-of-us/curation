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

# Vocabulary
CONCEPT = 'concept'
CONCEPT_ANCESTOR = 'concept_ancestor'
CONCEPT_CLASS = 'concept_class'
CONCEPT_RELATIONSHIP = 'concept_relationship'
CONCEPT_SYNONYM = 'concept_synonym'
DOMAIN = 'domain'
DRUG_STRENGTH = 'drug_strength'
RELATIONSHIP = 'relationship'
VOCABULARY = 'vocabulary'
VOCABULARY_TABLES = [CONCEPT, CONCEPT_ANCESTOR, CONCEPT_CLASS, CONCEPT_RELATIONSHIP, CONCEPT_SYNONYM, DOMAIN,
                     DRUG_STRENGTH, RELATIONSHIP, VOCABULARY]
# Achilles
ACHILLES_ANALYSIS = 'achilles_analysis'
ACHILLES_RESULTS = 'achilles_results'
ACHILLES_RESULTS_DIST = 'achilles_results_dist'
ACHILLES_TABLES = [ACHILLES_ANALYSIS, ACHILLES_RESULTS, ACHILLES_RESULTS_DIST]
ACHILLES_HEEL_RESULTS = 'achilles_heel_results'
ACHILLES_RESULTS_DERIVED = 'achilles_results_derived'
ACHILLES_HEEL_TABLES = [ACHILLES_HEEL_RESULTS, ACHILLES_RESULTS_DERIVED]
REQUIRED_TABLES = ['person']
REQUIRED_FILES = [table + '.csv' for table in REQUIRED_TABLES]
ACHILLES_EXPORT_PREFIX_STRING = "curation_report/data/"
IGNORE_STRING_LIST = [ACHILLES_EXPORT_PREFIX_STRING]
ACHILLES_EXPORT_DATASOURCES_JSON = ACHILLES_EXPORT_PREFIX_STRING + 'datasources.json'

# latest vocabulary dataset name in test and prod
VOCABULARY_DATASET = 'vocabulary20190423'
CLINICAL = 'clinical'
ACHILLES = 'achilles'
CDM_COMPONENTS = [CLINICAL, VOCABULARY, ACHILLES]
UNKNOWN_FILE = 'Unknown file'


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
MEASUREMENT_DOMAIN_CONCEPT_ID = 21
OBSERVATION_DOMAIN_CONCEPT_ID = 27
PERSON_DOMAIN_CONCEPT_ID = 56
OBSERVATION_TO_MEASUREMENT_CONCEPT_ID = 581410
MEASUREMENT_TO_OBSERVATION_CONCEPT_ID = 581411
PARENT_TO_CHILD_MEASUREMENT_CONCEPT_ID = 581436
CHILD_TO_PARENT_MEASUREMENT_CONCEPT_ID = 581437
DIASTOLIC_TO_SYSTOLIC_CONCEPT_ID = 46233682
SYSTOLIC_TO_DIASTOLIC_CONCEPT_ID = 46233683
VISIT_OCCURRENCE = 'visit_occurrence'
VISIT_OCCURRENCE_ID = 'visit_occurrence_id'
CARE_SITE = 'care_site'
CARE_SITE_ID = 'care_site_id'
PERSON = 'person'
PERSON_ID = 'person_id'
LOCATION = 'location'
LOCATION_ID = 'location_id'
FACT_RELATIONSHIP = 'fact_relationship'
LATEST_REPORTS_JSON = 'latest_reports.json'
LATEST_RESULTS_JSON = 'latest_results.json'
REPORT_FOR_ACHILLES = 'achilles'
REPORT_FOR_RESULTS = 'results'
LOG_YEAR = '2019'

DELIMITER = '\t'
LINE_TERMINATOR = '\n'
TRANSFORM_FILES = 'transform_files'
APPEND_VOCABULARY = 'append_vocabulary'
APPEND_CONCEPTS = 'append_concepts'
ADD_AOU_GENERAL = 'add_aou_general'
ERRORS = 'errors'
AOU_GEN_ID = 'AoU_General'
AOU_GEN_NAME = 'AoU_General'
AOU_GEN_VOCABULARY_CONCEPT_ID = '2000000000'
AOU_GEN_VOCABULARY_REFERENCE = 'https://docs.google.com/document/d/10Gji9VW5-RTysM-yAbRa77rXqVfDfO2li2U4LxUQH9g'
OMOP_VOCABULARY_CONCEPT_ID = '44819096'
ERROR_APPENDING = 'Appending to {in_path} which already contains rows for ' + AOU_GEN_ID
