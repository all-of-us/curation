from constants.bq_utils import VALIDATION_DATASET_REGEX
from constants.validation.participants.identity_match import REPORT_DIRECTORY_REGEX


# AOU required PII tables
PII_NAME = 'pii_name'
PII_EMAIL = 'pii_email'
PII_PHONE_NUMBER = 'pii_phone_number'
PII_ADDRESS = 'pii_address'
PII_MRN = 'pii_mrn'
PARTICIPANT_MATCH = 'participant_match'
PII_TABLES = [PII_NAME, PII_EMAIL, PII_PHONE_NUMBER, PII_ADDRESS, PII_MRN, PARTICIPANT_MATCH]

# AOU required CDM tables
CARE_SITE = 'care_site'
CONDITION_OCCURRENCE = 'condition_occurrence'
DEATH = 'death'
DEVICE_EXPOSURE = 'device_exposure'
DRUG_EXPOSURE = 'drug_exposure'
FACT_RELATIONSHIP = 'fact_relationship'
LOCATION = 'location'
MEASUREMENT = 'measurement'
NOTE = 'note'
OBSERVATION = 'observation'
PERSON = 'person'
PROCEDURE_OCCURRENCE = 'procedure_occurrence'
PROVIDER = 'provider'
SPECIMEN = 'specimen'
VISIT_OCCURRENCE = 'visit_occurrence'
AOU_REQUIRED = [CARE_SITE, CONDITION_OCCURRENCE, DEATH, DEVICE_EXPOSURE, DRUG_EXPOSURE,
                FACT_RELATIONSHIP, LOCATION, MEASUREMENT, NOTE, OBSERVATION, PERSON,
                PROCEDURE_OCCURRENCE, PROVIDER, SPECIMEN, VISIT_OCCURRENCE]

# Standardized clinical data tables in OMOP. All should contain a person_id column. See
# https://github.com/OHDSI/CommonDataModel/wiki/Standardized-Clinical-Data-Tables

# Clinical tables which do not have a corresponding mapping table.
MAPPED_CLINICAL_DATA_TABLES = [VISIT_OCCURRENCE, CONDITION_OCCURRENCE,
                               DRUG_EXPOSURE, MEASUREMENT, PROCEDURE_OCCURRENCE,
                               OBSERVATION, DEVICE_EXPOSURE, SPECIMEN]
# Clinical tables which do not have a corresponding mapping table.
UNMAPPED_CLINICAL_DATA_TABLES = [DEATH]
# All clinical tables.
CLINICAL_DATA_TABLES = MAPPED_CLINICAL_DATA_TABLES + UNMAPPED_CLINICAL_DATA_TABLES

# other CDM tables
OBSERVATION_PERIOD = 'observation_period'
DRUG_ERA = 'drug_era'
CONDITION_ERA = 'condition_era'


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

# fact relationship id constants
MEASUREMENT_DOMAIN_CONCEPT_ID = 21
OBSERVATION_DOMAIN_CONCEPT_ID = 27
PERSON_DOMAIN_CONCEPT_ID = 56
# ID Spaces
#
# The following constants are added to values in all ID (or "primary key") fields to prevent
# collisions during union/combine phases

# Values for ID fields for each HPO are summed with a factor of ID_CONSTANT_FACTOR
ID_CONSTANT_FACTOR = 1000000000000000


# Added to value in all ID fields in records coming from the RDR
RDR_ID_CONSTANT = ID_CONSTANT_FACTOR

PARTICIPANT_DIR = 'participant/'
IGNORE_DIRECTORIES = [
    PARTICIPANT_DIR,
    REPORT_DIRECTORY_REGEX,
    VALIDATION_DATASET_REGEX,
]

OBSERVATION_TO_MEASUREMENT_CONCEPT_ID = 581410
MEASUREMENT_TO_OBSERVATION_CONCEPT_ID = 581411
PARENT_TO_CHILD_MEASUREMENT_CONCEPT_ID = 581436
CHILD_TO_PARENT_MEASUREMENT_CONCEPT_ID = 581437
DIASTOLIC_TO_SYSTOLIC_CONCEPT_ID = 46233682
SYSTOLIC_TO_DIASTOLIC_CONCEPT_ID = 46233683

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
ADD_AOU_VOCABS = 'add_aou_vocabs'
ERRORS = 'errors'
AOU_GEN_ID = 'AoU_General'
AOU_GEN_NAME = 'AoU_General'
AOU_GEN_VOCABULARY_CONCEPT_ID = '2000000000'
AOU_GEN_VOCABULARY_REFERENCE = 'https://docs.google.com/document/d/10Gji9VW5-RTysM-yAbRa77rXqVfDfO2li2U4LxUQH9g'
AOU_CUSTOM_ID = 'AoU_Custom'
AOU_CUSTOM_NAME = 'AoU_Custom'
AOU_CUSTOM_VOCABULARY_CONCEPT_ID = '3000000000'
AOU_CUSTOM_VOCABULARY_REFERENCE = 'https://precisionmedicineinitiative.atlassian.net/browse/DC-618'
OMOP_VOCABULARY_CONCEPT_ID = '44819096'
ERROR_APPENDING = 'Appending to {in_path} which already contains rows for {vocab_id}'
VERSION_TEMPLATE = 'insert version info here'
VOCABULARY_UPDATES = {
    AOU_GEN_ID: [AOU_GEN_ID, AOU_GEN_NAME, AOU_GEN_VOCABULARY_CONCEPT_ID, VERSION_TEMPLATE, AOU_GEN_VOCABULARY_REFERENCE],
    AOU_CUSTOM_ID: [AOU_CUSTOM_ID, AOU_CUSTOM_NAME, AOU_CUSTOM_VOCABULARY_CONCEPT_ID, VERSION_TEMPLATE, AOU_CUSTOM_VOCABULARY_REFERENCE]
}