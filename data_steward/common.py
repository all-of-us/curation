# Python imports
import os

# Third-party import
import jinja2

# Project imports
from constants.bq_utils import VALIDATION_DATASET_REGEX
from constants.validation.participants.identity_match import REPORT_DIRECTORY_REGEX

# AOU required PII tables
PII_WILDCARD = 'pii*'
PII_NAME = 'pii_name'
PII_EMAIL = 'pii_email'
PII_PHONE_NUMBER = 'pii_phone_number'
PII_ADDRESS = 'pii_address'
PII_MRN = 'pii_mrn'
PARTICIPANT_MATCH = 'participant_match'
PII_TABLES = [
    PII_NAME, PII_EMAIL, PII_PHONE_NUMBER, PII_ADDRESS, PII_MRN,
    PARTICIPANT_MATCH
]

# DRC identity match
IDENTITY_MATCH = 'identity_match'

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
VISIT_DETAIL = 'visit_detail'
AOU_REQUIRED = [
    CARE_SITE, CONDITION_OCCURRENCE, DEATH, DEVICE_EXPOSURE, DRUG_EXPOSURE,
    FACT_RELATIONSHIP, LOCATION, MEASUREMENT, NOTE, OBSERVATION, PERSON,
    PROCEDURE_OCCURRENCE, PROVIDER, SPECIMEN, VISIT_OCCURRENCE, VISIT_DETAIL
]

# CATI Tables
SURVEY_CONDUCT = 'survey_conduct'
CATI_TABLES = AOU_REQUIRED + [SURVEY_CONDUCT]
QUESTIONNAIRE_RESPONSE_ADDITIONAL_INFO = 'questionnaire_response_additional_info'

# Standardized clinical data tables in OMOP. All should contain a person_id column. See
# https://github.com/OHDSI/CommonDataModel/wiki/Standardized-Clinical-Data-Tables

# Clinical tables which have a corresponding mapping table.
MAPPED_CLINICAL_DATA_TABLES = [
    VISIT_OCCURRENCE, CONDITION_OCCURRENCE, DRUG_EXPOSURE, MEASUREMENT,
    PROCEDURE_OCCURRENCE, OBSERVATION, DEVICE_EXPOSURE, SPECIMEN,
    SURVEY_CONDUCT, VISIT_DETAIL
]
# Clinical tables which do not have a corresponding mapping table.
UNMAPPED_CLINICAL_DATA_TABLES = [DEATH]
# All clinical tables.
CLINICAL_DATA_TABLES = MAPPED_CLINICAL_DATA_TABLES + UNMAPPED_CLINICAL_DATA_TABLES

# other CDM tables
ATTRIBUTE_DEFINITION = 'attribute_definition'
COHORT_DEFINITION = 'cohort_definition'
CONDITION_ERA = 'condition_era'
DRUG_ERA = 'drug_era'
DOSE_ERA = 'dose_era'
COST = 'cost'
DRUG_COST = 'drug_cost'
VISIT_COST = 'visit_cost'
DEVICE_COST = 'device_cost'
PROCEDURE_COST = 'procedure_cost'
PAYER_PLAN_PERIOD = 'payer_plan_period'
METADATA = 'metadata'

# Other Clinical tables
OBSERVATION_PERIOD = 'observation_period'
NOTE_NLP = 'note_nlp'

OTHER_CLINICAL_TABLES = [OBSERVATION_PERIOD, NOTE_NLP]

OTHER_CDM_TABLES = [
    ATTRIBUTE_DEFINITION, COHORT_DEFINITION, CONDITION_ERA, DRUG_ERA, DOSE_ERA,
    DRUG_COST, VISIT_COST, DEVICE_COST, PROCEDURE_COST, PAYER_PLAN_PERIOD, COST,
    METADATA
] + OTHER_CLINICAL_TABLES

CDM_TABLES = AOU_REQUIRED + OTHER_CDM_TABLES

# AoU custom tables
AOU_DEATH = 'aou_death'
AOU_CUSTOM_TABLES = [AOU_DEATH]

AOU_REQUIRED_FILES = [f'{table}.csv' for table in AOU_REQUIRED]
PII_FILES = [f'{table}.csv' for table in PII_TABLES]
NOTE_JSONL = 'note.jsonl'
SUBMISSION_FILES = AOU_REQUIRED_FILES + PII_FILES
RESULTS_HTML = 'results.html'
PROCESSED_TXT = 'processed.txt'
LOG_JSON = 'log.json'
ACHILLES_HEEL_REPORT = 'achillesheel'
PERSON_REPORT = 'person'
DATA_DENSITY_REPORT = 'datadensity'
ALL_REPORTS = [ACHILLES_HEEL_REPORT, PERSON_REPORT, DATA_DENSITY_REPORT]
ALL_REPORT_FILES = [f'{report}.json' for report in ALL_REPORTS]

# Wearables
ACTIVITY_SUMMARY = 'activity_summary'
HEART_RATE_INTRADAY = 'heart_rate_intraday'
HEART_RATE_SUMMARY = 'heart_rate_summary'
STEPS_INTRADAY = 'steps_intraday'
SLEEP_DAILY_SUMMARY = 'sleep_daily_summary'
SLEEP_LEVEL = 'sleep_level'
DEVICE = 'device'

# Wearables supplement
WEAR_STUDY = 'wear_study'

FITBIT_TABLES = [
    ACTIVITY_SUMMARY, HEART_RATE_INTRADAY, HEART_RATE_SUMMARY, STEPS_INTRADAY,
    SLEEP_DAILY_SUMMARY, SLEEP_LEVEL, DEVICE
]

# Vocabulary
CONCEPT = 'concept'
CONCEPT_ANCESTOR = 'concept_ancestor'
CONCEPT_CLASS = 'concept_class'
CONCEPT_RELATIONSHIP = 'concept_relationship'
CONCEPT_SYNONYM = 'concept_synonym'
DOMAIN = 'domain'
DRUG_STRENGTH = 'drug_strength'
RELATIONSHIP = 'relationship'
SOURCE_TO_CONCEPT_MAP = 'source_to_concept_map'
VOCABULARY = 'vocabulary'
VOCABULARY_TABLES = [
    CONCEPT, CONCEPT_ANCESTOR, CONCEPT_CLASS, CONCEPT_RELATIONSHIP,
    CONCEPT_SYNONYM, DOMAIN, DRUG_STRENGTH, RELATIONSHIP, SOURCE_TO_CONCEPT_MAP,
    VOCABULARY
]
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
ACHILLES_EXPORT_DATASOURCES_JSON = f'{ACHILLES_EXPORT_PREFIX_STRING}datasources.json'

# latest vocabulary dataset name in test and prod
VOCABULARY_DATASET = os.environ.get('VOCABULARY_DATASET')
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
DAILY_PARTICIPANTS = 'DailyParticipants/'
IGNORE_DIRECTORIES = [
    PARTICIPANT_DIR, REPORT_DIRECTORY_REGEX, VALIDATION_DATASET_REGEX,
    DAILY_PARTICIPANTS
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
AOU_CUSTOM_VOCABULARY_CONCEPT_ID = '2100000000'
AOU_CUSTOM_VOCABULARY_REFERENCE = 'https://precisionmedicineinitiative.atlassian.net/browse/DC-618'
OMOP_VOCABULARY_CONCEPT_ID = '44819096'
ERROR_APPENDING = 'Appending to {in_path} which already contains rows for {vocab_id}'
VERSION_TEMPLATE = 'insert version info here'
VOCABULARY_UPDATES = {
    AOU_GEN_ID: [
        AOU_GEN_ID, AOU_GEN_NAME, AOU_GEN_VOCABULARY_REFERENCE,
        VERSION_TEMPLATE, AOU_GEN_VOCABULARY_CONCEPT_ID
    ],
    AOU_CUSTOM_ID: [
        AOU_CUSTOM_ID, AOU_CUSTOM_NAME, AOU_CUSTOM_VOCABULARY_REFERENCE,
        VERSION_TEMPLATE, AOU_CUSTOM_VOCABULARY_CONCEPT_ID
    ]
}

COMBINED = 'combined'
UNIONED = 'unioned'
UNIONED_EHR = 'unioned_ehr'
DEID = 'deid'
EHR = 'ehr'
RDR = 'rdr'
RELEASE = 'release'
FITBIT = 'fitbit'
OTHER = 'other'
SANDBOX = 'sandbox'

MAPPING = 'mapping'
MAPPING_PREFIX = '_mapping_'
EXT = 'ext'
EXT_SUFFIX = '_ext'

DEID_MAP = '_deid_map'
MAX_DEID_DATE_SHIFT = 364
COPE_SURVEY_MAP = 'cope_survey_semantic_version_map'
EHR_CONSENT_VALIDATION = 'consent_validation'
WEAR_CONSENT = 'wear_consent'

# pipeline_tables dataset and contents
PIPELINE_TABLES = 'pipeline_tables'
SITE_MASKING_TABLE_ID = 'site_maskings'
PID_RID_MAPPING = 'pid_rid_mapping'
PRIMARY_PID_RID_MAPPING = 'primary_pid_rid_mapping'
IDENTICAL_LABS_LOOKUP_TABLE = 'identical_labs_modification'
ZIP3_LOOKUP = 'zip3_lookup'
ZIP3_SES_MAP = 'zip3_ses_map'
DIGITAL_HEALTH_SHARING_STATUS = 'digital_health_sharing_status'
HEALTH_INSURANCE_PIDS = 'health_insurance_pids'
WEARABLES_DEVICE_ID_MASKING = 'wearables_device_id_masking'

ZIP_CODE_AGGREGATION_MAP = 'zip_code_aggregation_map'
DEID_QUESTIONNAIRE_RESPONSE_MAP = '_deid_questionnaire_response_map'

# Participant Summary
EHR_OPS = 'ehr_ops'
DRC_OPS = 'drc_ops'
PS_API_VALUES = 'ps_api_values'

# Dataset labels
DE_IDENTIFIED = 'de_identified'

# src_id tables from RDR
SRC_ID_TABLES = [
    CARE_SITE, CONDITION_ERA, CONDITION_OCCURRENCE, COPE_SURVEY_MAP, COST,
    DEATH, DEVICE_EXPOSURE, DOSE_ERA, DRUG_ERA, DRUG_EXPOSURE,
    FACT_RELATIONSHIP, LOCATION, MEASUREMENT, METADATA, NOTE_NLP, OBSERVATION,
    OBSERVATION_PERIOD, PAYER_PLAN_PERIOD, PERSON, PID_RID_MAPPING,
    PROCEDURE_OCCURRENCE, PROVIDER, QUESTIONNAIRE_RESPONSE_ADDITIONAL_INFO,
    SURVEY_CONDUCT, VISIT_DETAIL, VISIT_OCCURRENCE, EHR_CONSENT_VALIDATION
]

# JINJA
JINJA_ENV = jinja2.Environment(
    # block tags on their own lines
    # will not cause extra white space
    trim_blocks=True,
    lstrip_blocks=True,
    # syntax highlighting should be better
    # with these comment delimiters
    comment_start_string='--',
    comment_end_string=' --',
    # in jinja2 autoescape is for html; jinjasql supports autoescape for sql
    # TODO Look into jinjasql for sql templating
    autoescape=False)

# Google scopes for running CDR pipeline stages
CDR_SCOPES = [
    'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/devstorage.read_write',
    'https://www.googleapis.com/auth/trace.append'
]

# Default Concept valid_start_date
DEFAULT_CONCEPT_VALID_START_DATE = '1970-01-01'

# OS imports
BIGQUERY_DATASET_ID = os.environ.get('BIGQUERY_DATASET_ID')
UNIONED_DATASET_ID = os.environ.get('UNIONED_DATASET_ID')
RDR_DATASET_ID = os.environ.get('RDR_DATASET_ID')
COMBINED_DATASET_ID = os.environ.get('COMBINED_DATASET_ID')
