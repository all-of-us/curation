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
CLINICAL = 'clinical'
ACHILLES = 'achilles'
CDM_COMPONENTS = [CLINICAL, VOCABULARY, ACHILLES]

# ID Spaces
#
# The following constants are added to values in all ID (or "primary key") fields to prevent
# collisions during union/combine phases

# Values for ID fields for each HPO are summed with a factor of ID_CONSTANT_FACTOR
ID_CONSTANT_FACTOR = 1000000000000000

# Added to value in all ID fields in records coming from the RDR
RDR_ID_CONSTANT = ID_CONSTANT_FACTOR

# Starting factor to create ID spaces for each HPO (without RDR collisions)
EHR_ID_MULTIPLIER_START = 2

PARTICIPANT_DIR = 'participant/'
IGNORE_DIRECTORIES = [PARTICIPANT_DIR]
DELIMITER = '\t'
LINE_TERMINATOR = '\n'
RAW_DATE_REGEX = r'\d{8}$'  # yyyymmdd
BQ_DATE_REGEX = r'\d{4}-\d{2}-\d{2}$'  # yyyy-mm-dd
TRANSFORM_FILES = 'transform_files'
APPEND_VOCABULARY = 'append_vocabulary'
APPEND_CONCEPTS = 'append_concepts'
ADD_AOU_GENERAL = 'add_aou_general'
ERRORS = 'errors'
AOU_GEN = 'AoU_General'
AOU_GEN_VOCABULARY_CONCEPT_ID = '2000000000'
AOU_GEN_VOCABULARY_REFERENCE = 'https://docs.google.com/document/d/10Gji9VW5-RTysM-yAbRa77rXqVfDfO2li2U4LxUQH9g'
OMOP_VOCABULARY_CONCEPT_ID = '44819096'
ERROR_APPENDING = 'Appending to {in_path} which already contains rows for ' + AOU_GEN