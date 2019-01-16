import resources
import os

CDM_TABLES = resources.cdm_schemas().keys()
CDM_FILES = map(lambda t: t + '.csv', CDM_TABLES)
ACHILLES_INDEX_FILES = resources.achilles_index_files()
ALL_ACHILLES_INDEX_FILES = [name.split(resources.resource_path + os.sep)[1].strip() for name in ACHILLES_INDEX_FILES]
DATASOURCES_JSON = os.path.join(resources.achilles_index_path, 'data/datasources.json')
PII_TABLES = ['pii_name', 'pii_email', 'pii_phone_number', 'pii_address', 'pii_mrn']
PII_FILES = map(lambda t: t+'.csv', PII_TABLES)
RESULT_CSV = 'result.csv'
ERRORS_CSV = 'errors.csv'
WARNINGS_CSV = 'warnings.csv'
PROCESSED_TXT = 'processed.txt'
LOG_JSON = 'log.json'
ACHILLES_HEEL_REPORT = 'achillesheel'
PERSON_REPORT = 'person'
DATA_DENSITY_REPORT = 'datadensity'
ALL_REPORTS = [ACHILLES_HEEL_REPORT, PERSON_REPORT, DATA_DENSITY_REPORT]
ALL_REPORT_FILES = map(lambda s: s + '.json', ALL_REPORTS)
IGNORE_LIST = [RESULT_CSV, ERRORS_CSV, PROCESSED_TXT] + ALL_ACHILLES_INDEX_FILES
VOCABULARY_TABLES = ['concept', 'concept_ancestor', 'concept_class', 'concept_relationship', 'concept_synonym',
                     'domain', 'drug_strength', 'relationship', 'vocabulary']
REQUIRED_TABLES = ['person']
REQUIRED_FILES = [table + '.csv' for table in REQUIRED_TABLES]
ACHILLES_EXPORT_PREFIX_STRING = "curation_report/data/"
ACHILLES_EXPORT_DATASOURCES_JSON = ACHILLES_EXPORT_PREFIX_STRING + 'datasources.json'
VOCABULARY = 'vocabulary'
CLINICAL = 'clinical'
ACHILLES = 'achilles'
CDM_COMPONENTS = [CLINICAL, VOCABULARY, ACHILLES]

