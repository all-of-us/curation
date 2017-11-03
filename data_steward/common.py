import resources

CDM_TABLES = set([r['table_name'] for r in resources.cdm_csv()])
CDM_FILES = map(lambda t: t + '.csv', CDM_TABLES)
ACHILLES_INDEX_FILES = resources.achilles_index_files()
RESULT_CSV = 'result.csv'
ERRORS_CSV = 'errors.csv'
WARNINGS_CSV = 'warnings.csv'
LOG_JSON = 'log.json'
ACHILLES_HEEL_REPORT = 'achillesheel'
PERSON_REPORT = 'person'
DATA_DENSITY_REPORT = 'datadensity'
ALL_REPORTS = [ACHILLES_HEEL_REPORT, PERSON_REPORT, DATA_DENSITY_REPORT]
ALL_REPORT_FILES = map(lambda s: s + '.json', ALL_REPORTS)
IGNORE_LIST = [RESULT_CSV, ERRORS_CSV, WARNINGS_CSV] + ALL_REPORT_FILES
VOCABULARY_TABLES = ['concept', 'concept_ancestor', 'concept_class', 'concept_relationship', 'concept_synonym',
                     'domain', 'drug_strength', 'relationship', 'vocabulary']
REQUIRED_TABLES = ['person', 'condition_occurrence', 'visit_occurrence', 'procedure_occurrence', 'measurement',
                   'drug_exposure']
REQUIRED_FILES = [table + '.csv' for table in REQUIRED_TABLES]
