import resources

CDM_TABLES = set([r['table_name'] for r in resources.cdm_csv()])
CDM_FILES = map(lambda t: t + '.csv', CDM_TABLES)
