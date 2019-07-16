import cdm

SOURCE_VALUE_EHR_CONSENT = 'EHRConsentPII_ConsentPermission'
CONCEPT_ID_CONSENT_PERMISSION_YES = 1586100  # ConsentPermission_Yes
EHR_CONSENT_TABLE_ID = '_ehr_consent'
PERSON_TABLE = 'person'
PERSON_ID = 'person_id'
OBSERVATION_TABLE = 'observation'
FOREIGN_KEYS_FIELDS = ['visit_occurrence_id', 'location_id', 'care_site_id', 'provider_id']
RDR_TABLES_TO_COPY = ['person']
EHR_TABLES_TO_COPY = ['death']
DOMAIN_TABLES = list(set(cdm.tables_to_map()) - set(RDR_TABLES_TO_COPY + EHR_TABLES_TO_COPY))
TABLES_TO_PROCESS = RDR_TABLES_TO_COPY + EHR_TABLES_TO_COPY + DOMAIN_TABLES
LEFT_JOIN = (' LEFT JOIN'
             ' ('
             ' SELECT *'
             ' FROM ('
             ' SELECT'
             ' *,'
             ' row_number() OVER (PARTITION BY {prefix}.{field}, {prefix}.src_hpo_id ) '
             ' AS row_num'
             ' FROM {dataset_id}.{table} {prefix}'
             ' )'
             ' WHERE row_num = 1'
             ' ) {prefix}  ON t.{field} = {prefix}.src_{field}'
             ' AND m.src_dataset_id = {prefix}.src_dataset_id')