# This File consists of all the constants and sql queries from validation/main
UNION_ALL = '''

        UNION ALL

'''
HEEL_ERROR_FAIL_MESSAGE = 'There was an error while running Achilles. Please check back in a few hours.'
NULL_MESSAGE = '-'
ACHILLES_HEEL_RESULTS_TABLE = 'achilles_heel_results'
DRUG_CHECK_TABLE = 'drug_exposure'

# results.html check mark codes and colors
RESULT_FAIL_CODE = '&#x2718'
RESULT_PASS_CODE = '&#x2714'
RESULT_FAIL_COLOR = 'red'
RESULT_PASS_COLOR = 'green'

# datetime format
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S %Z'

# lag before submission is processed
SUBMISSION_LAG_TIME_MINUTES = 5

# Table Headers
RESULT_FILE_HEADERS = ["File Name", "Found", "Parsed", "Loaded"]
ERROR_FILE_HEADERS = ["File Name", "Message"]
DRUG_CHECK_HEADERS = [
    'Counts by Drug class', 'Drug Class Concept Name', 'Drug Class',
    'Percentage', 'Drug Class Concept ID'
]
HEEL_ERROR_HEADERS = ['Record Count', 'Heel Error', 'Analysis ID', 'Rule ID']
DUPLICATE_IDS_HEADERS = ['Table Name', 'Duplicate ID Count']

# Used in get_heel_errors_in_results_html()
HEEL_ERROR_QUERY_VALIDATION = '''
    SELECT
        analysis_id AS analysis_id,
        achilles_heel_warning AS heel_error,
        rule_id AS rule_id,
        record_count AS record_count
    FROM `{project_id}.{dataset_id}.{table_id}`
    WHERE achilles_heel_warning LIKE 'ERROR:%'
    ORDER BY
        record_count DESC,
        analysis_id
    '''
HEEL_ERROR_FAIL_ROWS = [(NULL_MESSAGE, HEEL_ERROR_FAIL_MESSAGE, NULL_MESSAGE,
                         NULL_MESSAGE)]

EHR_OPS_METRICS = "ehr_ops_metrics_staging"
EHR_RDR_PARTICIPANT = "mv_ehr_rdr_participant"
DATAVIEW_ID = "rdr_ops_data_view"
PARTICIPANT_STATUS = "pdr_ehr_participant_status"
PARTICIPANT_PATIENT_STATUS = "v_pdr_participant_patient_status"
PARTICIPANT_PM = "v_pdr_participant_pm"
PARTICIPANT_BIOBANK_ORDER = "v_pdr_participant_biobank_order"
ORG_HPO_MAPPING = "mv_org_hpo_mapping"

PARTICIPANT_STATS_QUERY = """
WITH pt_w_ehr AS (
  SELECT DISTINCT(person_id) FROM `{project_id}.{dataset_id}.{hpo_id}_condition_occurrence`
  UNION DISTINCT
  SELECT DISTINCT(person_id) FROM `{project_id}.{dataset_id}.{hpo_id}_drug_exposure`
  UNION DISTINCT
  SELECT DISTINCT(person_id) FROM `{project_id}.{dataset_id}.{hpo_id}_procedure_occurrence`
  UNION DISTINCT
  SELECT DISTINCT(person_id) FROM `{project_id}.{dataset_id}.{hpo_id}_measurement`
  UNION DISTINCT
  SELECT DISTINCT(person_id) FROM `{project_id}.{dataset_id}.{hpo_id}_observation`
  UNION DISTINCT
  SELECT DISTINCT(person_id) FROM `{project_id}.{dataset_id}.{hpo_id}_visit_occurrence`
  UNION DISTINCT
  SELECT DISTINCT(person_id) FROM `{project_id}.{dataset_id}.{hpo_id}_device_exposure`
  UNION DISTINCT
  SELECT DISTINCT(person_id) FROM `{project_id}.{dataset_id}.{hpo_id}_death`
  UNION DISTINCT
  SELECT DISTINCT(person_id) FROM `{project_id}.{dataset_id}.{hpo_id}_specimen`),
paired_pt AS (
  SELECT DISTINCT participant_id, ORGANIZATION,
  FROM `{ehr_project_id}.{ehr_ops_metrics}.{ehr_rdr_participant}`
  WHERE ORGANIZATION NOT LIKE 'CARE%OMOP%'),
ehr_consent AS (
  SELECT pdr_cons.participant_id, CASE WHEN consent_for_electronic_health_records = 'yes' THEN 1 ELSE 0 END AS ehr_consent_yes_flag
  FROM `{pdr_project_id}.{dataview_id}.{participant_status}` pdr_cons),
ps_status AS (
  SELECT DISTINCT participant_id, patient_status, RANK() OVER (PARTITION BY participant_id ORDER BY patient_status_modified DESC) ps_order
  FROM `{pdr_project_id}.{dataview_id}.{participant_patient_status}`),
pm_status AS (
  SELECT DISTINCT pm.participant_id, pm.pm_status, RANK() OVER (PARTITION BY pm.participant_id ORDER BY pm_finalized DESC) pm_order
  FROM `{pdr_project_id}.{dataview_id}.{participant_pm}` pm),
bio_status AS (
  SELECT participant_id, bbo_collection_method
  FROM `{pdr_project_id}.{dataview_id}.{participant_biobank_order}`)
SELECT DISTINCT 
  p.person_id,
  CASE WHEN pwe.person_id IS NOT NULL THEN 1 ELSE 0 END AS ehr_data_available,
  CASE WHEN LOWER(hpo_map.HPO_ID) = '{hpo_id}' THEN 1 ELSE 0 END AS hpo_paired_participant,
  pp.ORGANIZATION,
  e.ehr_consent_yes_flag,
  ps_status.patient_status,
  pm_status.pm_status,
  bio_status.bbo_collection_method
FROM `{project_id}.{dataset_id}.{hpo_id}_person` p
LEFT JOIN pt_w_ehr pwe USING (person_id)
LEFT JOIN paired_pt pp ON p.person_id = pp.participant_id
LEFT JOIN ehr_consent e ON p.person_id = e.participant_id
LEFT JOIN ps_status ON p.person_id = ps_status.participant_id AND ps_order = 1
LEFT JOIN pm_status ON p.person_id = pm_status.participant_id AND pm_order = 1
LEFT JOIN bio_status ON p.person_id = bio_status.participant_id AND bbo_collection_method = 'ON_SITE'
LEFT JOIN `{ehr_project_id}.{ehr_ops_metrics}.{org_hpo_mapping}` hpo_map ON pp.ORGANIZATION = hpo_map.Org_ID
"""

PARTICIPANT_STATS_SUMMARY_QUERY = """
WITH pt_w_ehr AS (
  SELECT DISTINCT(person_id) FROM `{project_id}.{dataset_id}.{hpo_id}_condition_occurrence`
  UNION DISTINCT
  SELECT DISTINCT(person_id) FROM `{project_id}.{dataset_id}.{hpo_id}_drug_exposure`
  UNION DISTINCT
  SELECT DISTINCT(person_id) FROM `{project_id}.{dataset_id}.{hpo_id}_procedure_occurrence`
  UNION DISTINCT
  SELECT DISTINCT(person_id) FROM `{project_id}.{dataset_id}.{hpo_id}_measurement`
  UNION DISTINCT
  SELECT DISTINCT(person_id) FROM `{project_id}.{dataset_id}.{hpo_id}_observation`
  UNION DISTINCT
  SELECT DISTINCT(person_id) FROM `{project_id}.{dataset_id}.{hpo_id}_visit_occurrence`
  UNION DISTINCT
  SELECT DISTINCT(person_id) FROM `{project_id}.{dataset_id}.{hpo_id}_device_exposure`
  UNION DISTINCT
  SELECT DISTINCT(person_id) FROM `{project_id}.{dataset_id}.{hpo_id}_death`
  UNION DISTINCT
  SELECT DISTINCT(person_id) FROM `{project_id}.{dataset_id}.{hpo_id}_specimen`),
paired_pt AS (
  SELECT DISTINCT participant_id, ORGANIZATION
  FROM `{ehr_project_id}.{ehr_ops_metrics}.{ehr_rdr_participant}`
  WHERE ORGANIZATION NOT LIKE 'CARE%OMOP%'),
ehr_consent AS (
  SELECT pdr_cons.participant_id, CASE WHEN consent_for_electronic_health_records = 'yes' THEN 1 ELSE 0 END AS ehr_consent_yes_flag
  FROM `{pdr_project_id}.{dataview_id}.{participant_status}` pdr_cons),
ps_status AS (
  SELECT DISTINCT participant_id, patient_status, RANK() OVER (PARTITION BY participant_id ORDER BY patient_status_modified DESC) ps_order
  FROM `{pdr_project_id}.{dataview_id}.{participant_patient_status}`),
pm_status AS (
  SELECT DISTINCT pm.participant_id, pm.pm_status, RANK() OVER (PARTITION BY pm.participant_id ORDER BY pm_finalized DESC) pm_order
  FROM `{pdr_project_id}.{dataview_id}.{participant_pm}` pm),
bio_status AS (
  SELECT participant_id, bbo_collection_method
  FROM `{pdr_project_id}.{dataview_id}.{participant_biobank_order}`)
SELECT
  SUM(CASE WHEN pwe.person_id IS NOT NULL THEN 1 ELSE 0 END) AS ehr_data_available,
  SUM(CASE WHEN LOWER(hpo_map.HPO_ID) = '{hpo_id}' THEN 1 ELSE 0 END) AS hpo_paired_participant,
  SUM(e.ehr_consent_yes_flag) AS ehr_consent_yes,
  SUM(CASE WHEN ps_status.patient_status = "YES" THEN 1 ELSE 0 END) AS patient_status_yes,
  SUM(CASE WHEN pm_status.pm_status = "COMPLETED" THEN 1 ELSE 0 END) AS physical_measurement_completed,
  SUM(CASE WHEN bio_status.bbo_collection_method = "ON_SITE" THEN 1 ELSE 0 END) AS biospecimen_on_site
FROM `{project_id}.{dataset_id}.{hpo_id}_person` p
LEFT JOIN pt_w_ehr pwe USING (person_id)
LEFT JOIN paired_pt pp ON p.person_id = pp.participant_id
LEFT JOIN ehr_consent e ON p.person_id = e.participant_id
LEFT JOIN ps_status ON p.person_id = ps_status.participant_id AND ps_order = 1
LEFT JOIN pm_status ON p.person_id = pm_status.participant_id AND pm_order = 1
LEFT JOIN bio_status ON p.person_id = bio_status.participant_id AND bbo_collection_method = 'ON_SITE'
LEFT JOIN `{ehr_project_id}.{ehr_ops_metrics}.{org_hpo_mapping}` hpo_map ON pp.ORGANIZATION = hpo_map.Org_ID
"""

# Used in get_drug_checks_in_results_html()
DRUG_CHECKS_QUERY_VALIDATION = '''
SELECT
  init.*,
  CASE
    WHEN ( SELECT COUNT(*) FROM `{project_id}.{dataset_id}.{table_id}`) > 0
    THEN CONCAT(CAST(ROUND(init.count/( SELECT COUNT(*) FROM `{project_id}.{dataset_id}.{table_id}`)*100, 2) AS STRING), '%')
  ELSE
    '0'
  END
    AS percentage
FROM (
  SELECT
    concept_classes.concept_id AS concept_id,
    concept_classes.drug_class_name AS drug_class,
    concept_classes.concept_name AS concept_name,
    COUNT(drug_exposure.drug_exposure_id) AS count
  FROM
    `{project_id}.{dataset_id}.{table_id}` AS drug_exposure
  JOIN
    `{project_id}.{dataset_id}.concept_ancestor` AS ancestor
  ON
    ancestor.descendant_concept_id = drug_exposure.drug_concept_id
  RIGHT JOIN
    `{project_id}.{dataset_id}.drug_class` AS concept_classes
  ON
    concept_classes.concept_id = ancestor.ancestor_concept_id
    AND ancestor.min_levels_of_separation != 0
  GROUP BY
    concept_classes.concept_id,
    concept_classes.concept_name,
    concept_classes.drug_class_name) AS init
ORDER BY
  count DESC,
  concept_id
    '''

# Used in _create_drug_class_table()
DRUG_CLASS_QUERY = '''
    SELECT
        concept_id,
        concept_name,
        CASE
            WHEN c.concept_id = 21602796 THEN 'Antibiotics'
            WHEN c.concept_id = 21601745 THEN 'CCB'
            WHEN c.concept_id = 21601462 THEN 'Diuretics'
            WHEN c.concept_id = 21604254 THEN 'Opioids'
            WHEN c.concept_id = 21601855 THEN 'Statins'
            WHEN c.concept_id = 21603933 THEN 'MSK NSAIDS'
            WHEN c.concept_id = 21600744 THEN 'Oral Hypoglycemics'
            WHEN c.concept_id = 21604303 THEN 'Pain NSAIDS'
            WHEN c.concept_id = 21601278 THEN 'Vaccines'
            WHEN c.concept_id = 21601783 THEN 'ACE Inhibitor'
            ELSE '0'
        END AS drug_class_name
    FROM
        `{dataset_id}.concept` c
    WHERE
        c.concept_id IN (21602796,
            21601745,
            21601462,
            21604254,
            21601855,
            21603933,
            21600744,
            21604303,
            21601278,
            21601783)
    '''

DUPLICATE_IDS_WRAPPER = '''
    SELECT
        table_name,
        count
    FROM
        ({union_of_subqueries})
    WHERE count IS NOT NULL
    '''

DUPLICATE_IDS_SUBQUERY = '''
    SELECT
        '{table_name}' AS table_name,
        SUM(Individual_Duplicate_ID_Count-1) as count
    FROM
    (SELECT
        COUNT({primary_key}) AS Individual_Duplicate_ID_Count
    FROM
        `{project_id}.{dataset_id}.{table_id}`
    GROUP BY
        {primary_key}
    HAVING
        COUNT({primary_key}) > 1)
    '''

EHR_NO_PII = 'EHR person record exists but no PII Name record'
EHR_NO_RDR = 'EHR person record exists but data/consent records not found as of {date}. Please contact ' \
             'EHR Ops for details.'
PII_NO_EHR = 'PII record exists but no EHR person record'
EHR_NO_PARTICIPANT_MATCH = 'EHR record exists but no participant match record'

MISSING_PII_QUERY = '''
WITH ehr_persons AS
(SELECT person_id
FROM `{project_id}.{dataset_id}.{person_table_id}`),
pii_names AS
(SELECT person_id
FROM `{project_id}.{dataset_id}.{pii_name_table_id}`),
all_pii AS
(SELECT DISTINCT person_id
FROM `{project_id}.{dataset_id}.{pii_wildcard}`),
participant_records AS
(SELECT person_id
FROM `{project_id}.{dataset_id}.{participant_match_table_id}`)
(SELECT DISTINCT '{ehr_no_pii}' AS missingness_type,
    COUNT(person_id) AS count
FROM (SELECT person_id FROM ehr_persons
    EXCEPT DISTINCT
    SELECT person_id FROM pii_names))
UNION ALL
(SELECT DISTINCT '{pii_no_ehr}' AS missingness_type,
    COUNT(person_id) AS count
FROM (SELECT person_id FROM all_pii
    EXCEPT DISTINCT
    SELECT person_id FROM ehr_persons))
UNION ALL
(SELECT DISTINCT '{ehr_no_participant_match}' AS missingness_type,
    COUNT(person_id) AS count
FROM (SELECT person_id FROM ehr_persons
    EXCEPT DISTINCT
    SELECT person_id FROM participant_records))
ORDER BY count DESC
'''

PREFIX = '/data_steward/v1/'

# Cron URLs
PARTICIPANT_VALIDATION = 'ParticipantValidation/'

# Return value for participant validation cron
VALIDATION_SUCCESS = 'participant-validation-done'

# Return value for ps_api cron
PS_API_SUCCESS = 'ps-api-done'

CONTENT_TYPE = 'content-type'
APPLICATION_JSON = 'application/json'
ERROR = 'error'
ERRORS = 'errors'
REASON = 'reason'

FOLDER_NAME_REGEX = r'\d{4}-\d{2}-\d{2}-v\d+'
FOLDER_NAMING_CONVENTION = 'YYYY-MM-DD-vN/'

VALIDATE = 'validate'
FETCH_PS_DATA = 'FetchPSData'
