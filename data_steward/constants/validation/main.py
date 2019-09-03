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

# Table Headers
RESULT_FILE_HEADERS = ["File Name", "Found", "Parsed", "Loaded"]
ERROR_FILE_HEADERS = ["File Name", "Message"]
DRUG_CHECK_HEADERS = ['Counts by Drug class', 'Drug Class Concept Name',
                      'Drug Class', 'Percentage', 'Drug Class Concept ID']
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
HEEL_ERROR_FAIL_ROWS = [(NULL_MESSAGE, HEEL_ERROR_FAIL_MESSAGE, NULL_MESSAGE, NULL_MESSAGE)]

# Used in get_drug_checks_in_results_html()
DRUG_CHECKS_QUERY_VALIDATION = '''
    SELECT
        init.*,
        CONCAT(CAST(ROUND(init.count/(
            SELECT
                COUNT(*)
            FROM
                `{project_id}.{dataset_id}.{table_id}`)*100, 2) AS STRING), '%') AS percentage
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
        COUNT({table_name}_id) AS Individual_Duplicate_ID_Count
    FROM
        `{project_id}.{dataset_id}.{table_id}`
    GROUP BY
        {table_name}_id
    HAVING
        COUNT({table_name}_id) > 1)
    '''

PREFIX = '/data_steward/v1/'

# Cron URLs
PARTICIPANT_VALIDATION = 'ParticipantValidation/'
WRITE_DRC_VALIDATION_FILE = PARTICIPANT_VALIDATION + 'DRCFile'
WRITE_SITE_VALIDATION_FILES = PARTICIPANT_VALIDATION + 'SiteFiles'

# Return Values
VALIDATION_SUCCESS = 'participant-validation-done'
DRC_VALIDATION_REPORT_SUCCESS = 'drc-participant-validation-report-written'
SITES_VALIDATION_REPORT_SUCCESS = 'sites-participant-validation-reports-written'

CONTENT_TYPE = 'content-type'
APPLICATION_JSON = 'application/json'
ERROR = 'error'
ERRORS = 'errors'
REASON = 'reason'
