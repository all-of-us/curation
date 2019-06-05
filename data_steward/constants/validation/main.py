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
        analysis_id AS Analysis_ID,
        achilles_heel_warning AS Heel_Error,
        rule_id AS Rule_ID,
        record_count AS Record_Count
    FROM `{application}.{dataset}.{table_id}`
    WHERE achilles_heel_warning LIKE 'ERROR:%'
    ORDER BY 
        record_count DESC, 
        analysis_id
    '''

# Used in get_drug_checks_in_results_html()
DRUG_CHECKS_QUERY_VALIDATION = '''
    SELECT
        init.*,
        CONCAT(CAST(ROUND(init.Counts_by_Drug_class/(
            SELECT
                COUNT(*)
            FROM
                `{application}.{dataset}.{table_id}`)*100, 2) AS STRING), '%') AS Percentage
    FROM (
        SELECT
            concept_classes.concept_id AS Drug_Class_Concept_ID,
            concept_classes.drug_class_name AS Drug_Class,
            concept_classes.concept_name AS Drug_Class_Concept_Name,
            COUNT(drug_exposure.drug_exposure_id) AS Counts_by_Drug_class
        FROM
            `{application}.{dataset}.{table_id}` AS drug_exposure
        JOIN
            `{application}.{dataset}.concept_ancestor` AS ancestor
        ON
            ancestor.descendant_concept_id = drug_exposure.drug_concept_id
        JOIN 
            `{application}.{dataset}.drug_class` AS concept_classes
        ON
            concept_classes.concept_id = ancestor.ancestor_concept_id
        AND ancestor.min_levels_of_separation != 0
        GROUP BY
            concept_classes.concept_id,
            concept_classes.concept_name,
            concept_classes.drug_class_name) AS init
    ORDER BY
        Counts_by_Drug_class DESC,
        Drug_Class_Concept_ID
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
        Table_Name,
        Duplicate_ID_Count
    FROM
        ({union_of_subqueries})
    WHERE Duplicate_ID_Count IS NOT NULL
    '''

DUPLICATE_IDS_SUBQUERY = '''
    SELECT 
        '{domain_table}' AS Table_Name,
        SUM(Individual_Duplicate_ID_Count-1) as Duplicate_ID_Count
    FROM
    (SELECT
        COUNT({domain_table}_id) AS Individual_Duplicate_ID_Count
    FROM
        `{app_id}.{dataset_id}.{hpo_id}_{domain_table}`
    GROUP BY
        {domain_table}_id
    HAVING
        COUNT({domain_table}_id) > 1)
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
