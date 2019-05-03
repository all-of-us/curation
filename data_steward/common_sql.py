# This File consists of all the SQL Queries across the modules

# Used in validation/main/get_heel_errors_in_results_html()
HEEL_ERROR_QUERY_VALIDATION = '''
     SELECT analysis_id AS Analysis_ID,
      achilles_heel_warning AS Heel_Error,
      rule_id AS Rule_ID,
      record_count AS Record_Count
     FROM `{application}.{dataset}.{table_id}`
     WHERE achilles_heel_warning LIKE 'ERROR:%'
     ORDER BY record_count DESC, analysis_id
    '''

# Used in Validation/main/get_drug_checks_in_results_html()
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
