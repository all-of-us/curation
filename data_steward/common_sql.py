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

# Used in validation_test/_create_drug_class_table()
drug_class_query = '''
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
    21601783) '''