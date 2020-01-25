import pandas as pd
from notebooks import bq, render

identify_labs_query = """
WITH get_excluded_ancestor_ids AS
(
  -- 36206173 and 36208978 are the root concepts `Laboratory` and `Clinical Categories`,
  -- we exclude all the direct children of `Laboratory` and `Clinical Categories` in the ancestor lookup later
  SELECT 
    ca.concept_id_1 AS excluded_ancestor_concept_id
  FROM `prod_drc_dataset.concept_relationship`  AS ca
  WHERE ca.concept_id_2 IN (36206173, 36208978) AND ca.relationship_id = 'Is a'
  
  UNION DISTINCT
  
  SELECT 
    ca.descendant_concept_id AS excluded_ancestor_concept_id
  FROM `prod_drc_dataset.concept_ancestor` AS ca
  WHERE ca.ancestor_concept_id IN (36206173, 36208978) AND ca.min_levels_of_separation = 1
  
  UNION DISTINCT
  -- Exclude the list of general concept is below
  -- 36208978 Clinical Categories
  -- 36206173 Laboratory Categories
  -- 36208195 Lab terms not yet categorized
  -- 36207527 Clinical terms not yet categorized
  -- 36210656 Survey terms not yet categorized
  
  -- Exclude the list of the "coarse" generalized concept ids 
  -- 40772590: Cholesterol
  -- 40782521: Leukocytes
  -- 40779250: Protein in the grandparent lookup
  SELECT 
    excluded_ancestor_concept_id 
  FROM UNNEST([36208978, 36206173, 36208195, 36207527, 36210656, 40782521, 40779250, 40772590]) AS excluded_ancestor_concept_id
),

get_direct_parents_loinc_group AS
(
  # We use left joins because there are concepts that don't have a LONIC_Hierarchy type ancestor in concept_ancestor
  SELECT DISTINCT
    m.Panel_OMOP_ID,
    m.Panel_Name,
    c1.concept_id AS measurement_concept_id,
    c1.concept_name AS measurement_concept_name,
    IF(ex.excluded_ancestor_concept_id IS NULL, c2.concept_id, NULL) AS parent_concept_id,
    IF(ex.excluded_ancestor_concept_id IS NULL, c2.concept_name, NULL) AS parent_concept_name,
    IF(ex.excluded_ancestor_concept_id IS NULL, c2.concept_class_id, NULL) AS parent_concept_class_id,
    IF(ex.excluded_ancestor_concept_id IS NULL, ca.min_levels_of_separation, -1) AS distance
  FROM
    `prod_drc_dataset.measurement_concept_sets` AS m
  JOIN 
    `prod_drc_dataset.concept` AS c1
  ON 
    m.Measurement_OMOP_ID = c1.concept_id
  LEFT JOIN
    `prod_drc_dataset.concept_ancestor` AS ca
  ON
    m.Measurement_OMOP_ID = ca.descendant_concept_id 
    AND ca.min_levels_of_separation = 1
  LEFT JOIN 
    get_excluded_ancestor_ids AS ex
  ON 
    ca.ancestor_concept_id = ex.excluded_ancestor_concept_id
  LEFT JOIN
    `prod_drc_dataset.concept` AS c2
  ON 
    ca.ancestor_concept_id = c2.concept_id
  WHERE c2.concept_class_id IS NULL OR c2.concept_class_id = 'LOINC Group'
),

get_ancestors_loinc_hierarchy AS
(
  # We use left joins because there are concepts that don't have a LONIC_Hierarchy type ancestor in concept_ancestor
  SELECT DISTINCT
    m.Panel_OMOP_ID,
    m.Panel_Name,
    c1.concept_id AS measurement_concept_id,
    c1.concept_name AS measurement_concept_name,
    IF(ex.excluded_ancestor_concept_id IS NULL, c2.concept_id, NULL) AS ancestor_concept_id,
    IF(ex.excluded_ancestor_concept_id IS NULL, c2.concept_name, NULL) AS ancestor_concept_name,
    IF(ex.excluded_ancestor_concept_id IS NULL, c2.concept_class_id, NULL) AS ancestor_concept_class_id,
    IF(ex.excluded_ancestor_concept_id IS NULL, COALESCE(ca.min_levels_of_separation, -1), -1) AS distance
  FROM
    `prod_drc_dataset.measurement_concept_sets` AS m
  JOIN 
    `prod_drc_dataset.concept` AS c1
  ON 
    m.Measurement_OMOP_ID = c1.concept_id
  LEFT JOIN
    `prod_drc_dataset.concept_ancestor` AS ca
  ON
    m.Measurement_OMOP_ID = ca.descendant_concept_id 
      AND ca.min_levels_of_separation IN (1, 2)
  LEFT JOIN 
    get_excluded_ancestor_ids AS ex
  ON 
    ca.ancestor_concept_id = ex.excluded_ancestor_concept_id
  LEFT JOIN
    `prod_drc_dataset.concept` AS c2
  ON 
    ca.ancestor_concept_id = c2.concept_id  
  WHERE
    -- if there is not ancestors for the measurement_concept_id
    (ca.descendant_concept_id IS NULL)
    OR
    -- if the level of seperation is 1, we keep them
    (c2.concept_class_id = 'LOINC Hierarchy' AND ca.min_levels_of_separation = 1)
    OR
    -- if the level of seperation is 2, we keep them only when the concept_name subsumes the grandparent concept_name
    (c2.concept_class_id = 'LOINC Hierarchy' AND ca.min_levels_of_separation = 2 AND c1.concept_name LIKE CONCAT('%', c2.concept_name , '%'))
    OR
    -- if the level of seperation is 2, the 6 concept names (such as MCH [Entitic mass], MCV [Entitic volume]) do not follow the previous rule, 
    -- because the acronyms are used in the concept_name and full names are used in the grandparent concept_name
    (c2.concept_class_id = 'LOINC Hierarchy' AND ca.min_levels_of_separation = 2 AND c1.concept_id IN (3035941, 3024731, 3003338, 3012030, 3009744, 3023599))
),

get_ancestors_loinc_hierarchy_distinct AS 
(
  -- For some concepts in LONIC Hierarchy, we include both parent and grandparent concept_ids, 
  -- We want to remove the parent concept_id if the grandparent concept_id is present. 
  SELECT DISTINCT
      Panel_OMOP_ID,
      Panel_Name,
      measurement_concept_id,
      measurement_concept_name,
      ancestor_concept_id,
      ancestor_concept_name,
      ancestor_concept_class_id,
      distance
  FROM
  (
    SELECT DISTINCT 
      *,
      dense_rank() over(PARTITION BY measurement_concept_id ORDER BY distance DESC) AS rank_order
    FROM get_ancestors_loinc_hierarchy
  ) l
  WHERE rank_order = 1
),

get_loinc_group_descendant_concept_ids AS
(
  SELECT 
    lg.Panel_OMOP_ID,
    lg.Panel_Name,
    lg.measurement_concept_id,
    lg.measurement_concept_name,
    lg.parent_concept_id,
    lg.parent_concept_name,
    COALESCE(lg.parent_concept_class_id, 'LOINC Group') AS parent_concept_class_id,
    COALESCE(ca1.descendant_concept_id, lg.parent_concept_id, lg.measurement_concept_id) AS loinc_groupy_descendant_concept_id,
    COALESCE(c1.concept_name, lg.parent_concept_name, lg.measurement_concept_name) AS loinc_groupy_descendant_concept_name,
    COALESCE(c1.concept_class_id, lg.parent_concept_class_id) AS loinc_groupy_descendant_concept_class_id,
    COALESCE(ca1.min_levels_of_separation, -1) AS distance
  FROM get_direct_parents_loinc_group AS lg
  LEFT JOIN 
    prod_drc_dataset.concept_ancestor AS ca1
  ON
    lg.parent_concept_id = ca1.ancestor_concept_id 
      AND ca1.min_levels_of_separation <> 0
  LEFT JOIN prod_drc_dataset.concept AS c1
    ON ca1.descendant_concept_id = c1.concept_id 
),

get_loinc_hierarchy_descendant_concept_ids AS
(
  SELECT
    lh.Panel_OMOP_ID,
    lh.Panel_Name,
    lh.measurement_concept_id,
    lh.measurement_concept_name,
    lh.ancestor_concept_id,
    lh.ancestor_concept_name,
    COALESCE(lh.ancestor_concept_class_id, 'LOINC Hierarchy') AS ancestor_concept_class_id,
    COALESCE(ca1.descendant_concept_id, lh.ancestor_concept_id, lh.measurement_concept_id) AS loinc_hierarchy_descendant_concept_id,
    COALESCE(c1.concept_name, lh.ancestor_concept_name, lh.measurement_concept_name) AS loinc_hierarchy_descendant_concept_name,
    COALESCE(c1.concept_class_id, lh.ancestor_concept_class_id) AS loinc_hierarchy_descendant_concept_class_id,
    COALESCE(ca1.min_levels_of_separation, -1) AS distance
  FROM get_ancestors_loinc_hierarchy_distinct AS lh
  LEFT JOIN 
    prod_drc_dataset.concept_ancestor AS ca1
  ON
    lh.ancestor_concept_id = ca1.ancestor_concept_id
      AND ca1.min_levels_of_separation <> 0
  LEFT JOIN prod_drc_dataset.concept AS c1
    ON ca1.descendant_concept_id = c1.concept_id  
)

SELECT DISTINCT
  COALESCE(lh.Panel_OMOP_ID, lg.Panel_OMOP_ID) AS panel_omop_id,
  COALESCE(lh.Panel_Name, lg.Panel_Name) AS panel_name,
  COALESCE(lh.measurement_concept_id, lg.measurement_concept_id) AS measurement_concept_id,
  COALESCE(lh.measurement_concept_name, lg.measurement_concept_name) AS measurement_concept_name,
  COALESCE(lh.ancestor_concept_id, lg.parent_concept_id, lh.measurement_concept_id, lg.measurement_concept_id) AS ancestor_concept_id,
  COALESCE(lh.ancestor_concept_name, lg.parent_concept_name, lh.measurement_concept_name, lg.measurement_concept_name) AS ancestor_concept_name,
  CASE
    WHEN lh.loinc_hierarchy_descendant_concept_id IS NOT NULL AND lg.loinc_groupy_descendant_concept_id IS NOT NULL THEN CONCAT(lh.ancestor_concept_class_id, ' / ', lg.parent_concept_class_id)
    WHEN lh.loinc_hierarchy_descendant_concept_id IS NOT NULL AND lg.loinc_groupy_descendant_concept_id IS NULL THEN lh.ancestor_concept_class_id
    WHEN lh.loinc_hierarchy_descendant_concept_id IS NULL AND lg.loinc_groupy_descendant_concept_id IS NOT NULL THEN lg.parent_concept_class_id
    ELSE 'N/A'
  END AS classification,
  COALESCE(lh.loinc_hierarchy_descendant_concept_id, lg.loinc_groupy_descendant_concept_id) AS descendant_concept_id,
  COALESCE(lh.loinc_hierarchy_descendant_concept_name, lg.loinc_groupy_descendant_concept_name) AS descendant_concept_name,
  COALESCE(lh.loinc_hierarchy_descendant_concept_class_id, lg.loinc_groupy_descendant_concept_class_id) AS descendant_concept_class_id
FROM get_loinc_hierarchy_descendant_concept_ids AS lh
FULL OUTER JOIN 
  get_loinc_group_descendant_concept_ids AS lg
ON
  lh.loinc_hierarchy_descendant_concept_id = lg.loinc_groupy_descendant_concept_id
"""

lab_count_per_site_query = """
SELECT
  panel_name,
  valid_lab.ancestor_concept_name,
  valid_lab.ancestor_concept_id,
  src_dataset_id,
  src_hpo_id,
  COUNT(DISTINCT person_id) AS n_person,
  COUNT(DISTINCT measurement_id) AS n_meas,
  COUNT(DISTINCT meas.measurement_concept_id) AS n_measurement_concept_ids
FROM 
(
  SELECT
    measurement_id,
    person_id,
    IF(measurement_concept_id IS NULL OR measurement_concept_id=0, measurement_source_concept_id, measurement_concept_id) AS measurement_concept_id
  FROM
    `combined20190802_base.measurement` 
) meas
JOIN
  `combined20190802_base._mapping_measurement`
USING
  (measurement_id)
JOIN 
  `aou-res-curation-prod.prod_drc_dataset.measurement_concept_sets_descendants_new` AS valid_lab
ON
  meas.measurement_concept_id = valid_lab.descendant_concept_id
GROUP BY
  1,
  2,
  3,
  4,
  5
ORDER BY 
  1,2
"""

identify_labs_query_results = bq.query(identify_labs_query)
render.dataframe(identify_labs_query_results)

lab_count_per_site_query_results = bq.query(lab_count_per_site_query)
render.dataframe(lab_count_per_site_query_results)


