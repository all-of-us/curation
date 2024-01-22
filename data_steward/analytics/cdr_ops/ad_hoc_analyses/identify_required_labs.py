# -*- coding: utf-8 -*-
import utils.bq
from notebooks import render

# ## Strategy for getting all related lab concepts
#
# We are going to use a combination of LOINC Group and LOINC Hierarchy because both classification systems cover some concepts the other does not cover. In general, LOINC Hierarchy provides generalized concepts that cover more descendants except for **physical measurements**, in which case we want to use LOINC Group.
#
# <ul>
#     <li>LONIC Group: going up 1 level is sufficient for getting the generalized lab concept ids. </li>
#     <br>
#     <li>LOINC Hierarchy: going up by 2 levels improved the coverages greatly. However, we need to place the restriction (going up by 1 level only) on Leukocytes, Protein and Cholesterol because their granularities are too “coarse”. Leukocytes in the hierarchy subsume concepts such as Neutrophils, Basophils, which are separate labs on their own. The same issue with another lab Protein that subsumes Albumin, which is a separate lab on its own.</li>
#     <br>
#     <li>We are going to union the descendants of the generalized lab concepts using LOINC Group and LOINC Hierarchy in order to identify whether or not the HPO sites have submitted the required labs. If a site submits data on any one of the labs, we will flag it as yes otherwise flag it as no. </li>
# </ul>

DATASET_ID = ''  # the dataset_id for which we are checking the require labs
VOCAB_DATASET_ID = ''  # the latest vocabulary dataset_id

IDENTIFY_LABS_QUERY = """
WITH get_excluded_ancestor_ids AS
(
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
  # We use left joins because there are concepts that don't have a LONIC_Group type ancestor in concept_ancestor
  SELECT DISTINCT
    m.Panel_OMOP_ID,
    m.Panel_Name,
    c1.concept_id AS measurement_concept_id,
    c1.concept_name AS measurement_concept_name,
    IF(ex.excluded_ancestor_concept_id IS NULL, c2.concept_id, NULL) AS parent_concept_id,
    IF(ex.excluded_ancestor_concept_id IS NULL, c2.concept_name, NULL) AS parent_concept_name,
    IF(ex.excluded_ancestor_concept_id IS NULL, c2.concept_class_id, NULL) AS parent_concept_class_id,
    IF(ex.excluded_ancestor_concept_id IS NULL, COALESCE(ca.min_levels_of_separation, -1), -1) AS distance
  FROM
    `ehr_ops.measurement_concept_sets` AS m
  JOIN
    `{VOCAB_DATASET_ID}.concept` AS c1
  ON
    m.Measurement_OMOP_ID = c1.concept_id
  LEFT JOIN
    `{VOCAB_DATASET_ID}.concept_ancestor` AS ca
  ON
    m.Measurement_OMOP_ID = ca.descendant_concept_id
    AND ca.min_levels_of_separation = 1
  LEFT JOIN
    get_excluded_ancestor_ids AS ex
  ON
    ca.ancestor_concept_id = ex.excluded_ancestor_concept_id
  LEFT JOIN
    `{VOCAB_DATASET_ID}.concept` AS c2
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
    `ehr_ops.measurement_concept_sets` AS m
  JOIN
    `{VOCAB_DATASET_ID}.concept` AS c1
  ON
    m.Measurement_OMOP_ID = c1.concept_id
  LEFT JOIN
    `{VOCAB_DATASET_ID}.concept_ancestor` AS ca
  ON
    m.Measurement_OMOP_ID = ca.descendant_concept_id
      AND ca.min_levels_of_separation IN (1, 2)
  LEFT JOIN
    get_excluded_ancestor_ids AS ex
  ON
    ca.ancestor_concept_id = ex.excluded_ancestor_concept_id
  LEFT JOIN
    `{VOCAB_DATASET_ID}.concept` AS c2
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
  # For some concepts in LONIC Hierarchy, we include both parent and grandparent concept_ids,
  # We want to remove the parent concept_id if the grandparent concept_id is present.
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
  # We use left join to concept_ancestor because not all the concepts have an ancestor, in which case
  # we make the measurement_concept_id its own ancestor
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
    {VOCAB_DATASET_ID}.concept_ancestor AS ca1
  ON
    lg.parent_concept_id = ca1.ancestor_concept_id
      AND ca1.min_levels_of_separation <> 0
  LEFT JOIN {VOCAB_DATASET_ID}.concept AS c1
    ON ca1.descendant_concept_id = c1.concept_id
),

get_loinc_hierarchy_descendant_concept_ids AS
(
  # We use left join to concept_ancestor because not all the concepts have an ancestor, in which case
  # we make the measurement_concept_id its own ancestor
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
    {VOCAB_DATASET_ID}.concept_ancestor AS ca1
  ON
    lh.ancestor_concept_id = ca1.ancestor_concept_id
      AND ca1.min_levels_of_separation <> 0
  LEFT JOIN {VOCAB_DATASET_ID}.concept AS c1
    ON ca1.descendant_concept_id = c1.concept_id
),

get_measurement_concept_sets_descendants AS
(
  # We use a full outer join between the loinc_hierarchy descendants and loinc_group descendants
  # in order to maximize the number of descendants retrieved by both classficiation systems.
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
)

SELECT
  panel_name,
  valid_lab.ancestor_concept_name,
  valid_lab.ancestor_concept_id,
  src_dataset_id,
  src_hpo_id,
  COUNT(DISTINCT person_id) AS n_person,
  COUNT(DISTINCT measurement_id) AS n_meas,
  COUNT(DISTINCT descendant_concept_id) AS n_descendant
FROM
(
  SELECT
    measurement_id,
    person_id,
    IF(measurement_concept_id IS NULL OR measurement_concept_id=0, measurement_source_concept_id, measurement_concept_id) AS measurement_concept_id
  FROM
    `{DATASET_ID}.measurement`
) meas
JOIN
  `{DATASET_ID}._mapping_measurement`
USING
  (measurement_id)
JOIN
  get_measurement_concept_sets_descendants AS valid_lab
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

identify_labs_query_results = utils.bq.query(
    IDENTIFY_LABS_QUERY.format(VOCAB_DATASET_ID=VOCAB_DATASET_ID,
                               DATASET_ID=DATASET_ID))
render.dataframe(identify_labs_query_results)
