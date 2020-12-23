IDENTIFY_LABS_QUERY = '''
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

concept_ancestor AS
(
SELECT
  *
FROM
  `{project_id}.{vocab_dataset_id}.concept_ancestor`
UNION ALL
SELECT
  *
FROM
  `{project_id}.{ehr_ops_dataset_id}.concept_ancestor_extension`
),

get_direct_parents_loinc_group AS
(
  -- We use left joins because there are concepts that don`t have a LONIC_Group type ancestor in concept_ancestor
  SELECT DISTINCT
    m.Panel_OMOP_ID,
    m.Panel_Name,
    COALESCE(c1.concept_id, m.measurement_omop_id) AS measurement_concept_id,
    COALESCE(c1.concept_name, m.measurement_name) AS measurement_concept_name,
    IF(ex.excluded_ancestor_concept_id IS NULL, c2.concept_id, NULL) AS parent_concept_id,
    IF(ex.excluded_ancestor_concept_id IS NULL, c2.concept_name, NULL) AS parent_concept_name,
    IF(ex.excluded_ancestor_concept_id IS NULL, c2.concept_class_id, NULL) AS parent_concept_class_id,
    IF(ex.excluded_ancestor_concept_id IS NULL, COALESCE(ca.min_levels_of_separation, -1), -1) AS distance
  FROM
    `{project_id}.{ehr_ops_dataset_id}.{measurement_concept_sets}` AS m
  LEFT JOIN 
    `{project_id}.{vocab_dataset_id}.concept` AS c1
  ON 
    m.Measurement_OMOP_ID = c1.concept_id
  LEFT JOIN
    concept_ancestor AS ca
  ON
    m.Measurement_OMOP_ID = ca.descendant_concept_id 
    AND ca.min_levels_of_separation = 1
  LEFT JOIN 
    get_excluded_ancestor_ids AS ex
  ON 
    ca.ancestor_concept_id = ex.excluded_ancestor_concept_id
  LEFT JOIN
    `{project_id}.{vocab_dataset_id}.concept` AS c2
  ON 
    ca.ancestor_concept_id = c2.concept_id
  WHERE c2.concept_class_id IS NULL OR c2.concept_class_id = 'LOINC Group'
),

get_ancestors_loinc_hierarchy AS
(
  -- We use left joins because there are concepts that don't have a LONIC_Hierarchy type ancestor in concept_ancestor
  SELECT DISTINCT
    m.Panel_OMOP_ID,
    m.Panel_Name,
    COALESCE(c1.concept_id, m.measurement_omop_id) AS measurement_concept_id,
    COALESCE(c1.concept_name, m.measurement_name) AS measurement_concept_name,
    IF(ex.excluded_ancestor_concept_id IS NULL, c2.concept_id, NULL) AS ancestor_concept_id,
    IF(ex.excluded_ancestor_concept_id IS NULL, c2.concept_name, NULL) AS ancestor_concept_name,
    IF(ex.excluded_ancestor_concept_id IS NULL, c2.concept_class_id, NULL) AS ancestor_concept_class_id,
    IF(ex.excluded_ancestor_concept_id IS NULL, COALESCE(ca.min_levels_of_separation, -1), -1) AS distance
  FROM
    `{project_id}.{ehr_ops_dataset_id}.{measurement_concept_sets}` AS m
  LEFT JOIN 
    `{project_id}.{vocab_dataset_id}.concept` AS c1
  ON 
    m.Measurement_OMOP_ID = c1.concept_id
  LEFT JOIN
    concept_ancestor AS ca
  ON
    m.Measurement_OMOP_ID = ca.descendant_concept_id 
      AND ca.min_levels_of_separation IN (1, 2)
  LEFT JOIN 
    get_excluded_ancestor_ids AS ex
  ON 
    ca.ancestor_concept_id = ex.excluded_ancestor_concept_id
  LEFT JOIN
    `{project_id}.{vocab_dataset_id}.concept` AS c2
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
  -- We use left join to concept_ancestor because not all the concepts have an ancestor, in which case 
  -- we make the measurement_concept_id its own ancestor
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
    concept_ancestor AS ca1
  ON
    lg.parent_concept_id = ca1.ancestor_concept_id 
      AND ca1.min_levels_of_separation <> 0
  LEFT JOIN `{project_id}.{vocab_dataset_id}.concept` AS c1
    ON ca1.descendant_concept_id = c1.concept_id 
),

get_loinc_hierarchy_descendant_concept_ids AS
(
  -- We use left join to concept_ancestor because not all the concepts have an ancestor, in which case 
  -- we make the measurement_concept_id its own ancestor
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
    concept_ancestor AS ca1
  ON
    lh.ancestor_concept_id = ca1.ancestor_concept_id
      AND ca1.min_levels_of_separation <> 0
  LEFT JOIN `{project_id}.{vocab_dataset_id}.concept` AS c1
    ON ca1.descendant_concept_id = c1.concept_id  
)

-- We use a full outer join between the loinc_hierarchy descendants and loinc_group descendants 
-- in order to maximize the number of descendants retrieved by both classficiation systems. 
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
'''

CHECK_REQUIRED_LAB_QUERY = '''
WITH get_panels_with_num_of_members AS
(
  -- Count the number of members each panel has
  SELECT DISTINCT
    panel_name,
    ancestor_concept_id,
    COUNT(*) OVER (PARTITION BY Panel_Name) AS panel_name_count
  FROM 
    `{project_id}.{ehr_ops_dataset_id}.{measurement_concept_sets_descendants}`
),

get_related_panels AS
(
  -- For those panels that overlap with each other such as BMP AND CMP, we want to put those labs together in the 
  -- result table. To do that, we want to choose one of the overlapping panels as the master and all the other panel 
  -- names will be replaced by the master so that we can group all related labs together in the result table.
  -- The panel that contains more members is considered as the master panel, all the other panels overlapping with 
  -- the master panel, their names will be replaced by the master_panel_name  
  SELECT DISTINCT
    cs1.panel_name AS master_panel_name,
    cs2.panel_name AS panel_name
  FROM 
    get_panels_with_num_of_members AS cs1
  JOIN 
    get_panels_with_num_of_members AS cs2
  ON 
    cs1.ancestor_concept_id = cs2.ancestor_concept_id 
      AND cs1.panel_name <> cs2.panel_name
      AND cs1.panel_name_count >= cs2.panel_name_count
),

get_measurement_concept_sets_descendants AS 
(
  -- Replace panel names with the standard panel name
  SELECT DISTINCT
    COALESCE(p.master_panel_name, csd.panel_name) AS panel_name,
    csd.ancestor_concept_id,
    csd.ancestor_concept_name,
    csd.descendant_concept_id
  FROM 
    `{project_id}.{ehr_ops_dataset_id}.{measurement_concept_sets_descendants}` AS csd
  LEFT JOIN
    get_related_panels AS p
  ON 
    csd.panel_name = p.panel_name
),

get_measurements_from_hpo_site AS
(
  SELECT
    meas.measurement_id,
    meas.person_id,
    IF(measurement_concept_id IS NULL OR measurement_concept_id=0, measurement_source_concept_id, measurement_concept_id) AS measurement_concept_id
  FROM
    `{project_id}.{ehr_ops_dataset_id}.{hpo_measurement_table}` AS meas
)

SELECT DISTINCT
  valid_lab.panel_name,
  valid_lab.ancestor_concept_id,
  valid_lab.ancestor_concept_name,
  CAST(COUNT(DISTINCT meas.measurement_id) > 0 AS INT64) AS measurement_concept_id_exists
FROM 
  get_measurement_concept_sets_descendants AS valid_lab
LEFT JOIN
  get_measurements_from_hpo_site AS meas
ON
  meas.measurement_concept_id = valid_lab.descendant_concept_id
GROUP BY
  1,2,3
ORDER BY
  1,2
'''
