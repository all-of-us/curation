import app_identity
import bq_utils

MEASUREMENT = 'measurement'
MEASUREMENT_CONCEPT_SETS_DESCENDANTS = 'measurement_concept_sets_descendants'

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
        {project_id}.{ehr_ops_dataset_id}.{measurement_concept_sets_descendants} AS csd
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


def get_lab_concept_summary_query(hpo_id):
    project_id = app_identity.get_application_id()
    dataset_id = bq_utils.get_dataset_id()
    hpo_measurement_table = bq_utils.get_table_id(hpo_id, MEASUREMENT)

    return CHECK_REQUIRED_LAB_QUERY.format(
        project_id=project_id,
        ehr_ops_dataset_id=dataset_id,
        hpo_measurement_table=hpo_measurement_table,
        measurement_concept_sets_descendants=MEASUREMENT_CONCEPT_SETS_DESCENDANTS
    )
