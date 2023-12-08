# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.7.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# + tags=["parameters"]
src_project_id = ""  # project_id of the project the copy initiated from
dest_project_id = ""  # project_id of the project the copy is going to
src_dataset_id = ""  # dataset_id of the dataset whose contents will be copied
dest_dataset_id = ""  # dataset_id of the destination dataset
prev_dest_dataset_id = ""  # dataset_id of the previous release of this dataset to downstream teams in their environment
run_as = ""  # using impersonation, run all these queries as this service account
# -

# # QC for Publishing Datasets to output-prod Environment
#
# Quality checks performed on a newly published dataset.

from common import JINJA_ENV
from utils import auth
from utils.bq import get_client
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES, render_message

impersonation_creds = auth.get_impersonation_credentials(
    run_as, target_scopes=IMPERSONATION_SCOPES)

client = get_client(src_project_id, credentials=impersonation_creds)

# ## Table Comparison
#
# The copied tables should be the same and have the same row counts across datasets.<br>
# If the status column indicates a problem, some portion of the copy failed.

# +
tpl = JINJA_ENV.from_string('''
WITH prod_versions AS(
    SELECT 
        table_id
        ,row_count
    FROM `{{src_project_id}}.{{src_dataset_id}}.__TABLES__`
),

output_prod_versions AS (
    SELECT 
        table_id
        ,row_count
    FROM `{{dest_project_id}}.{{dest_dataset_id}}.__TABLES__`
),

results AS (
    SELECT 
        table_id
        ,p.row_count
        ,o.row_count
        ,CASE WHEN p.row_count <> o.row_count THEN 'PROBLEM'
            WHEN p.row_count is null THEN 'PROBLEM'
            WHEN o.row_count is null THEN 'PROBLEM'
            ELSE 'ok' END AS status
    FROM prod_versions p
    FULL OUTER JOIN output_prod_versions o
    USING (table_id)
)

SELECT 
    COUNTIF(status = 'ok') AS passing
    ,COUNTIF(status <> 'ok') AS failing
    ,COUNT(*) AS total_tables
FROM results;
''')

query = tpl.render(src_project_id=src_project_id,
                   dest_project_id=dest_project_id,
                   src_dataset_id=src_dataset_id,
                   dest_dataset_id=dest_dataset_id)
execute(client, query)
# -

# ## Person vs Person_Ext in Destination Dataset
# Make sure the destination person and the destination person_ext tables have harmonious data for the five appended columns to the person table.
# 1. sex_at_birth_concept_id
# 2. sex_at_birth_source_concept_id,
# 3. sex_at_birth_source_value
# 4. state_of_residence_concept_id
# 5. state_of_residence_source_value <br>
#
# Investigate any failed output.

tpl = JINJA_ENV.from_string('''
WITH calculation AS ( 
SELECT 
-- check state_of_residence columns --
  COUNTIF(p.state_of_residence_concept_id <> pe.state_of_residence_concept_id) AS ne_state_of_residence_concept_id
  ,COUNTIF((p.state_of_residence_concept_id IS NULL AND pe.state_of_residence_concept_id IS NOT NULL)
      OR (p.state_of_residence_concept_id IS NOT NULL AND pe.state_of_residence_concept_id IS NULL)) as ne_nulls_state_of_residence_concept_id
  ,COUNTIF(p.state_of_residence_source_value <> pe.state_of_residence_source_value) as ne_state_of_residence_source_value
  ,COUNTIF((p.state_of_residence_source_value IS NULL AND pe.state_of_residence_source_value IS NOT NULL)
      OR (p.state_of_residence_source_value IS NOT NULL AND pe.state_of_residence_source_value IS NULL)) as ne_nulls_state_of_residence_source_value
-- check sex_at_birth columns --
  ,COUNTIF(p.sex_at_birth_concept_id <> pe.sex_at_birth_concept_id) as ne_sex_at_birth_concept_id
  ,COUNTIF((p.sex_at_birth_concept_id IS NULL AND pe.sex_at_birth_concept_id IS NOT NULL)
      OR (p.sex_at_birth_concept_id IS NOT NULL AND pe.sex_at_birth_concept_id IS NULL)) as ne_nulls_sex_at_birth_concept_id
  ,COUNTIF(p.sex_at_birth_source_concept_id <> pe.sex_at_birth_source_concept_id) as ne_sex_at_birth_source_concept_id
  ,COUNTIF((p.sex_at_birth_source_concept_id IS NULL AND pe.sex_at_birth_source_concept_id IS NOT NULL)
      OR (p.sex_at_birth_source_concept_id IS NOT NULL AND pe.sex_at_birth_source_concept_id IS NULL)) as ne_nulls_sex_at_birth_source_concept_id
  ,COUNTIF(p.sex_at_birth_source_value <> pe.sex_at_birth_source_value) as ne_sex_at_birth_source_value
  ,COUNTIF((p.sex_at_birth_source_value IS NULL AND pe.sex_at_birth_source_value IS NOT NULL)
      OR (p.sex_at_birth_source_value IS NOT NULL AND pe.sex_at_birth_source_value IS NULL)) as ne_nulls_sex_at_birth_source_value
FROM `{{dest_project_id}}.{{dest_dataset_id}}.person` p
JOIN `{{dest_project_id}}.{{dest_dataset_id}}.person_ext` pe
USING (person_id)
)

SELECT
    'state_of_residence_concept_id_check' AS check
    ,CASE
        WHEN c.ne_state_of_residence_concept_id > 0
          THEN 'FAILED'
          ELSE 'passed'
          END AS result
FROM calculation AS c
      
UNION ALL 

SELECT
   'nulls_state_of_residence_concept_id' AS check
    ,CASE
        WHEN c.ne_nulls_state_of_residence_concept_id > 0
          THEN 'FAILED'
          ELSE 'passed'
          END AS result
FROM calculation AS c
            
UNION ALL 

SELECT
'state_of_residence_source_value_check' AS check
,CASE
    WHEN c.ne_state_of_residence_source_value > 0
      THEN 'FAILED'
      ELSE 'passed'
      END AS result
FROM calculation AS c
            
UNION ALL 

SELECT
'nulls_state_of_residence_source_value_check' AS check
,CASE
    WHEN c.ne_nulls_state_of_residence_source_value > 0
      THEN 'FAILED'
      ELSE 'passed'
      END AS result
FROM calculation AS c
            
UNION ALL 

SELECT
'sex_at_birth_concept_id_check' AS check
,CASE
    WHEN c.ne_sex_at_birth_concept_id > 0
      THEN 'FAILED'
      ELSE 'passed'
      END AS result
FROM calculation AS c
            
UNION ALL 

SELECT
'nulls_sex_at_birth_concept_id_check' AS check
,CASE
    WHEN c.ne_nulls_sex_at_birth_concept_id > 0
      THEN 'FAILED'
      ELSE 'passed'
      END AS result
FROM calculation AS c
            
UNION ALL 

SELECT
'sex_at_birth_source_concept_id_check' AS check
,CASE
    WHEN c.ne_sex_at_birth_source_concept_id > 0
      THEN 'FAILED'
      ELSE 'passed'
      END AS result
FROM calculation AS c
            
UNION ALL 

SELECT
'nulls_sex_at_birth_source_concept_id_check' AS check
,CASE
    WHEN c.ne_nulls_sex_at_birth_source_concept_id > 0
      THEN 'FAILED'
      ELSE 'passed'
      END AS result
FROM calculation AS c

UNION ALL

SELECT
'sex_at_birth_source_value_check' AS check
,CASE
    WHEN c.ne_sex_at_birth_source_value > 0
      THEN 'FAILED'
      ELSE 'passed'
      END AS result
FROM calculation AS c

UNION ALL 

SELECT
'null_sex_at_birth_source_value_check' AS check
,CASE
    WHEN c.ne_nulls_sex_at_birth_source_value > 0
      THEN 'FAILED'
      ELSE 'passed'
      END AS result
FROM calculation AS c


''')
query = tpl.render(dest_project_id=dest_project_id,
                   dest_dataset_id=dest_dataset_id)
execute(client, query)

# ## Person in destination Dataset vs Person_Ext in Source Dataset
# Make sure the destination person and source person_ext tables have harmonious data for the five appended columns to the person table.
#
# 1. sex_at_birth_concept_id
# 2. sex_at_birth_source_concept_id
# 3. sex_at_birth_source_value
# 4. state_of_residence_concept_id
# 5. state_of_residence_source_value.<br>
#
# Investigate any failed output.

tpl = JINJA_ENV.from_string('''
WITH calculation AS (
SELECT 
-- check state_of_residence columns --
    COUNTIF(p.state_of_residence_concept_id <> pe.state_of_residence_concept_id) AS ne_state_of_residence_concept_id
    ,COUNTIF((p.state_of_residence_concept_id IS NULL AND pe.state_of_residence_concept_id IS NOT NULL) 
        OR (p.state_of_residence_concept_id IS NOT NULL AND pe.state_of_residence_concept_id IS NULL)) AS ne_nulls_state_of_residence_concept_id
    ,COUNTIF(p.state_of_residence_source_value <> pe.state_of_residence_source_value) AS ne_state_of_residence_source_value
    ,COUNTIF((p.state_of_residence_source_value IS NULL AND pe.state_of_residence_source_value IS NOT NULL)
        OR (p.state_of_residence_source_value IS NOT NULL AND pe.state_of_residence_source_value IS NULL)) AS ne_nulls_state_of_residence_source_value
-- check sex_at_birth columns --
    ,COUNTIF(p.sex_at_birth_concept_id <> pe.sex_at_birth_concept_id) AS ne_sex_at_birth_concept_id
    ,COUNTIF((p.sex_at_birth_concept_id IS NULL AND pe.sex_at_birth_concept_id IS NOT NULL)
        OR (p.sex_at_birth_concept_id IS NOT NULL AND pe.sex_at_birth_concept_id IS NULL)) AS ne_nulls_sex_at_birth_concept_id
    ,COUNTIF(p.sex_at_birth_source_concept_id <> pe.sex_at_birth_source_concept_id) AS ne_sex_at_birth_source_concept_id
    ,COUNTIF((p.sex_at_birth_source_concept_id IS NULL AND pe.sex_at_birth_source_concept_id IS NOT NULL)
        OR (p.sex_at_birth_source_concept_id IS NOT NULL AND pe.sex_at_birth_source_concept_id IS NULL)) AS ne_nulls_sex_at_birth_source_concept_id
    ,COUNTIF(p.sex_at_birth_source_value <> pe.sex_at_birth_source_value) AS ne_sex_at_birth_source_value
    ,COUNTIF((p.sex_at_birth_source_value IS NULL AND pe.sex_at_birth_source_value IS NOT NULL)
        OR(p.sex_at_birth_source_value IS NOT NULL AND pe.sex_at_birth_source_value IS NULL)) AS ne_nulls_sex_at_birth_source_value
FROM `{{dest_project_id}}.{{dest_dataset_id}}.person` p
JOIN `{{src_project_id}}.{{src_dataset_id}}.person_ext` pe
USING(person_id))

SELECT
 'state_of_residence_concept_id_check' as check
 ,CASE
    WHEN c.ne_state_of_residence_concept_id > 0
      THEN 'FAILED'
      ELSE 'passed'
      END as result
FROM calculation as c

UNION ALL 

SELECT
 'nulls_state_of_residence_concept_id_check' as check
 ,CASE
    WHEN c.ne_nulls_state_of_residence_concept_id > 0
      THEN 'FAILED'
      ELSE 'passed'
      END as result
FROM calculation as c

UNION ALL 

SELECT
 'state_of_residence_source_value_check' as check
 ,CASE
    WHEN c.ne_state_of_residence_source_value > 0
      THEN 'FAILED'
      ELSE 'passed'
      END as result
FROM calculation as c

UNION ALL 

SELECT
 'nulls_state_of_residence_source_value_check' as check
 ,CASE
    WHEN c.ne_nulls_state_of_residence_source_value > 0
      THEN 'FAILED'
      ELSE 'passed'
      END as result
FROM calculation as c

UNION ALL 

SELECT
 'sex_at_birth_concept_id_check' as check
 ,
CASE
    WHEN c.ne_sex_at_birth_concept_id > 0
      THEN 'FAILED'
      ELSE 'passed'
      END as result
FROM calculation as c

UNION ALL 

SELECT
 'nulls_sex_at_birth_concept_id_check' as check
 ,CASE
    WHEN c.ne_nulls_sex_at_birth_concept_id > 0
      THEN 'FAILED'
      ELSE 'passed'
      END as result
FROM calculation as c

UNION ALL 

SELECT
 'sex_at_birth_source_concept_id_check' as check
 ,CASE
    WHEN c.ne_sex_at_birth_source_concept_id > 0
      THEN 'FAILED'
      ELSE 'passed'
      END as result
FROM calculation as c      


UNION ALL 

SELECT
 'nulls_sex_at_birth_source_concept_id_check' as check
 ,CASE
    WHEN c.ne_nulls_sex_at_birth_source_concept_id > 0
      THEN 'FAILED'
      ELSE 'passed'
      END as result
FROM calculation as c      


UNION ALL 

SELECT
 'sex_at_birth_source_value_check' as check
 ,CASE
    WHEN c.ne_sex_at_birth_source_value > 0
      THEN 'FAILED'
      ELSE 'passed'
      END as result
FROM calculation as c

UNION ALL 

SELECT
 'null_sex_at_birth_source_value_check' as check
 ,CASE
    WHEN c.ne_nulls_sex_at_birth_source_value > 0
      THEN 'FAILED'
      ELSE 'passed'
      END as result
FROM calculation as c
''')
query = tpl.render(src_project_id=src_project_id,
                   dest_project_id=dest_project_id,
                   src_dataset_id=src_dataset_id,
                   dest_dataset_id=dest_dataset_id)
execute(client, query)

# ## Copy status from prod to output prod
#

tpl = JINJA_ENV.from_string('''
WITH all_results AS(
SELECT 
  src.table_id AS curation_table_id
  ,src.row_count AS curation_row_count
  ,dest.table_id AS output_table_id
  ,dest.row_count AS output_row_count
  ,CASE WHEN src.row_count <> dest.row_count THEN "PROBLEM" ELSE "ok" END AS copy_status
FROM `{{src_project_id}}.{{src_dataset_id}}.__TABLES__` src
FULL OUTER JOIN `{{dest_project_id}}.{{dest_dataset_id}}.__TABLES__` dest
USING (table_id))


SELECT 
    COUNTIF(copy_status = 'ok') AS passing
    ,COUNTIF(copy_status <> 'ok') AS failing
    ,COUNT(*) AS total_tables
FROM all_results;

''')
query = tpl.render(src_project_id=src_project_id,
                   dest_project_id=dest_project_id,
                   src_dataset_id=src_dataset_id,
                   dest_dataset_id=dest_dataset_id)
execute(client, query)

# ## Compare to previous release in output prod
# This will identify new or missing tables. <br>
# **The check automatically passes if there are no results.** <br>
# Some table changes are expected and any query results should be reviewed as a sanity check.
#
# The prep_% , cb_% , and ds_% tables are added by downstream teams after the datasets are published. These will most likely be in the previous dataset but will not be in the new dataset at time of publication. These tables will be ignored by the query.

# +
tpl = JINJA_ENV.from_string('''
WITH all_results AS (
SELECT
  c.table_id AS current_table_id
  ,c.row_count AS current_row_count
  ,p.table_id AS previous_table_id
  ,p.row_count AS previous_row_count
  ,CASE WHEN p.row_count IS NULL AND c.row_count IS NOT NULL THEN "new table - review"
        WHEN p.row_count IS NOT NULL AND c.row_count IS NULL THEN "dropped table - review"
        ELSE "ok" END AS table_change_status
FROM `{{dest_project_id}}.{{dest_dataset_id}}.__TABLES__` c
FULL OUTER JOIN `{{dest_project_id}}.{{prev_dest_dataset_id}}.__TABLES__` p
USING (table_id)
ORDER BY 5, 1, 3)

SELECT *
FROM all_results
WHERE table_change_status <> "ok"
AND (previous_table_id NOT LIKE 'prep_%'
AND previous_table_id NOT LIKE 'cb_%' 
AND previous_table_id NOT LIKE 'ds_%')
ORDER BY 5, 1, 3
;''')
query = tpl.render(dest_project_id=dest_project_id,
                   prev_dest_dataset_id=prev_dest_dataset_id,
                   dest_dataset_id=dest_dataset_id)
execute(client, query)

# # QC for AOU_DEATH table

# From CDR V8, Curation generates the AOU_DEATH table for output. AOU_DEATH allows more than one death record per participant.
# It has the column `primary_death_record` and it flags the primary records for each participant.
# The logic for deciding which is primary comes from the following business requirements:
# - If multiple death records exist from across sources, provide the first date EHR death record in the death table
# - If death_datetime is not available and multiple death records exist for the same death_date, provide the fullest record in the death table
# - Example: Order by HPO site name and insert the first into the death table
# - Death records from HealthPro can have NULL death_date. Such records must be always `primary_death_record=False`.
#
# This QC confirms that the logic for the primary records are applied as expected in the `AOU_DEATH` table.

# +
# query = JINJA_ENV.from_string("""
# WITH qc_aou_death AS (
#     SELECT
#         aou_death_id,
#         CASE WHEN aou_death_id IN (
#             SELECT aou_death_id FROM `{{project_id}}.{{dataset_id}}.aou_death`
#             WHERE death_date IS NOT NULL -- NULL death_date records must not become primary --
#             QUALIFY RANK() OVER (
#                 PARTITION BY person_id
#                 ORDER BY
#                     LOWER(src_id) NOT LIKE '%healthpro%' DESC, -- EHR records are chosen over HealthPro ones --
#                     death_date ASC, -- Earliest death_date records are chosen over later ones --
#                     death_datetime ASC NULLS LAST, -- Earliest non-NULL death_datetime records are chosen over later or NULL ones --
#                     src_id ASC -- EHR site that alphabetically comes first is chosen --
#             ) = 1
#         ) THEN TRUE ELSE FALSE END AS primary_death_record
#     FROM `{{project}}.{{dataset}}.aou_death`
# )
# SELECT ad.aou_death_id
# FROM `{{project_id}}.{{dataset}}.aou_death` ad
# LEFT JOIN qc_aou_death qad
# ON ad.aou_death_id = qad.aou_death_id
# WHERE ad.primary_death_record != qad.primary_death_record
# """).render(project_id=dest_project_id, dataset=dest_dataset_id)
# df = execute(client, query)

success_msg = 'All death records have the correct `primary_death_record` values.'
failure_msg = '''
    <b>{code_count}</b> records do not have the correct `primary_death_record` values. 
    Investigate and confirm if (a) any logic is incorrect, (b) the requirement has changed, or (c) something else.
'''
render_message(df,
               success_msg,
               failure_msg,
               failure_msg_args={'code_count': len(df)})
# -

# # QC for DEATH table

# From CDR V8, the DEATH table must exist and have the primary death records from AOU_DEATH in the output data stage.
# This QC confirms that the DEATH table is there and has correct data.

# +
# query = JINJA_ENV.from_string("""
# WITH primary_aou_death AS (
#     SELECT person_id, death_date, death_datetime, death_type_concept_id, cause_concept_id, cause_source_value, cause_source_concept_id
#     FROM `{{project_id}}.{{dataset}}.aou_death`
#     WHERE primary_death_record = TRUE
# ), primary_records_missing_from_death AS (
#     SELECT person_id, death_date, death_datetime, death_type_concept_id, cause_concept_id, cause_source_value, cause_source_concept_id
#     FROM primary_aou_death
#     EXCEPT DISTINCT
#     SELECT person_id, death_date, death_datetime, death_type_concept_id, cause_concept_id, cause_source_value, cause_source_concept_id
#     FROM `{{project_id}}.{{dataset}}.death`
# ), unexpected_records_in_death AS (
#     SELECT person_id, death_date, death_datetime, death_type_concept_id, cause_concept_id, cause_source_value, cause_source_concept_id
#     FROM `{{project_id}}.{{dataset}}.death`
#     EXCEPT DISTINCT
#     SELECT person_id, death_date, death_datetime, death_type_concept_id, cause_concept_id, cause_source_value, cause_source_concept_id
#     FROM primary_aou_death
# )
# SELECT "primary_records_missing_from_death" AS issue, * FROM primary_records_missing_from_death
# UNION ALL
# SELECT "unexpected_records_in_death" AS issue, * FROM unexpected_records_in_death
# """).render(project_id=dest_project_id, dataset=dest_dataset_id)
# df = execute(client, query)

success_msg = 'DEATH table has correct and complete data.'
failure_msg = '''
    There are some discrepancies between DEATH records and AOU_DEATH records with primary_death_record=TRUE.
    Investigation needed.
'''
render_message(df, success_msg, failure_msg)
# -