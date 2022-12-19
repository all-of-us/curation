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
src_dataset_id = ""  # datset_id of the dataset whose contents will be copied
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
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES

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
