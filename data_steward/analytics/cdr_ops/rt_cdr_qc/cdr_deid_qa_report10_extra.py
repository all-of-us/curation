# -*- coding: utf-8 -*-
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

# + [markdown] papermill={"duration": 0.024011, "end_time": "2021-02-02T22:30:31.951734", "exception": false, "start_time": "2021-02-02T22:30:31.927723", "status": "completed"} tags=[]
# #  Below are the queries we ran for extra validation that can be added to the RT _deid validation notebook:
#
# https://precisionmedicineinitiative.atlassian.net/browse/DC-1404
#
#
# -

import urllib
import pandas as pd
from common import JINJA_ENV
from utils import auth
from gcloud.bq import BigQueryClient
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES
pd.options.display.max_rows = 120

# + papermill={"duration": 0.023643, "end_time": "2021-02-02T22:30:31.880820", "exception": false, "start_time": "2021-02-02T22:30:31.857177", "status": "completed"} tags=["parameters"]
project_id = ""
rt_cdr_deid = ""
ct_cdr_deid = ""
deid_sand = ""
pipeline = ""
rt_cdr_deid_clean = ''
reg_combine = ''
combine = ''
run_as = ""
cdr_cutoff_date = ""

# +
impersonation_creds = auth.get_impersonation_credentials(
    run_as, target_scopes=IMPERSONATION_SCOPES)

client = BigQueryClient(project_id, credentials=impersonation_creds)
# -

# df will have a summary in the end
df = pd.DataFrame(columns=['query', 'result'])

# + [markdown] papermill={"duration": 0.02327, "end_time": "2021-02-02T22:30:32.708257", "exception": false, "start_time": "2021-02-02T22:30:32.684987", "status": "completed"} tags=[]
# # Q1 No person exists over 89 in the dataset:
# -

cdr_cutoff_date

# + papermill={"duration": 4.105203, "end_time": "2021-02-02T22:30:36.813460", "exception": false, "start_time": "2021-02-02T22:30:32.708257", "status": "completed"} tags=[]
query = JINJA_ENV.from_string("""

SELECT COUNT(*) as n_participants_over_89 FROM `{{project_id}}.{{rt_cdr_deid}}.person`
WHERE FLOOR(DATE_DIFF(DATE('{{cdr_cutoff_date}}'),DATE(birth_datetime), DAY)/365.25) > 89
""")
q = query.render(project_id=project_id,
                 rt_cdr_deid=rt_cdr_deid,
                 cdr_cutoff_date=cdr_cutoff_date)

df1 = execute(client, q)
if df1.loc[0].sum() == 0:
    df = df.append(
        {
            'query': 'Query1 No person exists over 89 in the dataset',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query1 No person exists over 89 in the dataset',
            'result': 'Failure'
        },
        ignore_index=True)
df1

# + [markdown] papermill={"duration": 0.023633, "end_time": "2021-02-02T22:30:36.860798", "exception": false, "start_time": "2021-02-02T22:30:36.837165", "status": "completed"} tags=[]
# # Q2   No original person_id exists in the de-identified dataset:

# +
query = JINJA_ENV.from_string("""
SELECT COUNT(*) as n_original_person_ids FROM `{{project_id}}.{{rt_cdr_deid}}.person`
WHERE person_id IN (
SELECT person_id FROM `{{project_id}}.{{combine}}.person`)
""")
q = query.render(project_id=project_id,
                 rt_cdr_deid=rt_cdr_deid,
                 combine=combine)
df1 = execute(client, q)

if df1.loc[0].sum() == 0:
    df = df.append(
        {
            'query': 'Query2 No original person_id exists',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query2 No original person_id exists',
            'result': 'Failure'
        },
        ignore_index=True)
df1
# -

# # Q3  These columns should be null, zero, or blank, queries should return 0 results:
#
# a. non null provider_id in condition_occurrence table:

query = JINJA_ENV.from_string("""
SELECT COUNT(*) AS non_null_provider_ids FROM `{{project_id}}.{{rt_cdr_deid}}.condition_occurrence`
WHERE provider_id IS NOT NULL
""")
q = query.render(project_id=project_id, rt_cdr_deid=rt_cdr_deid)
df1 = execute(client, q)
if df1.loc[0].sum() == 0:
    df = df.append(
        {
            'query':
                'Query3a non null provider_id in condition_occurrence table',
            'result':
                'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query':
                'Query3a non null provider_id in condition_occurrence table',
            'result':
                'Failure'
        },
        ignore_index=True)
df1

# b. non null cause_concept_id, cause_source_value, cause_source_concept_id in death table:

query = JINJA_ENV.from_string("""
SELECT COUNT(*) AS non_null_values FROM `{{project_id}}.{{rt_cdr_deid}}.death`
WHERE cause_concept_id IS NOT NULL OR cause_source_value IS NOT NULL OR cause_source_concept_id IS NOT NULL
""")
q = query.render(project_id=project_id, rt_cdr_deid=rt_cdr_deid)
df1 = execute(client, q)
if df1.loc[0].sum() == 0:
    df = df.append(
        {
            'query':
                'Query3b non null cause_concept_id, cause_source_value, cause_source_concept_id in death table',
            'result':
                'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query':
                'Query3b non null cause_concept_id, cause_source_value, cause_source_concept_id in death table',
            'result':
                ''
        },
        ignore_index=True)
df1

# c. non null provider_id in device_exposure table:

query = JINJA_ENV.from_string("""
SELECT COUNT(*) AS non_null_provider_ids FROM `{{project_id}}.{{rt_cdr_deid}}.device_exposure`
WHERE provider_id IS NOT NULL
""")
q = query.render(project_id=project_id, rt_cdr_deid=rt_cdr_deid)
df1 = execute(client, q)
if df1.loc[0].sum() == 0:
    df = df.append(
        {
            'query': 'Query3c non null provider_id in device_exposure table',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query3c non null provider_id in device_exposure table',
            'result': ''
        },
        ignore_index=True)
df1

# d. non null value_source_value in measurement table:

query = JINJA_ENV.from_string("""
SELECT COUNT(*) AS non_null_value_source_value FROM `{{project_id}}.{{rt_cdr_deid}}.measurement`
WHERE value_source_value IS NOT NULL
""")
q = query.render(project_id=project_id, rt_cdr_deid=rt_cdr_deid)
df1 = execute(client, q)
if df1.loc[0].sum() == 0:
    df = df.append(
        {
            'query': 'Query3d non null value_source_value in measurement table',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query3d non null value_source_value in measurement table',
            'result': ''
        },
        ignore_index=True)
df1

#   e. non null value_source_value, value_as_string, and provider_id in observation table:

query = JINJA_ENV.from_string("""
SELECT COUNT(*) AS non_null_values FROM `{{project_id}}.{{rt_cdr_deid}}.observation`
WHERE value_source_value IS NOT NULL OR value_as_string IS NOT NULL OR provider_id IS NOT NULL
""")
q = query.render(project_id=project_id, rt_cdr_deid=rt_cdr_deid)
df1 = execute(client, q)
if df1.loc[0].sum() == 0:
    df = df.append(
        {
            'query':
                'Query3e non null value_source_value, value_as_string, and provider_id in observation table',
            'result':
                'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query':
                'Query3e non null value_source_value, value_as_string, and provider_id in observation table',
            'result':
                ''
        },
        ignore_index=True)
df1

# f. non null values in person table:

query = JINJA_ENV.from_string("""
SELECT COUNT(*) AS non_null_values FROM `{{project_id}}.{{rt_cdr_deid}}.person` WHERE
month_of_birth IS NOT NULL OR day_of_birth IS NOT NULL OR location_id IS NOT NULL
OR provider_id IS NOT NULL OR care_site_id IS NOT NULL OR person_source_value IS NOT NULL
OR gender_source_value IS NOT NULL OR gender_source_concept_id IS NOT NULL OR race_source_value IS NOT NULL
OR race_source_concept_id IS NOT NULL OR ethnicity_source_value IS NOT NULL OR ethnicity_source_concept_id IS NOT NULL
""")
q = query.render(project_id=project_id, rt_cdr_deid=rt_cdr_deid)
df1 = execute(client, q)
if df1.loc[0].sum() == 0:
    df = df.append(
        {
            'query': 'Query3f non null values in person table',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query3f non null values in person table',
            'result': ''
        },
        ignore_index=True)
df1

# g. non zero year_of_birth, race_concept_id, and ethnicity_concept_id in person table:

query = JINJA_ENV.from_string("""
SELECT COUNT(*) AS non_zero_values FROM `{{project_id}}.{{rt_cdr_deid}}.person`
WHERE race_concept_id != 0 OR ethnicity_concept_id != 0 OR year_of_birth != 0
""")
q = query.render(project_id=project_id, rt_cdr_deid=rt_cdr_deid)
df1 = execute(client, q)
if df1.loc[0].sum() == 0:
    df = df.append(
        {
            'query':
                'Query3g non zero year_of_birth, race_concept_id, and ethnicity_concept_id in person table:',
            'result':
                'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query':
                'Query3g non zero year_of_birth, race_concept_id, and ethnicity_concept_id in person table:',
            'result':
                ''
        },
        ignore_index=True)
df1

# h. non null provider_id in procedure_occurrence table:

query = JINJA_ENV.from_string("""
SELECT COUNT(*) AS non_null_provider_ids FROM `{{project_id}}.{{rt_cdr_deid}}.procedure_occurrence`
WHERE provider_id IS NOT NULL
""")
q = query.render(project_id=project_id, rt_cdr_deid=rt_cdr_deid)
df1 = execute(client, q)
if df1.loc[0].sum() == 0:
    df = df.append(
        {
            'query':
                'Query3h non null provider_id in procedure_occurrence table',
            'result':
                'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query':
                'Query3h non null provider_id in procedure_occurrence table',
            'result':
                ''
        },
        ignore_index=True)
df1

# i. non null provider_id and care_site_id in visit_occurrence table:

query = JINJA_ENV.from_string("""
SELECT COUNT(*) AS non_null_values FROM `{{project_id}}.{{rt_cdr_deid}}.visit_occurrence`
WHERE provider_id IS NOT NULL OR care_site_id IS NOT NULL
""")
q = query.render(project_id=project_id, rt_cdr_deid=rt_cdr_deid)
df1 = execute(client, q)
if df1.loc[0].sum() == 0:
    df = df.append(
        {
            'query':
                'Query3i non null provider_id and care_site_id in visit_occurrence table',
            'result':
                'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query':
                'Query3i non null provider_id and care_site_id in visit_occurrence table',
            'result':
                ''
        },
        ignore_index=True)
df1

# + [markdown] papermill={"duration": 0.023649, "end_time": "2021-02-02T22:30:39.115495", "exception": false, "start_time": "2021-02-02T22:30:39.091846", "status": "completed"} tags=[]
# # Q4 In the extension tables the srce_id/<omop_table>_id pairs match between the RT and CT:
# -

# ## 4a observation_ext

query = JINJA_ENV.from_string("""
SELECT COUNT (*) AS n_row_not_pass
FROM `{{project_id}}.{{ct_cdr_deid}}.observation_ext` c
LEFT JOIN `{{project_id}}.{{rt_cdr_deid}}.observation_ext` r
USING (observation_id)
WHERE c.src_id != r.src_id AND r.src_id IS NOT NULL and c.src_id IS NOT NULL
-- identify if RT and CT are USING the same masking values --
""")
q = query.render(project_id=project_id,
                 rt_cdr_deid=rt_cdr_deid,
                 ct_cdr_deid=ct_cdr_deid)
df1 = execute(client, q)
if df1.loc[0].sum() == 0:
    df = df.append(
        {
            'query': 'Query4a src_id matching in observation between CT and RT',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query4a src_id matching in observation between CT and RT',
            'result': 'Failure'
        },
        ignore_index=True)
df1

# ## 4b sandbox._site_mappings
# this rule was removed based on DC-2391 and no longer needed

# ## 4c pipeline_tables.site_maskings

# +
query = JINJA_ENV.from_string("""
SELECT COUNT (*) AS n_row_not_pass
FROM `{{project_id}}.{{pipeline}}.site_maskings` as c
LEFT JOIN `{{project_id}}.{{deid_sand}}.site_maskings` as r
USING (hpo_id)
WHERE c.src_id != r.src_id
-- registered tier did use the stabilized maskings for cross pipeline compatibility --
""")
q = query.render(project_id=project_id, pipeline=pipeline, deid_sand=deid_sand)
df1 = execute(client, q)
if df1.loc[0].sum() == 0:
    df = df.append(
        {
            'query': 'Query4c pipeline_tables.site_maskings matching',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query4c pipeline_tables.site_maskings matching',
            'result': 'Failure'
        },
        ignore_index=True)

df1

# + [markdown] papermill={"duration": 0.023649, "end_time": "2021-02-02T22:30:39.115495", "exception": false, "start_time": "2021-02-02T22:30:39.091846", "status": "completed"} tags=[]
# # Q5 A participant should have only one gender identity record in the observation table:

# + papermill={"duration": 2.338821, "end_time": "2021-02-02T22:30:41.501415", "exception": false, "start_time": "2021-02-02T22:30:39.162594", "status": "completed"} tags=[]
query = JINJA_ENV.from_string("""
WITH df1 AS (
SELECT value_source_concept_id, value_as_concept_id, count(person_id) as n_answers
FROM `{{project_id}}.{{rt_cdr_deid}}.observation`
WHERE observation_source_concept_id = 1585838
GROUP BY person_id, value_source_concept_id, value_as_concept_id
HAVING count(person_id) > 1)

SELECT COUNT (*) AS n_row_not_pass FROM df1
""")
q = query.render(project_id=project_id, rt_cdr_deid=rt_cdr_deid)
df1 = execute(client, q)
if df1.loc[0].sum() == 0:
    df = df.append(
        {
            'query':
                'Query5 only one gender identity record in the observation',
            'result':
                'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query':
                'Query5 only one gender identity record in the observation',
            'result':
                'Failure'
        },
        ignore_index=True)
df1

# + [markdown] papermill={"duration": 0.023649, "end_time": "2021-02-02T22:30:39.115495", "exception": false, "start_time": "2021-02-02T22:30:39.091846", "status": "completed"} tags=[]
# # Q6  A participant has one race answer (excluding ethnicity answers) in observation table:
# -

# correct one by francis
query = JINJA_ENV.from_string("""
WITH df1 AS (
SELECT person_id, count(value_source_concept_id) as countp
FROM `{{project_id}}.{{rt_cdr_deid}}.observation`
WHERE observation_source_concept_id = 1586140 AND value_as_concept_id !=1586147
GROUP BY person_id
)
SELECT COUNT (*) AS n_row_not_pass FROM df1
WHERE countp >1
""")
q = query.render(project_id=project_id, rt_cdr_deid=rt_cdr_deid)
df1 = execute(client, q)
if df1.loc[0].sum() == 0:
    df = df.append(
        {
            'query': 'Query6 has one race answer in observation',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query6 has one race answer in observation',
            'result': 'Failure'
        },
        ignore_index=True)
df1

# # Q7  Any response that isn’t straight (1585900) should be generalized to (2000000003):

# +
query = JINJA_ENV.from_string("""
WITH df1 AS (
SELECT person_id
FROM `{{project_id}}.{{combine}}.observation` ob
WHERE ob.observation_source_concept_id = 1585899
AND value_source_concept_id !=1585900)

SELECT COUNT (*) AS n_row_not_pass
FROM `{{project_id}}.{{rt_cdr_deid}}.observation` ob_deid
WHERE ob_deid.person_id in (SELECT person_id FROM df1)
and ob_deid.observation_source_concept_id = 1585899
and ob_deid.value_source_concept_id !=2000000003
""")
q = query.render(project_id=project_id,
                 rt_cdr_deid=rt_cdr_deid,
                 combine=combine)
df1 = execute(client, q)
if df1.loc[0].sum() == 0:
    df = df.append(
        {
            'query': 'Query7 non_straight gender be generalized',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query7 non_straight gender be generalized',
            'result': 'Failure'
        },
        ignore_index=True)

df1
# -

# # Q8 Sex at birth should be limited to 1585847, 1585846, and 2000000009:
#
# need some work here
#
# is orignal sql answering the question?

query = JINJA_ENV.from_string("""
SELECT COUNT(*) as n_row_not_pass
FROM `{{project_id}}.{{rt_cdr_deid}}.observation`
WHERE observation_source_concept_id = 1585845
AND value_source_concept_id not in (1585847, 1585846,2000000009)
""")
q = query.render(project_id=project_id, rt_cdr_deid=rt_cdr_deid)
df1 = execute(client, q)
if df1.loc[0].sum() == 0:
    df = df.append(
        {
            'query': 'Query8 correct sex_at_birth concept_id',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query8 correct sex_at_birth concept_id',
            'result': 'Failure'
        },
        ignore_index=True)
df1

# # Q9 Education levels ( value_source_concept_id) should be limited to 2000000007, 2000000006, 1585945, 43021808, 903079, 1177221, 1585946, 4260980, and 903096:

# +
query = JINJA_ENV.from_string("""
SELECT COUNT (*) as n_row_not_pass
FROM `{{project_id}}.{{rt_cdr_deid}}.observation`
WHERE observation_source_concept_id = 1585940
AND value_source_concept_id NOT IN (2000000007, 2000000006, 1585945, 43021808, 903079,
1177221, 1585946, 4260980, 903096)
""")
q = query.render(project_id=project_id, rt_cdr_deid=rt_cdr_deid)
df1 = execute(client, q)
if df1.loc[0].sum() == 0:
    df = df.append(
        {
            'query': 'Query9 correct education level concept_id',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query9 correct education level concept_id',
            'result': 'Failure'
        },
        ignore_index=True)

df1
# -

# # Q10. Employment records should be restricted to 2000000005 and 2000000004:
# need work
#
# questions: is other two ok?

query = JINJA_ENV.from_string("""
SELECT COUNT (*) as n_row_not_pass
FROM `{{project_id}}.{{rt_cdr_deid}}.observation`
WHERE observation_source_concept_id = 1585952
And value_source_concept_id not in (2000000005, 2000000004,903079,903096)
""")
q = query.render(project_id=project_id, rt_cdr_deid=rt_cdr_deid)
df1 = execute(client, q)
if df1.loc[0].sum() == 0:
    df = df.append(
        {
            'query': 'Query10 correct Employment records concept_id',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query10 correct Employment records concept_id',
            'result': 'Failure'
        },
        ignore_index=True)
df1

# # Q11. questionnaire_response_id should be the same between RT and CT:

query = JINJA_ENV.from_string("""
SELECT COUNT (*) AS n_row_not_pass
FROM `{{project_id}}.{{ct_cdr_deid}}.observation` c
LEFT JOIN `{{project_id}}.{{rt_cdr_deid}}.observation` r
USING (observation_id)
WHERE c.questionnaire_response_id != r.questionnaire_response_id
AND r.questionnaire_response_id IS NOT NULL
AND c.questionnaire_response_id IS NOT NULL
""")
q = query.render(project_id=project_id,
                 rt_cdr_deid=rt_cdr_deid,
                 ct_cdr_deid=ct_cdr_deid)
df1 = execute(client, q)
if df1.loc[0].sum() == 0:
    df = df.append(
        {
            'query': 'Query11  same questionnaire_response_id in RT and CT ',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query11 same questionnaire_response_id in RT and CT',
            'result': 'Failure'
        },
        ignore_index=True)
df1

# # Q12 update The participant lives in one state should have no EHR records from another state.
#
# DC-544/DC-512
#
# DC-2377
#

# +
# step1, get OMOP tables
query = JINJA_ENV.from_string("""

WITH
    table1 AS (
    SELECT
      table_name,
      column_name
    FROM
      `{{project_id}}.{{rt_cdr_deid_clean}}.INFORMATION_SCHEMA.COLUMNS`
    WHERE
      column_name='person_id' ),
    table2 AS (
    SELECT
      table_id AS table_name,
      row_count
    FROM
      `{{project_id}}.{{rt_cdr_deid_clean}}.__TABLES__`
    WHERE
      row_count>1)

  SELECT
    distinct table_name, column_name

  FROM
    `{{project_id}}.{{rt_cdr_deid_clean}}.INFORMATION_SCHEMA.COLUMNS` c
  WHERE
    table_name IN (
    SELECT
      DISTINCT table_name
    FROM
      table2
    WHERE
      table_name IN (
      SELECT
        DISTINCT table_name
      FROM
        table1))
    AND REGEXP_CONTAINS(column_name, r'(?i)(_id)')
    AND NOT REGEXP_CONTAINS(table_name, r'(?i)(_ext)')
    AND NOT REGEXP_CONTAINS(table_name, r'(?i)(steps)|(heart)|(activity)|(person)|death')
    AND NOT REGEXP_CONTAINS(column_name, r'(?i)(person_id)|(concept_id)|(provider_id)|(visit_occurrence_id)|(care_site_id)|(response_id)|(source_id)|(device_id)|(visit_detail)')


""")

q = query.render(project_id=project_id, rt_cdr_deid_clean=rt_cdr_deid_clean)
target_tables = execute(client, q)
target_tables.shape
# -

# have to manully add visit talbes
target_tables.loc[len(
    target_tables.index)] = ['visit_occurrence', 'visit_occurrence_id']
target_tables.loc[len(
    target_tables.index)] = ['visit_detail', 'visit_detail_id']
target_tables


def my_sql(table_name, column_name):

    query = JINJA_ENV.from_string("""

with df_person AS (
SELECT distinct person_id,
state_of_residence_concept_id as state_id_1,
state_of_residence_source_value

FROM `{{project_id}}.{{rt_cdr_deid_clean}}.person_ext` p
JOIN `{{project_id}}.{{reg_combine}}._mapping_src_hpos_to_allowed_states` ON value_source_concept_id=state_of_residence_concept_id
JOIN `{{project_id}}.{{deid_sand}}.site_maskings` m ON hpo_id=src_hpo_id
WHERE state_of_residence_concept_id IS NOT NULL
),

df_omop AS (
SELECT distinct person_id,ext.src_id,m.src_id,hpo_id,mhpo.src_hpo_id,m.hpo_id,
State,mhpo.value_source_concept_id as state_id_2
FROM `{{project_id}}.{{rt_cdr_deid_clean}}.{{table_name}}`
JOIN `{{project_id}}.{{rt_cdr_deid_clean}}.{{table_name}}_ext` ext USING ({{column_name}})
JOIN `{{project_id}}.{{deid_sand}}.site_maskings` m ON ext.src_id=m.src_id
JOIN `{{project_id}}.{{reg_combine}}._mapping_src_hpos_to_allowed_states` mhpo ON mhpo.src_hpo_id=m.hpo_id
WHERE ext.src_id !='PPI/PM'
)

SELECT '{{table_name}}' AS table_name,
COUNT(*) as row_counts,
CASE WHEN
  COUNT(*) > 0
  THEN 1 ELSE 0
END
 AS Failure_row_counts
FROM df_omop
LEFT JOIN df_person USING (person_id)
WHERE state_id_2 !=state_id_1

""")
    q = query.render(project_id=project_id,
                     reg_combine=reg_combine,
                     deid_sand=deid_sand,
                     rt_cdr_deid_clean=rt_cdr_deid_clean,
                     table_name=table_name,
                     column_name=column_name)
    df11 = execute(client, q)
    return df11


# +
# use a loop to get table name AND column name AND run sql function

result = [
    my_sql(table_name, column_name) for table_name, column_name in zip(
        target_tables['table_name'], target_tables['column_name'])
]
result

# +
# AND then get the result back FROM loop result list
n = len(target_tables.index)
res2 = pd.DataFrame(result[0])

for x in range(1, n):
    res2 = res2.append(result[x])

res2 = res2.sort_values(by='Failure_row_counts', ascending=False)
res2
# -

if res2['Failure_row_counts'].sum() == 0:
    df = df.append(
        {
            'query': 'Query12  person state info matches EHR state info',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query12 person state info matches EHR state info',
            'result': 'Failure'
        },
        ignore_index=True)

# # Summary_deid_extra_validation


# +
def highlight_cells(val):
    color = 'red' if 'Failure' in val else 'white'
    return f'background-color: {color}'


df.style.applymap(highlight_cells).set_properties(**{'text-align': 'left'})
