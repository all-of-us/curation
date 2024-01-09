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

# # DST-251 Add more QA/QCs for the Controlled Tier data
#
# We want to mirror the QCs in here   to add more checks to the Controlled Tier (CT).
#
# The privacy checks against the CT are done. But we need more QCs that check against the OMOP rules AND against the Program constraints.
#
# Create a notebook for the following checks AND commit them here. If you are just starting working WITH curation, follow the instructions here  .
#
# The checks are:
#
# 1.all the birthdates are set to 15th June of the birth year in person table
#
# 2 No dates earlier than birth dates
#
# 3 No dates earlier than 1980 in any table, except for the observation
#
# 4 No dates after death:done
#
# 5 No WITHdrawn participants: need pdr access
#
# 6 No data after participant's suspension: need pdr access
#
# 7 All participants have basics
#
# 8 All participants WITH EHR data have said yes to EHR consents
#
# 9 All participants WITH Fitbit have said yes to primary consents: need PDR access
#
# 10 All primary keys are in _ext
#
# 11 No duplicated primary keys
#
# 12 OMOP tables should have standard concept ids
#
# 13 observation concept ids (4013886, 4135376, 4271761) that have dates equal to birth dates should be set to CDR cutoff date
#
# 14 all other observation concept ids WITH dates similar to birth dates other than the 3 above should be removed
#
# 15 All the descendants of ancestor_concept_id IN (4054924, 141771) -- motor vehicle accidents should be dropped in condition_occurrence table
#
# 19 Test for the presences of at least one race/ethnicity sub category
#

# + tags=["parameters"]
# Parameters
project_id = ""
rt_dataset = ""
ct_dataset = ""
combined_dataset = ""
deid_sandbox = ""
earliest_ehr_date = ""
cut_off_date = ""

# +
import pandas as pd
from analytics.cdr_ops.notebook_utils import execute
from common import JINJA_ENV
from gcloud.bq import BigQueryClient

client = BigQueryClient(project_id)

pd.options.display.max_rows = 120
# -

# summary will have a summary in the end
df = pd.DataFrame(columns=['query', 'result'])

# wear_consent and wear_consent_ptsc question and module concepts where not in multiple surveys.
# The concepts found in multiple surveys are: 'resultsconsent_helpmewithconsent' and 'helpmewithconsent_name'
WEAR_SURVEY_CODES = [
    'havesmartphone', 'wearwatch', 'usetracker', 'wear12months', 'receivesms',
    'frequency', 'agreetoshare', 'onlyparticipantinhousehold', 'haveaddress',
    'resultsconsent_wear', 'email_help_consent', 'timeofday',
    'wearconsent_signature', 'wearconsent_todaysdate', 'wear_consent',
    'wear_consent_ptsc'
]

# # Query1: all the birthdates are set to 15th June of the birth year in person table
#

# +
# step1 , to get the tables AND columns that have person_id, size >1 AND DATE columns AND save to a data frame
query = JINJA_ENV.from_string("""

SELECT
'person' as table_name,
'birth_datetime' as column_name,
count (*) as row_counts_failures
    FROM {{project_id}}.{{ct_dataset}}.person
    where EXTRACT(MONTH FROM DATE (birth_datetime))!=6
    or EXTRACT(DAY FROM DATE (birth_datetime)) !=15
""")

q = query.render(project_id=project_id, ct_dataset=ct_dataset)
res = execute(client, q)
res.shape
# -

res

if res.iloc[:, 2].sum() == 0:
    df = df.append(
        {
            'query':
                'Query1: all the birthdates are set to 06-15 in person table',
            'result':
                'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query':
                'Query1: all the birthdates are set to 06-15 in person table',
            'result':
                'Failure'
        },
        ignore_index=True)

# # Query2: No dates before birth_date
#

# +
# step1 , to get the tables AND columns that have person_id, size >1 AND DATE columns AND save to a data frame
query = JINJA_ENV.from_string("""

WITH
    table1 AS (
    SELECT
      table_name,
      column_name
    FROM
      `{{project_id}}.{{ct_dataset}}.INFORMATION_SCHEMA.COLUMNS`
    WHERE
      column_name='person_id' ),
    table2 AS (
    SELECT
      table_id AS table_name,
      row_count
    FROM
      `{{project_id}}.{{ct_dataset}}.__TABLES__`
    WHERE
      row_count>1)

  SELECT
    table_name,
    column_name
  FROM
    `{{project_id}}.{{ct_dataset}}.INFORMATION_SCHEMA.COLUMNS` c
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
    AND c.data_type IN ('DATE','TIMESTAMP')
    AND NOT REGEXP_CONTAINS(column_name, r'(?i)(_PAR)')
    AND NOT REGEXP_CONTAINS(column_name, r'(?i)(birth)')
""")

q = query.render(project_id=project_id, ct_dataset=ct_dataset)
target_tables = execute(client, q)
target_tables.shape
# -

target_tables


def my_sql(table_name, column_name):

    query = JINJA_ENV.from_string("""
    WITH rt_map as (
     SELECT
  research_id AS person_id,
  SAFE_CAST (birth_datetime AS DATE) AS birth_date
FROM
  {{project_id}}.{{rt_dataset}}.person
JOIN
  {{project_id}}.{{deid_sandbox}}._deid_map
USING
  (person_id)
  )

SELECT
'{{table_name}}' AS table_name,
'{{column_name}}' AS column_name,
COUNT(*) AS row_counts_failure,
CASE WHEN
  COUNT(*) > 0
  THEN 1 ELSE 0
END
 AS Failure_birth_date

FROM `{{project_id}}.{{ct_dataset}}.{{table_name}}` c
JOIN rt_map r USING (person_id)
WHERE  DATE(c.{{column_name}})< r.birth_date
""")
    q = query.render(project_id=project_id,
                     rt_dataset=rt_dataset,
                     ct_dataset=ct_dataset,
                     deid_sandbox=deid_sandbox,
                     table_name=table_name,
                     column_name=column_name)
    df11 = execute(client, q)
    return df11


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

res2 = res2.sort_values(by='row_counts_failure', ascending=False)
res2
# -

if res2.iloc[:, 3].sum() == 0:
    df = df.append(
        {
            'query': 'Query2: No dates before birth_date',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query2: No dates before birth_date',
            'result': 'Failure'
        },
        ignore_index=True)

# # Query3: No dates after 30_days_after_death

# need to do obs table seperatly
df1 = target_tables
df1 = df1[df1.table_name.str.contains("obs")]
df1 = df1[~df1.table_name.str.contains("period")]
target_tables2 = df1
target_tables2


def my_sql(table_name, column_name):

    query = JINJA_ENV.from_string("""

    WITH df1 as (
SELECT
person_id,c.{{column_name}},
DATE_ADD(d.death_date, INTERVAL 30 DAY) AS after_death_30_days

FROM `{{project_id}}.{{ct_dataset}}.{{table_name}}` c
FULL JOIN `{{project_id}}.{{ct_dataset}}.aou_death` d USING (person_id)
WHERE  DATE(c.{{column_name}}) > d.death_date
AND d.primary_death_record = TRUE
AND c.{{table_name}}_concept_id NOT IN (4013886, 4135376, 4271761)
)

SELECT
'{{table_name}}' AS table_name,
'{{column_name}}' AS column_name,
COUNT(*) AS row_counts_failure,
CASE WHEN
  COUNT(*) > 0
  THEN 1 ELSE 0
END
 AS Failure_after_death_30_days

FROM df1
WHERE  DATE({{column_name}}) > after_death_30_days

""")
    q = query.render(project_id=project_id,
                     ct_dataset=ct_dataset,
                     table_name=table_name,
                     column_name=column_name)
    df11 = execute(client, q)
    return df11


# +
result = [
    my_sql(table_name, column_name) for table_name, column_name in zip(
        target_tables2['table_name'], target_tables2['column_name'])
]
result

# AND then get the result back FROM loop result list
n = len(target_tables2.index)
res2 = pd.DataFrame(result[0])

for x in range(1, n):
    res2 = res2.append(result[x])

res2 = res2.sort_values(by='row_counts_failure', ascending=False)
res2
# -

# then do the rest of tables
df1 = target_tables
df1 = df1[~df1.table_name.str.contains("obs")]
target_tables2 = df1
target_tables2


def my_sql(table_name, column_name):

    query = JINJA_ENV.from_string("""

    WITH death_30_days as (
SELECT
c.{{column_name}},
DATE_ADD(d.death_date, INTERVAL 30 DAY) AS after_death_30_days
 FROM `{{project_id}}.{{ct_dataset}}.{{table_name}}` c
 JOIN `{{project_id}}.{{ct_dataset}}.aou_death` d USING (person_id)
WHERE  DATE(c.{{column_name}}) > d.death_date
AND d.primary_death_record = TRUE
)

SELECT
'{{table_name}}' AS table_name,
'{{column_name}}' AS column_name,
COUNT(*) AS row_counts_failure,
CASE WHEN
  COUNT(*) > 0
  THEN 1 ELSE 0
END
 AS Failure_after_death_30_days
FROM death_30_days
WHERE  DATE({{column_name}}) > after_death_30_days
""")
    q = query.render(project_id=project_id,
                     ct_dataset=ct_dataset,
                     table_name=table_name,
                     column_name=column_name)
    df11 = execute(client, q)
    return df11


# +
result = [
    my_sql(table_name, column_name) for table_name, column_name in zip(
        target_tables2['table_name'], target_tables2['column_name'])
]
result

# AND then get the result back FROM loop result list
n = len(target_tables2.index)
res21 = pd.DataFrame(result[0])

for x in range(1, n):
    res21 = res21.append(result[x])

res21 = res21.sort_values(by='row_counts_failure', ascending=False)
res21
# -

# combine both results
res2 = res2.append(res21, ignore_index=True)
res2

if res2.iloc[:, 3].sum() == 0:
    df = df.append({
        'query': 'Query3: No dates after death',
        'result': 'PASS'
    },
                   ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query3: No dates after death',
            'result': 'Failure'
        },
        ignore_index=True)

# # Query4: No dates earlier than 1980 in any table, except for the observation

# get target tables WITHout observation
df1 = target_tables
df1 = df1[~df1.table_name.str.contains("obs")]
target_tables2 = df1
target_tables2


def my_sql(table_name, column_name):

    query = JINJA_ENV.from_string("""

SELECT
'{{table_name}}' AS table_name,
'{{column_name}}' AS column_name,
COUNT(*) AS row_counts_failure,
CASE WHEN
  COUNT(*) > 0
  THEN 1 ELSE 0
END
 AS Failure_cutoff_date
FROM `{{project_id}}.{{ct_dataset}}.{{table_name}}` c
WHERE  DATE(c.{{column_name}}) < '{{earliest_ehr_date}}'
""")
    q = query.render(project_id=project_id,
                     ct_dataset=ct_dataset,
                     table_name=table_name,
                     column_name=column_name,
                     earliest_ehr_date=earliest_ehr_date)
    df11 = execute(client, q)
    return df11


result = [
    my_sql(table_name, column_name) for table_name, column_name in zip(
        target_tables2['table_name'], target_tables2['column_name'])
]

# +
n = len(target_tables2.index)
res2 = pd.DataFrame(result[0])

for x in range(1, n):
    res2 = res2.append(result[x])

res2 = res2.sort_values(by='row_counts_failure', ascending=False)
res2
# -

if res2.iloc[:, 3].sum() == 0:
    df = df.append(
        {
            'query':
                'Query4: No dates earlier than 1980 in any table, except for the observation',
            'result':
                'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query':
                'Query4: No dates earlier than 1980 in any table, except for the observation',
            'result':
                'Failure'
        },
        ignore_index=True)

# # query 7  All participants have basics data
#
# There should not be any data in the CT datasets for participants who don't have basics data.
#
# If this check fails investigate. Ensure all participants lacking basics data have been dropped.
#
# Note: Since the CR `drop_participants_without_any_basics` occurs in the RDR cleaning stage it is possible that a small number of participants have their Basics data dropped between the rdr and CT pipeline stages. At this time(V8), participants without Basics data in the CT dataset are allowed in the CDR if they had Basics data in the rdr stage.

# +
query = JINJA_ENV.from_string("""
WITH person_all as (
SELECT person_id FROM `{{project_id}}.{{ct_dataset}}.person`),

person_basics as (
SELECT distinct person_id
FROM
`{{project_id}}.{{ct_dataset}}.concept`
JOIN `{{project_id}}.{{ct_dataset}}.concept_ancestor` on (concept_id=ancestor_concept_id)
JOIN `{{project_id}}.{{ct_dataset}}.observation` on (descendant_concept_id=observation_concept_id)
JOIN `{{project_id}}.{{ct_dataset}}.observation_ext` USING(observation_id)
WHERE observation_concept_id NOT IN (40766240,43528428,1585389)
AND concept_class_id='Module'
AND concept_name IN ('The Basics')
AND NOT REGEXP_CONTAINS(src_id, r'(?i)(PPI/PM)|(EHR site)')
AND questionnaire_response_id is not null)

SELECT
'observation' AS table_name,
'person_id' AS column_name,
COUNT(*) AS row_counts_failure,
CASE WHEN
  COUNT(*) > 0
  THEN 1 ELSE 0
END
 AS Failure_bascis

FROM person_all
WHERE person_id NOT IN (SELECT person_id FROM person_basics)
""")

q = query.render(project_id=project_id, ct_dataset=ct_dataset)
df1 = execute(client, q)
df1.shape
# -

df1

if df1.iloc[:, 3].sum() == 0:
    df = df.append(
        {
            'query': 'Query7: All participants have basics',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query7: All participants have basics',
            'result': 'Failure'
        },
        ignore_index=True)

# # query 8 All participants WITH EHR data have said yes to EHR consents
#
# yes to 1586099 EHR Consent PII: Consent Permission

# +
query = JINJA_ENV.from_string("""
WITH person_ehr as (
  SELECT distinct person_id FROM `{{project_id}}.{{ct_dataset}}.observation`
  JOIN `{{project_id}}.{{ct_dataset}}.observation_ext` USING (observation_id)
  WHERE REGEXP_CONTAINS(src_id, r'(?i)EHR site')

  UNION DISTINCT

  SELECT distinct person_id FROM `{{project_id}}.{{ct_dataset}}.measurement`
  JOIN `{{project_id}}.{{ct_dataset}}.measurement_ext` USING (measurement_id)
  WHERE REGEXP_CONTAINS(src_id, r'(?i)EHR site')

  UNION DISTINCT

  SELECT distinct person_id FROM `{{project_id}}.{{ct_dataset}}.condition_occurrence`
  JOIN `{{project_id}}.{{ct_dataset}}.condition_occurrence_ext` USING (condition_occurrence_id)
  WHERE REGEXP_CONTAINS(src_id, r'(?i)EHR site')

  UNION DISTINCT

  SELECT distinct person_id FROM `{{project_id}}.{{ct_dataset}}.device_exposure`
  JOIN `{{project_id}}.{{ct_dataset}}.device_exposure_ext` USING (device_exposure_id)
  WHERE REGEXP_CONTAINS(src_id, r'(?i)EHR site')

  UNION DISTINCT

  SELECT distinct person_id FROM `{{project_id}}.{{ct_dataset}}.drug_exposure`
  JOIN `{{project_id}}.{{ct_dataset}}.drug_exposure_ext` USING (drug_exposure_id)
  WHERE REGEXP_CONTAINS(src_id, r'(?i)EHR site')

  UNION DISTINCT

  SELECT distinct person_id FROM `{{project_id}}.{{ct_dataset}}.visit_occurrence`
  JOIN `{{project_id}}.{{ct_dataset}}.visit_occurrence_ext` USING (visit_occurrence_id)
  WHERE REGEXP_CONTAINS(src_id, r'(?i)EHR site')
),

person_yes as (
  SELECT
    distinct person_id
  FROM
    `{{project_id}}.{{ct_dataset}}.observation`
  WHERE
    observation_concept_id = 1586099
  AND
    value_source_concept_id = 1586100
)

SELECT
'person_ehr' AS table_name,
'person_id' AS column_name,
COUNT(*) AS row_counts_failure,
CASE WHEN
  COUNT(*) > 0
  THEN 1 ELSE 0
END
 AS Failure_EHR_consent_permission

FROM person_ehr
WHERE  person_id NOT IN (SELECT person_id FROM person_yes)
""")

q = query.render(project_id=project_id, ct_dataset=ct_dataset)
df1 = execute(client, q)
df1.shape
df1
# -

if df1.iloc[:, 3].sum() == 0:
    df = df.append(
        {
            'query':
                'Query8: All participants WITH EHR data have said yes to EHR consents',
            'result':
                'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query':
                'Query8: All participants WITH EHR data have said yes to EHR consents',
            'result':
                'Failure'
        },
        ignore_index=True)

# # Query 10 All primary keys are in _ext

# +
query = JINJA_ENV.from_string("""
WITH
    table1 AS (
    SELECT
      table_name,
      column_name
    FROM
      `{{project_id}}.{{ct_dataset}}.INFORMATION_SCHEMA.COLUMNS`
    WHERE
      column_name='person_id' ),
    table2 AS (
    SELECT
      table_id AS table_name,
      row_count
    FROM
      `{{project_id}}.{{ct_dataset}}.__TABLES__`
    WHERE
      row_count>1)

  SELECT
    table_name,
    column_name
  FROM
    `{{project_id}}.{{ct_dataset}}.INFORMATION_SCHEMA.COLUMNS` c
  WHERE
    table_name IN (
    SELECT
      DISTINCT table_name
    FROM
      table2
    WHERE
     (table_name IN (
      SELECT
        DISTINCT table_name
      FROM
        table1))
    AND REGEXP_CONTAINS(column_name, r'(?i)(_id)')
    AND NOT REGEXP_CONTAINS(table_name, r'(?i)(person)')
    AND NOT REGEXP_CONTAINS(table_name, r'(?i)(aou_death)')
    AND NOT REGEXP_CONTAINS(column_name, r'(?i)(_PAR)')
    AND NOT REGEXP_CONTAINS(column_name, r'(?i)(person_)')
    AND NOT REGEXP_CONTAINS(column_name, r'(?i)(_concept)')
    AND NOT REGEXP_CONTAINS(column_name, r'(?i)(_site)')
    AND NOT REGEXP_CONTAINS(column_name, r'(?i)(provider)')
    AND NOT REGEXP_CONTAINS(column_name, r'(?i)(response)')
    AND NOT REGEXP_CONTAINS(column_name, r'(?i)(location)')
    AND NOT REGEXP_CONTAINS(column_name, r'(?i)(source)')
      AND NOT REGEXP_CONTAINS(column_name, r'(?i)(visit_occurrence)')
      AND NOT REGEXP_CONTAINS(column_name, r'(?i)(visit_detail)')
      AND NOT REGEXP_CONTAINS(column_name, r'(?i)(unique)')
      )

      OR (
    (table_name IN (
      SELECT
        DISTINCT table_name
      FROM
        table1))
        AND REGEXP_CONTAINS(table_name, r'(?i)(visit_occurrence)')
        AND REGEXP_CONTAINS(column_name, r'(?i)(visit_occurrence)')
    AND NOT REGEXP_CONTAINS(column_name, r'(?i)(preceding)') )

    OR (
    (table_name IN (
      SELECT
        DISTINCT table_name
      FROM
        table1))
        AND REGEXP_CONTAINS(table_name, r'(?i)(visit_detail)')
        AND REGEXP_CONTAINS(column_name, r'(?i)(visit_detail_id)')
        AND NOT REGEXP_CONTAINS(column_name, r'(?i)(preceding)') )
   """)

"""
OR (
    (table_name IN (
      SELECT
        DISTINCT table_name
      FROM
        table1))
        AND REGEXP_CONTAINS(table_name, r'(?i)(person)')
         AND NOT REGEXP_CONTAINS(table_name, r'(?i)(person_ext)')
        AND REGEXP_CONTAINS(column_name, r'(?i)(person_id)')
     )
"""

q = query.render(project_id=project_id, ct_dataset=ct_dataset)
target_tables = execute(client, q)
target_tables.shape
# -

target_tables


def my_sql(table_name, column_name):

    query = JINJA_ENV.from_string("""
    SELECT
'{{table_name}}' AS table_name,
'{{column_name}}' AS column_name,

COUNT(*) AS row_counts_failure,
CASE WHEN
  COUNT(*) > 0
  THEN 1 ELSE 0
END
 AS Failure_primary_key_match

FROM `{{project_id}}.{{ct_dataset}}.{{table_name}}` c
JOIN `{{project_id}}.{{ct_dataset}}.{{table_name}}_ext` ext USING ({{column_name}})
WHERE  c.{{column_name}} !=ext.{{column_name}}
""")
    q = query.render(project_id=project_id,
                     ct_dataset=ct_dataset,
                     table_name=table_name,
                     column_name=column_name)
    df11 = execute(client, q)
    return df11


result = [
    my_sql(table_name, column_name) for table_name, column_name in zip(
        target_tables['table_name'], target_tables['column_name'])
]
result

# +
n = len(target_tables.index)
res2 = pd.DataFrame(result[0])

for x in range(1, n):
    res2 = res2.append(result[x])

res2 = res2.sort_values(by='row_counts_failure', ascending=False)
res2
# -

if res2.iloc[:, 3].sum() == 0:
    df = df.append(
        {
            'query': 'Query10: All primary keys are in _ext',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query10: All primary keys are in _ext',
            'result': 'Failure'
        },
        ignore_index=True)

# # query 11 No duplicated primary keys¶

# +
query = JINJA_ENV.from_string("""

WITH
    table1 AS (
    SELECT
      table_name,
      column_name
    FROM
      `{{project_id}}.{{ct_dataset}}.INFORMATION_SCHEMA.COLUMNS`
    WHERE
      column_name='person_id' ),
    table2 AS (
    SELECT
      table_id AS table_name,
      row_count
    FROM    `{{project_id}}.{{ct_dataset}}.__TABLES__`
    WHERE     row_count>1)

  SELECT
    table_name,
    column_name
  FROM
    `{{project_id}}.{{ct_dataset}}.INFORMATION_SCHEMA.COLUMNS` c
  WHERE
    table_name IN (
    SELECT
      DISTINCT table_name
    FROM
      table2
    WHERE
     (table_name IN (
      SELECT
        DISTINCT table_name
      FROM
        table1))
    AND REGEXP_CONTAINS(column_name, r'(?i)(_id)')
    AND NOT REGEXP_CONTAINS(table_name, r'(?i)(person)')
    AND NOT REGEXP_CONTAINS(table_name, r'(?i)(aou_death)')
    AND NOT REGEXP_CONTAINS(column_name, r'(?i)(_PAR)')
    AND NOT REGEXP_CONTAINS(column_name, r'(?i)(person_)')
    AND NOT REGEXP_CONTAINS(column_name, r'(?i)(_concept)')
    AND NOT REGEXP_CONTAINS(column_name, r'(?i)(_site)')
    AND NOT REGEXP_CONTAINS(column_name, r'(?i)(provider)')
    AND NOT REGEXP_CONTAINS(column_name, r'(?i)(response)')
    AND NOT REGEXP_CONTAINS(column_name, r'(?i)(location)')
    AND NOT REGEXP_CONTAINS(column_name, r'(?i)(source)')
      AND NOT REGEXP_CONTAINS(column_name, r'(?i)(visit_occurrence)')
      AND NOT REGEXP_CONTAINS(column_name, r'(?i)(visit_detail)')
      AND NOT REGEXP_CONTAINS(column_name, r'(?i)(unique)')
      )

      OR (
          (table_name IN (
      SELECT
        DISTINCT table_name
      FROM
        table1))
        AND REGEXP_CONTAINS(table_name, r'(?i)(visit_occurrence)')
        AND REGEXP_CONTAINS(column_name, r'(?i)(visit_occurrence)')
    AND NOT REGEXP_CONTAINS(column_name, r'(?i)(preceding)') )

    OR (
          (table_name IN (
      SELECT
        DISTINCT table_name
      FROM
        table1))
        AND REGEXP_CONTAINS(table_name, r'(?i)(visit_detail)')
        AND REGEXP_CONTAINS(column_name, r'(?i)(visit_detail_id)')
    AND NOT REGEXP_CONTAINS(column_name, r'(?i)(preceding)') )

    OR (
          (table_name IN (
      SELECT
        DISTINCT table_name
      FROM
        table1))
        AND REGEXP_CONTAINS(table_name, r'(?i)(aou_death)')
        AND REGEXP_CONTAINS(column_name, r'(?i)(aou_death_id)')
    AND NOT REGEXP_CONTAINS(column_name, r'(?i)(src)') )
 """)

"""
OR (
           (table_name IN (
      SELECT
        DISTINCT table_name
      FROM
        table1))
        AND REGEXP_CONTAINS(table_name, r'(?i)(person)')
         AND NOT REGEXP_CONTAINS(table_name, r'(?i)(person_ext)')
        AND REGEXP_CONTAINS(column_name, r'(?i)(person_id)')
     )
"""

q = query.render(project_id=project_id, ct_dataset=ct_dataset)
target_tables = execute(client, q)
target_tables.shape
# -

target_tables


def my_sql(table_name, column_name):

    query = JINJA_ENV.from_string("""

SELECT
'{{table_name}}' AS table_name,
'{{column_name}}' AS column_name,
{{column_name}},

COUNT(*) AS row_counts_failure,
CASE WHEN
  COUNT(*) > 0
  THEN 1 ELSE 0
END
 AS Failure_primary_key_match
FROM `{{project_id}}.{{ct_dataset}}.{{table_name}}` c
GROUP BY {{column_name}}
HAVING COUNT(*) >1
""")
    q = query.render(project_id=project_id,
                     ct_dataset=ct_dataset,
                     table_name=table_name,
                     column_name=column_name)
    df11 = execute(client, q)
    return df11


result = [
    my_sql(table_name, column_name) for table_name, column_name in zip(
        target_tables['table_name'], target_tables['column_name'])
]
result

# +
n = len(target_tables.index)
res2 = pd.DataFrame(result[0])

for x in range(1, n):
    res2 = res2.append(result[x])

res2 = res2.sort_values(by='row_counts_failure', ascending=False)
res2
# -

if res2.empty:
    df = df.append(
        {
            'query': 'Query11 No duplicated primary keys',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query11 No duplicated primary keys',
            'result': 'Failure'
        },
        ignore_index=True)

# # Query 12 OMOP tables should have standard concept ids, done WITH questions

# +
query = JINJA_ENV.from_string("""

WITH
    table1 AS (
    SELECT
      table_name,
      column_name
    FROM
      `{{project_id}}.{{ct_dataset}}.INFORMATION_SCHEMA.COLUMNS`
    WHERE
      column_name='person_id' ),
    table2 AS (
    SELECT
      table_id AS table_name,
      row_count
    FROM
      `{{project_id}}.{{ct_dataset}}.__TABLES__`
    WHERE
      row_count>1)

  SELECT
    table_name,
    column_name
  FROM
    `{{project_id}}.{{ct_dataset}}.INFORMATION_SCHEMA.COLUMNS` c
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
    AND REGEXP_CONTAINS(column_name, r'(?i)(_concept_id)')
    AND NOT REGEXP_CONTAINS(column_name, r'(?i)(_PAR)')
    AND NOT REGEXP_CONTAINS(column_name, r'(?i)(_source)')
""")

q = query.render(project_id=project_id, ct_dataset=ct_dataset)
target_tables = execute(client, q)
target_tables.shape
# -

target_tables


def my_sql(table_name, column_name):

    query = JINJA_ENV.from_string("""

SELECT
'{{table_name}}' AS table_name,
'{{column_name}}' AS column_name,

COUNT(*) AS row_counts_failure,
CASE WHEN
  COUNT(*) > 0
  THEN 1 ELSE 0
END
 AS Failure_primary_key_match

FROM `{{project_id}}.{{ct_dataset}}.concept` c
JOIN `{{project_id}}.{{ct_dataset}}.{{table_name}}` ON (concept_id={{column_name}})
WHERE  standard_concept not in ('S', 'C')
AND {{column_name}} !=0
""")
    q = query.render(project_id=project_id,
                     ct_dataset=ct_dataset,
                     table_name=table_name,
                     column_name=column_name)
    df11 = execute(client, q)
    return df11


result = [
    my_sql(table_name, column_name) for table_name, column_name in zip(
        target_tables['table_name'], target_tables['column_name'])
]
result

# +
n = len(target_tables.index)
res2 = pd.DataFrame(result[0])

for x in range(1, n):
    res2 = res2.append(result[x])

res2 = res2.sort_values(by='row_counts_failure', ascending=False)
res2
# -

if res2.iloc[:, 3].sum() == 0:
    df = df.append(
        {
            'query': 'Query12: OMOP tables should have standard concept ids',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query12: OMOP tables should have standard concept ids',
            'result': 'Failure'
        },
        ignore_index=True)

# # Query 13 observation concept ids (4013886, 4135376, 4271761) that have dates equal to birth dates should be set to CDR cutoff date
#
# Note: CT person table does not contain exact birth dates, and some observations with exact date matches might not exist in RT, therefore, the combined dataset is used in this check.

# +

query = JINJA_ENV.from_string("""

 WITH rows_having_brith_date as (

SELECT distinct observation_id
FROM
`{{project_id}}.{{combined_dataset}}.observation` ob
JOIN `{{project_id}}.{{combined_dataset}}.person` p USING (person_id)
WHERE  observation_concept_id in (4013886, 4135376, 4271761)
AND observation_date=DATE(p.birth_datetime)
 )

 SELECT
'observation' AS table_name,
'observation_date' AS column_name,
COUNT(*) AS row_counts_failure,
CASE WHEN
  COUNT(*) > 0
  THEN 1 ELSE 0
END
 AS Failure_birth_date_cut_Off
FROM `{{project_id}}.{{ct_dataset}}.observation`
WHERE observation_id IN (SELECT observation_id FROM rows_having_brith_date)
AND observation_date != '{{cut_off_date}}'
 """)

q = query.render(project_id=project_id,
                 ct_dataset=ct_dataset,
                 combined_dataset=combined_dataset,
                 cut_off_date=cut_off_date)
df1 = execute(client, q)
df1
# -

if df1.iloc[:, 3].sum() == 0:
    df = df.append(
        {
            'query':
                'Query13: observation concept ids (4013886, 4135376, 4271761) that have dates equal to birth dates should be set to CDR cutoff date',
            'result':
                'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query':
                'Query13: observation concept ids (4013886, 4135376, 4271761) that have dates equal to birth dates should be set to CDR cutoff date',
            'result':
                'Failure'
        },
        ignore_index=True)

#
# # Query 14 done all other observation concept ids WITH dates similar to birth dates other than the 3 above should be removed
#
# Related to CR year_of_birth_records_suppression

# +
query = JINJA_ENV.from_string("""

WITH rows_having_brith_date as (

SELECT observation_id
  FROM {{project_id}}.{{ct_dataset}}.observation ob
JOIN  {{project_id}}.{{ct_dataset}}.person p USING (person_id)
 WHERE (observation_concept_id NOT IN (4013886, 4135376, 4271761) OR observation_concept_id IS NULL)
  AND ABS(EXTRACT(YEAR FROM observation_date)- p.year_of_birth) < 2
  )

SELECT
'observation' AS table_name,
 'observation_date' AS column_name,
 COUNT(*) AS row_counts_failure,
CASE WHEN
  COUNT(*) > 0
  THEN 1 ELSE 0
END
 AS Failure_no_birth_date
FROM `{{project_id}}.{{ct_dataset}}.observation` ob
WHERE  observation_id IN (SELECT observation_id FROM rows_having_brith_date)
""")

q = query.render(project_id=project_id,
                 ct_dataset=ct_dataset)
df1 = execute(client, q)

df1
# -

if df1.iloc[:, 3].sum() == 0:
    df = df.append(
        {
            'query':
                'Query14: no birth_date in observation table except (4013886, 4135376, 4271761)',
            'result':
                'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query':
                'Query14: no birth_date in observation table except (4013886, 4135376, 4271761)',
            'result':
                'Failure'
        },
        ignore_index=True)

# # Query 15:  All the descendants of ancestor_concept_id IN (4054924, 141771) -- motor vehicle accidents should be dropped in condition table¶

# +
query = JINJA_ENV.from_string("""

SELECT
'condition_occurrence' AS table_name,
 'concept_id' AS column_name,
 COUNT(*) AS row_counts_failure,
CASE WHEN
  COUNT(*) > 0
  THEN 1 ELSE 0
END
 AS Failure_no_two_concept_ids
FROM `{{project_id}}.{{ct_dataset}}.condition_occurrence`
JOIN `{{project_id}}.{{ct_dataset}}.concept` c ON (condition_concept_id=c.concept_id)
JOIN `{{project_id}}.{{ct_dataset}}.concept_ancestor` ON (c.concept_id=descendant_concept_id)
WHERE ancestor_concept_id IN (4054924, 141771)
""")

q = query.render(project_id=project_id,
                 rt_dataset=rt_dataset,
                 ct_dataset=ct_dataset)

df1 = execute(client, q)
df1.shape
# -

df1

if df1.iloc[:, 3].sum() == 0:
    df = df.append(
        {
            'query':
                'Query15: All the descendants of ancestor_concept_id IN (4054924, 141771)  be dropped in condition table',
            'result':
                'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query':
                'Query15: All the descendants of ancestor_concept_id IN (4054924, 141771)  be dropped in condition table',
            'result':
                'Failure'
        },
        ignore_index=True)

# # Query 16:  All the data from drug_era, condition_era, and dose_era tables is dropped

# +
era_tables = ['dose_era', 'drug_era', 'condition_era']


def query_template(table_era):

    query = JINJA_ENV.from_string("""
      WITH df1 AS (
        SELECT
          `{{table_era}}_id`
        FROM
          `{{project_id}}.{{ct_dataset}}.{{table_era}}`
      )

      SELECT
        '{{table_era}}' as table_name, COUNT(*) AS row_count
      FROM
        df1
    """)

    q = query.render(project_id=project_id,
                     ct_dataset=ct_dataset,
                     table_era=table_era)
    df2 = execute(client, q)
    return df2


result = []
for table in era_tables:
    result.append(query_template(table))

n = len(era_tables)
res2 = pd.DataFrame(result[0])

for x in range(1, n):
    res2 = res2.append(result[x])

res2 = res2.sort_values(by='row_count', ascending=False)

if res2['row_count'].sum() == 0:
    df = df.append(
        {
            'query':
                'Query16: All the data from drug_era, condition_era, and dose_era tables is dropped',
            'result':
                'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query':
                'Query16: All the data from drug_era, condition_era, and dose_era tables is dropped',
            'result':
                'Failure'
        },
        ignore_index=True)
res2

# # final summary result
# -


# # Q17 Wear study table
#
# DC-3340
#
# This check confirms that the wear_study table contains:
# 1. Only one row per participant
# 2. Wear study participants are also found in the CDR person table.
# 3. Wear study participants have primary consent records in observation.
#
# **If check fails:**<br>
# * The issue `participant with multiple records` means that those participants have multiple rows in the wear_study table, which should not be possible. Investigate the issue. Start with the CR that creates the wear_study table. <br>
# * The issue `not in person table` means that participants exist in the wear_study table that aren't in the person table, which should not be possible. Investigate the issue. Start with the CR that creates the wear_study table.<br>

# +
query = JINJA_ENV.from_string("""

WITH latest_primary_consent_records AS ( -- most current consent record per person --
    SELECT person_id, observation_source_value, MAX(observation_date) AS latest_date,
    FROM `{{project_id}}.{{ct_dataset}}.observation` o
    WHERE REGEXP_CONTAINS(observation_source_value, '(?i)extraconsent_agreetoconsent')
    GROUP BY person_id, observation_source_value
)

SELECT
  'participant with multiple records' as issue,
  COUNT(person_id) as bad_rows
FROM `{{project_id}}.{{ct_dataset}}.wear_study` ws
GROUP BY person_id
HAVING COUNT(person_id)>1

UNION ALL

SELECT
  'not in person table' as issue,
  COUNT(person_id) as bad_rows
FROM `{{project_id}}.{{ct_dataset}}.wear_study` ws
WHERE person_id not in ( -- person table --
  SELECT person_id
  FROM `{{project_id}}.{{ct_dataset}}.person` o
  )
""")
q = query.render(project_id=project_id, ct_dataset=ct_dataset)
df1 = execute(client, q)

if df1['bad_rows'].sum() == 0:
    df = df.append(
        {
            'query': 'Query17  wear_study table is as expected.',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query':
                'Query17 wear_study table is not as expected. See notes in the description.',
            'result':
                'Failure'
        },
        ignore_index=True)
# -


df1

# +
# Query 18:  Check that wear_consent records are suppressed in the 'observation' and 'survey_conduct' tables
# -

query = JINJA_ENV.from_string("""
SELECT
  'observation' as table,
  COUNT(*) AS bad_rows
FROM
  `{{project_id}}.{{ct_dataset}}.observation` o
  LEFT JOIN   `{{project_id}}.{{ct_dataset}}.survey_conduct` sc
  ON sc.survey_conduct_id = o.questionnaire_response_id
WHERE sc.survey_concept_id IN (2100000011,2100000012) -- captures questions asked in multiple surveys --
OR LOWER(observation_source_value) IN UNNEST ({{wear_codes}}) -- captures those that might be missing from survey_conduct --
GROUP BY 1
UNION ALL
SELECT
  'survey_conduct' as table,
  COUNT(*) AS bad_rows
FROM
  `{{project_id}}.{{ct_dataset}}.survey_conduct` sc
WHERE sc.survey_concept_id IN (2100000011,2100000012)
GROUP BY 1
""")
q = query.render(project_id=project_id,
                 ct_dataset=ct_dataset,
                 wear_codes=WEAR_SURVEY_CODES)
df1 = execute(client, q)
if df1['bad_rows'].sum() == 0:
    df = df.append(
        {
            'query': 'Query18 wear_consent records are cleaned as expected.',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query':
                'Query18 wear_consent records have not been cleaned as expected.',
            'result':
                'Failure'
        },
        ignore_index=True)
df1

# +
# Query 19:  Check for the existence of at least one race/ethnicity sub-categories
# -

query = JINJA_ENV.from_string("""
SELECT COUNT(*), value_source_value FROM `{{project_id}}.{{ct_dataset}}.observation`
WHERE value_source_concept_id IN (1585605, 1585606, 1585607, 1585608, 1585609, 1585610, 1585611, 1585612,1585613, 1585614, -- Asian --
                        1585616, 1585617, 1585618, 1585619, 1585620, 1585621, 1585622, 1585623, 1585624, 1585625, 1585626, 1585627,  -- African --
                        1585345, 1585346, 1586086, 1586087, 1586088, 1586089, 1586090, 1586091, 1586092, 1586093, -- Spanish --
                        1585316, 1585633, 1585630, 1585631, 1585629, 1585319, 1585318, 1585317, 1585632, -- Middle Eastern --
                        1585321, 1585322, 1585323, 1585324, 1585325, 1585328, 1585329, 1585330, -- Pacific --
                        1585339, 1585332, 1585334, 1585337, 1585338, 1585340, 1585341, 1585342, -- European --
                        1586149) -- None of these fully Describe me --

GROUP BY value_source_value
ORDER BY value_source_value
""")
q = query.render(
    project_id=project_id,
    ct_dataset=ct_dataset,
)
result = execute(client, q)
if not result.empty:
    df = df.append(
        {
            'query':
                'Query19 existence of at least one race/ethnicity sub-categories',
            'result':
                'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query':
                'Query19 At least one race/ethnicity sub-categories DOES NOT exist',
            'result':
                'Failure'
        },
        ignore_index=True)
result

# Query 20: verify all race/ethnicity values are top-level, not subcategories
# -

query = JINJA_ENV.from_string("""
SELECT race_concept_id,
  race_source_concept_id,
  race_source_value,
  race_concept_id,
  ethnicity_concept_id,
  ethnicity_source_concept_id,
  ethnicity_source_value
FROM `{{project_id}}.{{ct_dataset}}.person`
WHERE race_source_concept_id NOT IN (1586141, 1586142, 1586143, 1586144, 1586145, 1586146, 903079, 0)
AND ethnicity_concept_id NOT IN (38003563, 38003564, 1586148, 903079, 903096, 0)
""")
q = query.render(project_id=project_id, ct_dataset=ct_dataset)
result = execute(client, q)
if result.empty:
    df = df.append(
        {
            'query': 'Query20 All race/ethnicity categories are top-level',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query20 All race/ethnicity categories are NOT top-level.',
            'result': 'Failure',
        },
        ignore_index=True)
result

# Query 20: verify free text responses for Race/ethnicity is suppressed
# -

query = JINJA_ENV.from_string("""
SELECT * FROM `{{project_id}}.{{ct_dataset}}.observation`
WHERE value_as_concept_id = 1586149
""")
q = query.render(project_id=project_id, ct_dataset=ct_dataset)
result = execute(client, q)
if result.empty:
    df = df.append(
        {
            'query': 'No text found for "NoneOfTheseDescribeMe" free text.',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': '"NoneOfTheseDescribeMe" response have free text.',
            'result': 'Failure'
        },
        ignore_index=True)
result

# -


# # Results Summary

# +
def highlight_cells(val):
    color = 'red' if 'Failure' in val else 'white'
    return f'background-color: {color}'


df.style.applymap(highlight_cells).set_properties(**{'text-align': 'left'})
