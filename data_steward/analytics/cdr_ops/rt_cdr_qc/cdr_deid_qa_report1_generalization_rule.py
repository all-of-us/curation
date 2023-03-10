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

# # RT_CDR_deid_QA_report_generalization_rule

import urllib
import pandas as pd
from common import JINJA_ENV
from utils import auth
from gcloud.bq import BigQueryClient
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES
pd.options.display.max_rows = 120

# + tags=["parameters"]
project_id = ""
com_cdr = ""
deid_cdr = ""
pipeline = ""
run_as = ""

# +
impersonation_creds = auth.get_impersonation_credentials(
    run_as, target_scopes=IMPERSONATION_SCOPES)

client = BigQueryClient(project_id, credentials=impersonation_creds)
# -

# df will have a summary in the end
df = pd.DataFrame(columns=['query', 'result'])

# # 1 GR_01 Race Generalization Rule
#
# Objective: Verify that the field identified to follow the race generalization rule AS de-identification action in OBSERVATION table displays the generalized group concept id in the table for de-id dataset.

# # 1.1 step1 Verify the following columns in the Observation table have been set to null:
#
# o   value_as_string
#
#
# o   value_source_value
#
# expected results:
#
# Null is the value poplulated in the value_as_string & value_source_value fields in the deid table.
#

query = JINJA_ENV.from_string("""
SELECT 

SUM(CASE WHEN value_as_string IS NOT NULL THEN 1 ELSE 0 END) AS n_value_as_string_not_null,
SUM(CASE WHEN value_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_value_source_value_not_null

FROM `{{project_id}}.{{deid_cdr}}.observation`

""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_cdr=deid_cdr)
df1 = execute(client, q)
if df1.loc[0].sum() == 0:
    df = df.append(
        {
            'query': 'Query1 GR01 Race_col_suppressoin in observation',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query1 GR01 Race_col_suppression in observation',
            'result': 'Failure'
        },
        ignore_index=True)
df1

# # 1.2 steps
#
#
# - Verify that if the value_source_concept_id field in OBSERVATION table populates: 2000000001,
# the value_as_concept_id field in deid table should populate : 2000000001
#
# - Verify that if the value_source_concept_id field in OBSERVATION table populates: 2000000008,
# the value_as_concept_id field in de-id table should populate : 2000000008
#
# - Verify that if the value_source_concept_id  field in OBSERVATION table populates: 1586142,
# the value_as_concept_id field in de-id table populates:   45879439
#
# - Verify that if the value_source_concept_id field in OBSERVATION table populates: 1586143 ,
# the value_as_concept_id field in de-id table populates:  1586143
#
# - Verify that if the value_source_concept_id field in OBSERVATION table populates: 1586146,
# the value_as_concept_id field in de-id table populates: 45877987
#
# - Verify that if the value_source_concept_id field in OBSERVATION table populates: 1586147,
# the value_as_concept_id field in de-id table populates:   1586147,
#
# - Verify that if the value_source_concept_id field in OBSERVATION table populates: 1586148 ,
# the value_as_concept_id field in de-id table populates:  45882607
#
# - Verify that if the value_source_concept_id in OBSERVATION table populates: 903079,
# the value_as_concept_id field in de-id table populates 1177221
#
# - Verify that if the value_source_concept_id field in OBSERVATION table populates: 903096 ,
# the value_as_concept_id field in de-id table populates: 903096
#

query = JINJA_ENV.from_string("""

WITH df1 AS (
SELECT distinct value_source_concept_id,value_as_concept_id
 FROM `{{project_id}}.{{deid_cdr}}.observation`
 WHERE value_source_concept_id in (2000000001,2000000008,1586142,1586143,1586146,1586147,1586148,903079,903096)
 )

SELECT COUNT (*) AS n_row_not_pass FROM df1
WHERE (value_source_concept_id=2000000001 AND value_as_concept_id !=2000000001)
 OR (value_source_concept_id=2000000008 AND value_as_concept_id !=2000000008)
 OR (value_source_concept_id=1586142 AND value_as_concept_id !=45879439)
 OR (value_source_concept_id=1586143 AND value_as_concept_id !=1586143)
 OR (value_source_concept_id=1586146 AND value_as_concept_id !=45877987)
 OR (value_source_concept_id=1586147 AND value_as_concept_id !=1586147)
 OR (value_source_concept_id=1586148 AND value_as_concept_id !=45882607)
 OR (value_source_concept_id=903079 AND value_as_concept_id !=1177221)
 OR (value_source_concept_id=903096 AND value_as_concept_id !=903096)
 
""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_cdr=deid_cdr)
df1 = execute(client, q)
if df1['n_row_not_pass'].sum() == 0:
    df = df.append(
        {
            'query':
                'Query1.2 GR01 Race_value_source_concept_id matched value_as_concept_id in observation',
            'result':
                'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query':
                'Query1.2 GR01 Race_value_source_concept_id matched value_as_concept_id in observation',
            'result':
                'Failure'
        },
        ignore_index=True)
df1

# # 2 GR_02 Sexual Orientation Generalization Rule
#
#
# Verify that the field identified for de-identification action in OBSERVATION table follow the Sexual Orientation Generalization Rule for the deid table.
#
# - Verify the following columns in the Observation table have been set to null:
# o   value_as_string
# o   value_source_value", which is already done by query 1,very first one
# - Verify that the value_source_concept_id field in the de-id table is populating the correct generalized value.
# - Verify that if the value_source_concept_id field in OBSERVATION table populates: 2000000003,
# the value_as_concept_id field in deid table should populate 2000000003."
# - Verify that if the value_source_concept_id field in OBSERVATION table populates: 1585900,
# the value_as_concept_id field in de-id table populates: 4069091

# # 2.2 steps
#
# - Verify the following columns in the deid_cdr Observation table have been set to null:
# o   value_as_string
# o   value_source_value
#
# - Verify that the value_source_concept_id field in the deid table is populating the correct generalized value.
#
# -Verify that if the value_source_concept_id field in the deid OBSERVATION table populates: 2000000003,
# the value_as_concept_id field in deid table should populate 2000000003.
#
#
# - Verify that if the value_source_concept_id field in deid_cdr OBSERVATION table populates: 1585900,
# the value_as_concept_id field in de-id table populates: 4069091   "
#
# 'SexualOrientation_GeneralizedNotStraight'

# fine in both deid and deid_base
query = JINJA_ENV.from_string("""

WITH df1 AS (
SELECT distinct value_source_concept_id,value_as_concept_id
 FROM `{{project_id}}.{{deid_cdr}}.observation`
 WHERE value_source_concept_id in (2000000003,1585900)
 )
 
SELECT COUNT (*) AS n_row_not_pass FROM df1
WHERE (value_source_concept_id=2000000003 AND value_as_concept_id !=2000000003)
OR (value_source_concept_id=1585900 AND value_as_concept_id !=4069091)

""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_cdr=deid_cdr)
df1 = execute(client, q)
if df1['n_row_not_pass'].sum() == 0:
    df = df.append(
        {
            'query':
                'Query2.2 GR02 TheBasics_SexualOrientation matched value_source_concept_id in observation',
            'result':
                'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query':
                'Query2.2 GR02 TheBasics_SexualOrientation matched value_source_concept_id in observation',
            'result':
                'Failure'
        },
        ignore_index=True)
df1

# # 2.3 	Account for multiple SELECTions for sexual orientation (DC-859)
#
#
# Verify that if a person hAS multiple SELECTion for TheBasics_SexualOrientation in pre-deid_com_cdr dataset, then the the value_source_concept_id field in OBSERVATION table populates: 2000000003, for those person_id in deid dataset
#
# Account for multiple SELECTions for sexual orientation (DC-859)
#
# 1. Find person_ids that have more than 1 sexual_orientation SELECTions in the non-deid datasets (494038847, 326626269,353533275, 697092658,887791634,895181663)
#
#
# 2 Find the person_id of those in the map tables (1872508, 1303111, 2051219, 1488177, 1278442, 1159723)
#
# 3 AND then look up for those person_ids in the deid dataset to verify that the  value_as_concept_id field is generalized for those person_ids

# fine in both deid and deid_base
query = JINJA_ENV.from_string("""

WITH df1 AS (
SELECT m.research_id AS person_id,count (distinct ob.value_source_value) AS countp
FROM `{{project_id}}.{{com_cdr}}.observation` ob
JOIN `{{project_id}}.{{pipeline}}.primary_pid_rid_mappin` m
ON ob.person_id = m.person_id
WHERE REGEXP_CONTAINS(ob.observation_source_value, 'TheBasics_SexualOrientation')
GROUP BY 1),

df2 AS (
SELECT person_id 
FROM `{{project_id}}.{{deid_cdr}}.observation` 
WHERE value_as_concept_id =2000000003
)

SELECT COUNT (distinct person_id) AS n_PERSON_ID_not_pass FROM df1
WHERE countp >1
AND person_id NOT IN (SELECT distinct person_id FROM df2 )

""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_cdr=deid_cdr)
df1 = execute(client, q)
if df1.eq(0).any().any():
    df = df.append(
        {
            'query':
                'Query2.3 GR02 multiple_SexualOrientation matched value_source_concept_id in observation',
            'result':
                'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query':
                'Query2.3 GR02 multiple_SexualOrientation matched value_source_concept_id in observation',
            'result':
                'Failure'
        },
        ignore_index=True)
df1

# # 3 GR_03 Gender Generalization Rule
#
# Verify that the field identified for de-identification action in OBSERVATION table follow the Gender Generalization Rule  for the de-id table.
#
# steps:
#
# 1 Verify the following columns in the Observation table have been set to null:
# o   value_as_string
# o   value_source_value
#
# result:  All value_source_concept_id that have been marked for gender generalization rule are generalized in the " value_source_concept_id" column in the OBSERVATION table.
#
#
# 2 - Verify that the value_source_concept_id field in the de-id table is populating the correct generalized value
#
# 3 - Verify that if the value_source_concept_id field in OBSERVATION table populates: 2000000002 ,
# the value_as_concept_id field in de-id table should populate 2000000002 .
#
# results:
#
# 2000000002 is the value that is populated in value_as_concept_id field in the deid table.
#
#
#
# 4 - Verify that if the value_source_concept_id field in OBSERVATION table populates: 1585840,
# the value_as_concept_id field in de-id table populates 45878463
#
# results:
# 45878463 is the value that is populated in  value_as_concept_id field in the deid table.
#
# 5 - Verify that if the value_source_concept_id field in OBSERVATION table populates 1585839,
# the value_as_concept_id field in de-id table populates 45880669
#
# results:
# 45880669 is the value that is populated in value_as_concept_id field in the deid table.
#

# fine in both deid and deid_base
query = JINJA_ENV.from_string("""

WITH df1 AS (
SELECT distinct value_source_concept_id,value_as_concept_id
 FROM `{{project_id}}.{{deid_cdr}}.observation`
 WHERE value_source_concept_id IN (2000000002,1585840,1585839)
 )

SELECT COUNT (*) AS n_row_not_pass FROM df1
WHERE (value_source_concept_id=2000000002 AND value_as_concept_id !=2000000002)
OR (value_source_concept_id=1585840 AND value_as_concept_id !=45878463)
OR (value_source_concept_id=1585839 AND value_as_concept_id !=45880669)

""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_cdr=deid_cdr)
df1 = execute(client, q)
if df1['n_row_not_pass'].sum() == 0:
    df = df.append(
        {
            'query':
                'Query3 GR03 Gender_value_source_concept_id matched value_as_concept_id in observation',
            'result':
                'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query':
                'Query3 GR03 Gender_value_source_concept_id matched value_as_concept_id in observation',
            'result':
                'Failure'
        },
        ignore_index=True)
df1

# # 3 Biological Sex Generalization Rule
#
# Verify that the field identified for de-identification action in OBSERVATION table follow the Biological Sex Generalization Rule for the de-id table.
#
# "Verify the following columns in the Observation table have been set to null:
# o   value_as_string
# o   value_source_value"
#
# which has been done in query 1
#
# - Verify that the value_source_concept_id field in de-id table is populating the correct generalized value
#
# - Verify that if the value_source_concept_id field in OBSERVATION table populates: 2000000009,
# the value_as_concept_id field in de-id table should populate 2000000009
#
#
# - Verify that if the value_source_concept_id field in OBSERVATION table populates: 1585847,
# the value_as_concept_id field in de-id table populates 45878463
#
# - Verify that if the value_source_concept_id field in OBSERVATION table populates:  1585846,
# the value_as_concept_id field in de-id table populates : 45880669
#

query = JINJA_ENV.from_string("""

WITH df1 AS (
SELECT distinct value_source_concept_id,value_as_concept_id
FROM `{{project_id}}.{{deid_cdr}}.observation`
WHERE value_source_concept_id IN (2000000009,1585847,1585846)
 )

SELECT COUNT (*) AS n_row_not_pass FROM df1
WHERE (value_source_concept_id=2000000009 AND value_as_concept_id !=2000000009)
OR (value_source_concept_id=1585847 AND value_as_concept_id !=45878463)
OR (value_source_concept_id=1585846 AND value_as_concept_id !=45880669)

""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_cdr=deid_cdr)
df1 = execute(client, q)
if df1.eq(0).any().any():
    df = df.append(
        {
            'query': 'Query3 Biological Sex Generalization Rule in observation',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query3 Biological Sex Generalization Rule in observation',
            'result': 'Failure'
        },
        ignore_index=True)
df1

# # 3.3 GR_03_1 Sex/gender mismatch
#
# Verify that if a person responds
# 1. Male sex at birth AND female gender
#
# 2.Female sex at birth AND male gender
#
# In these instances, gender will need to be generalized AND sex at birth retained in deid dataset
#
# steps
#
# 1. look up for the pids with sexatBirth = female/male AND gender_male or female in pre_deid_com_cdr
#
#
# 2. lookup for the mismatch PIDs in deid_base_cdr.PERSON table, whether these people keep sex_birth AND gender wAS generalized to gender_source_concept_id = 2000000002
#
# 3. Lookup for the same pids in observation table AND Verify that if the value_source_concept_id field in OBSERVATION table populates: 2000000002  AND the value_as_concept_id field in deid_cdr table should populate  2000000002

# +
# in deid_cdr observation table

query = JINJA_ENV.from_string("""

WITH df1 AS (
SELECT m.research_id AS person_id
FROM  `{{project_id}}.{{com_cdr}}.observation` ob
JOIN  `{{project_id}}.{{pipeline}}.primary_pid_rid_mappin` m 
ON ob.person_id=m.person_id
WHERE value_source_value IN ('SexAtBirth_Female' ,'GenderIdentity_Man')
GROUP BY m.research_id
HAVING count (distinct value_source_value)=2 
  
  )
  
SELECT COUNT (*) AS n_row_not_pass FROM `{{project_id}}.{{deid_cdr}}.observation` 
WHERE person_id IN (SELECT person_id FROM df1)
AND observation_source_value LIKE 'Gender_GenderIdentity'
AND (value_as_concept_id !=2000000002 AND value_source_concept_id !=2000000002)

 """)
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_cdr=deid_cdr)
df1 = execute(client, q)
if df1['n_row_not_pass'].sum() == 0:
    df = df.append(
        {
            'query':
                'Query3.3.2 GR_03_1 Sex_female/gender_man mismatch in deid_cdr.observation',
            'result':
                'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query':
                'Query3.3.2 GR_03_1 Sex_female/gender_man mismatch in deid_cdr.observation',
            'result':
                'Failure'
        },
        ignore_index=True)
df1

# +
# in deid_cdr obs

query = JINJA_ENV.from_string("""

WITH df1 AS (

SELECT m.research_id AS person_id
FROM `{{project_id}}.{{com_cdr}}.observation` ob
JOIN  `{{project_id}}.{{pipeline}}.primary_pid_rid_mappin` m 
ON ob.person_id=m.person_id
WHERE value_source_value IN ('SexAtBirth_Male' ,'GenderIdentity_Woman')
GROUP BY m.research_id
HAVING count (distinct value_source_value)=2 
)
  
SELECT COUNT (*) AS n_row_not_pass FROM  `{{project_id}}.{{deid_cdr}}.observation`
WHERE person_id IN (SELECT person_id FROM df1)
AND observation_source_value LIKE 'Gender_GenderIdentity'
AND (value_as_concept_id !=2000000002 AND value_source_concept_id !=2000000002)
""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_cdr=deid_cdr)
df1 = execute(client, q)
if df1['n_row_not_pass'].sum() == 0:
    df = df.append(
        {
            'query':
                'Query3.3.4 GR_03_1 Sex_male/gender_woman mismatch in deid_cdr.observation',
            'result':
                'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query':
                'Query3.3.4 GR_03_1 Sex_male/gender_woman mismatch in deid_cdr.observation',
            'result':
                'Failure'
        },
        ignore_index=True)
df1
# -

# # 4 Education Status Generalization Rule
#
# Verify that the field identified for de-identification action in OBSERVATION table follow the Education Status Generalization Rule for the de-id table.
#
# - Verify the following columns in the Observation table have been set to null:
# o   value_as_string
# o   value_source_value
# which hAS been done in query1
#
# - Verify that the value_source_concept_id field in de-id table is populating the correct generalized value.
# - Verify that  if the value_source_concept_id in OBSERVATION table populates: 2000000007,
# the value_as_concept_id field in de-id table should populate: 2000000007."
# - Verify that  if the value_source_concept_id in OBSERVATION table populates: 2000000006, the value_as_concept_id field in the de-id table should populate: 2000000006.
# - Verify that  if the value_source_concept_id in OBSERVATION table populates: 1585945,
# the value_as_concept_id field in the de-id table should populate: 43021808"
# - Verify that  if the value_source_concept_id in OBSERVATION table populates: 1585946,
# the value_as_concept_id field in the de-id table should populate: 4260980"
# - Verify that if the value_source_concept_id in OBSERVATION table populates: 903079,
# the value_as_concept_id field in de-id table populates 1177221
#
# - Verify that if the value_source_concept_id in OBSERVATION table populates: 903096,
# the value_as_concept_id field in de-id table populates 903096

query = JINJA_ENV.from_string("""

WITH df1 AS (
SELECT distinct value_source_concept_id,value_as_concept_id
FROM `{{project_id}}.{{deid_cdr}}.observation`
WHERE value_source_concept_id IN (2000000007, 2000000006,1585945,1585946,903079,903096)
 )
  
SELECT COUNT (*) AS n_row_not_pass FROM df1
WHERE (value_source_concept_id=2000000007 AND value_as_concept_id !=2000000007)
OR (value_source_concept_id=2000000006 AND value_as_concept_id !=2000000006)
OR (value_source_concept_id=1585945 AND value_as_concept_id !=43021808)
OR (value_source_concept_id=1585946 AND value_as_concept_id !=4260980)
OR (value_source_concept_id=903096 AND value_as_concept_id !=903096)
OR (value_source_concept_id=903079 AND value_as_concept_id !=1177221)

""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_cdr=deid_cdr)
df1 = execute(client, q)
if df1['n_row_not_pass'].sum() == 0:
    df = df.append(
        {
            'query': 'Query4 Education Generalization Rule in observation',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query4 Education Sex Generalization Rule in observation',
            'result': 'Failure'
        },
        ignore_index=True)
df1

# # 5 Employment Generalization Rule
#
# Verify that the field identified for de-identification action in OBSERVATION table follow the Employment Generalization Rule for the de-id table.
#
# - Verify the following columns in the Observation table have been set to null:
# o   value_as_string
# o   value_source_value"
#
# which hAS been done in query1
# - Verify that the value_source_concept_id field in de-id table is populating the correct generalized value.
#
# - Verify that if the value_source_concept_id in OBSERVATION table populates: 2000000005,
# the value_as_concept_id field in de-id table should populate 2000000005."
# - Verify that if the value_source_concept_id in OBSERVATION table populates: 2000000004 ,
# the value_as_concept_id field in de-id table populates 2000000004 "
# - Verify that if the value_source_concept_id in OBSERVATION table populates: 903079,
# the value_as_concept_id field in de-id table populates 1177221
# - Verify that if the value_source_concept_id in OBSERVATION table populates: 903096,
# the value_as_concept_id field in de-id table populates 903096
#

query = JINJA_ENV.from_string("""

WITH df1 AS (
SELECT distinct value_source_concept_id,value_as_concept_id
FROM `{{project_id}}.{{deid_cdr}}.observation`
WHERE value_source_concept_id IN (2000000005, 2000000004,903079,903096)
 )
 
SELECT COUNT (*) AS n_row_not_pass FROM df1
WHERE (value_source_concept_id=2000000005 AND value_as_concept_id !=2000000005)
OR (value_source_concept_id=2000000004 AND value_as_concept_id !=2000000004)
OR (value_source_concept_id=903079 AND value_as_concept_id !=1177221)
OR (value_source_concept_id=903096 AND value_as_concept_id !=903096)


""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_cdr=deid_cdr)
df1 = execute(client, q)
if df1['n_row_not_pass'].sum() == 0:
    df = df.append(
        {
            'query': 'Query5 Employment Generalization Rule in observation',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query':
                'Query5 GR_06 Employment Generalization Rule in observation',
            'result':
                'Failure'
        },
        ignore_index=True)
df1

# # 6 Gender_concept_id should be 0, instead of null in person table
#
# [DC-1785]
#
# The person.gender_concept_id column, should be de-identified at the end of the Registered Tier run.  It will be repopulated with properly de-identified data from the observation table in the next data stage.  This field is a required field, so it cannot be set to null.  All values in this field should be set to 0.  We need to update the existing notebooks to ensure we are suppressing this data at the RT deid data stage.
#
# If given a dataset where any person.gender_concept_id field is not 0 will produce a failure.

# +
query = JINJA_ENV.from_string("""

SELECT
SUM(CASE WHEN gender_concept_id !=0 THEN 1 ELSE 0 END) AS n_gender_concept_id_not_zero,
SUM(CASE WHEN gender_concept_id IS NULL THEN 1 ELSE 0 END) AS n_gender_concept_id_is_null
FROM `{{project_id}}.{{deid_cdr}}.person`
""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_cdr=deid_cdr)
df1 = execute(client, q)

if df1.loc[0].sum() == 0:
    df = df.append(
        {
            'query': 'Query6 Gender_concept_id should be 0 in person table',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query':
                'Query6 GR_06 Gender_concept_id should be 0 in person table',
            'result':
                'Failure'
        },
        ignore_index=True)
df1

# -

# # Summary_cdr_deid_Generalization_rule


# +
def highlight_cells(val):
    color = 'red' if 'Failure' in val else 'white'
    return f'background-color: {color}'


df.style.applymap(highlight_cells).set_properties(**{'text-align': 'left'})
