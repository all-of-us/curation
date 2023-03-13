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

# These are queries to validate RT_deid_base_cdr </div>

import pandas as pd
from common import JINJA_ENV
from utils import auth
from gcloud.bq import BigQueryClient
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES
pd.options.display.max_rows = 120

# + tags=["parameters"]
project_id = ""
com_cdr = ""
deid_base_cdr = ""
pipeline = ""
run_as = ""

# +
impersonation_creds = auth.get_impersonation_credentials(
    run_as, target_scopes=IMPERSONATION_SCOPES)

client = BigQueryClient(project_id, credentials=impersonation_creds)
# -

# df will have a summary in the end
df = pd.DataFrame(columns=['query', 'result'])

# # 1 Verify that if a person has multiple SELECTion(Hispanic + other race) in pre_deid_com_cdr, the output in deid_base_cdr observation table should result in two rows - one for Ethnicity AND one for race.
#
# test steps:
#
# - Verify the following columns in the de-id Observation table have been set to null:
# o   value_as_string
# o   value_source_value"
#  however, this is already done in query 1, no need here anymore
# - Find person_ids in pre_deid_com_cdr person table who have ethnicity_source_concept_id values AS 1586147  & race_source_concept_id AS ( 1586146 OR 1586142 OR 1586143) , then verify that the output in the deid_base_cdr observation table for that person_id  will results in 2-rows .
# - Verify that the 2-rows have 2-different value_source_concept_id values in the deid_base_cdr Observation table.
# - Verify that if a person_id in pre_deid_com_cdr hAS ethnicity_source_concept_id values AS 1586147  & race_source_concept_id AS ( 1586145 OR 1586144)  in the person table, the output in the deid_base_cdr observation table for that person_id  will result in 2-rows .
# - Verify that if a person_id hAS ethnicity_source_concept_id values AS 1586147  & race_source_concept_id AS multiple SELECTions (2 or more) in the person table, the output in the deid_base_cdr observation table for that person_id  will result in 2 OR MORE rows .
#
#

# ## step1
# - Verify the following columns in the deid_cdr Observation table have been set to null:
# o   value_as_string
# o   value_source_value
#
# has been done in first sql for deid, can be skipped here

# ## Query 1.2 Find person_ids in pre_dedi_com_cdr person table who have ethnicity_source_concept_id values AS 1586147  & race_source_concept_id AS ( 1586146 OR 1586142 OR 1586143) , then verify that the output in the deid_base_cdr observation table for that person_id after mapping  will results in 2-rows .
#
#   step 3
# Verify that the 2-rows have 2-different value_source_concept_id values in the deid_base_cdr Observation table.

query = JINJA_ENV.from_string("""

WITH df1 AS (
SELECT m.research_id AS person_id
FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m 
JOIN `{{project_id}}.{{com_cdr}}.person` com 
ON m.person_id = com.person_id 
WHERE com.ethnicity_source_concept_id = 1586147
AND com.race_source_concept_id in (1586142, 1586143, 1586146 )
 ),
 
df2 AS (
SELECT DISTINCT person_id , COUNT (distinct value_source_concept_id ) AS countp
FROM `{{project_id}}.{{deid_base_cdr}}.observation`
WHERE  observation_source_value = 'Race_WhatRaceEthnicity' 
GROUP BY person_id
 )
 
SELECT COUNT (*) AS n_not_two_rows FROM df2
WHERE person_id IN (SELECT person_id FROM df1) AND countp !=2 
""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_base_cdr=deid_base_cdr)
df1 = execute(client, q)
if df1.eq(0).any().any():
    df = df.append(
        {
            'query': 'Query 1.2 these person_ids have 2-rows in observation',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query1.2 these person_ids have 2-rows in observation',
            'result': 'Failure'
        },
        ignore_index=True)
df1

# ## one error in new cdr. this person_id fails to meet the rule.

# +
query = JINJA_ENV.from_string("""

WITH df1 AS (
SELECT m.research_id AS person_id
FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m 
JOIN `{{project_id}}.{{com_cdr}}.person` com 
ON m.person_id = com.person_id 
WHERE com.ethnicity_source_concept_id = 1586147
AND com.race_source_concept_id in (1586142, 1586143, 1586146 )
 ),
 
df2 AS (
SELECT DISTINCT person_id , count (distinct value_source_concept_id ) AS countp
FROM `{{project_id}}.{{deid_base_cdr}}.observation`
WHERE  observation_source_value = 'Race_WhatRaceEthnicity' 
GROUP BY person_id
 )
 
SELECT distinct person_id, value_source_concept_id, value_source_value
FROM `{{project_id}}.{{deid_base_cdr}}.observation`
WHERE  observation_source_value = 'Race_WhatRaceEthnicity' 
AND person_id IN (SELECT person_id from df2 where countp !=2 )
AND person_id IN (SELECT person_id FROM df1) 
""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_base_cdr=deid_base_cdr)
df1 = execute(client, q)

df1
# -

# ## Query 1.3 step : Verify that if a person_id has ethnicity_source_concept_id values AS 1586147  & race_source_concept_id AS ( 1586145 OR 1586144)  in the pre_deid_com_cdr person table, the output in the deid_cdr observation table for that person_id  after mapping will result in 2-rows AND the 2000000001 race value is populated in value_source_concept_id field in the other row of observation table in the deid dataset.

# +

query = JINJA_ENV.from_string("""

WITH df1 AS (
SELECT distinct m.research_id AS person_id
FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m 
join `{{project_id}}.{{com_cdr}}.person` com 
ON m.person_id = com.person_id 
WHERE com.ethnicity_source_concept_id = 1586147
AND com.race_source_concept_id IN (1586145, 1586144) 
 ),
 
df2 AS (
SELECT  person_id , count (distinct value_source_concept_id) AS countp
FROM `{{project_id}}.{{deid_base_cdr}}.observation`
WHERE  observation_source_value = 'Race_WhatRaceEthnicity' 
AND value_source_concept_id IN (2000000001 ,1586147)
GROUP BY person_id
)
 
SELECT COUNT (*) AS n_row_not_pass FROM df1
WHERE person_id NOT IN (SELECT person_id FROM df2 WHERE countp=2)
""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_base_cdr=deid_base_cdr)
df1 = execute(client, q)
if df1.eq(0).any().any():
    df = df.append(
        {
            'query': 'Query1.3 these person_ids have 2-rows in observation',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query1.3 these person_ids have 2-rows in observation',
            'result': 'Failure'
        },
        ignore_index=True)
df1
# -

# ## Query 1.4 Verify that if a person_id has ethnicity_source_concept_id values AS 1586147  & race_source_concept_id AS multiple SELECTions (2 or more) in the pre_deid_com_cdr person table, the output in the deid_base_cdr observation table for that person_id  will result in 2 OR MORE rows .
#
# observation_source_value = 'Race_WhatRaceEthnicity' AND value_source_concept_id

query = JINJA_ENV.from_string("""

WITH df1 AS (
SELECT distinct person_id
FROM `{{project_id}}.{{deid_base_cdr}}.person` 
WHERE ethnicity_source_concept_id = 1586147
AND race_source_concept_id=2000000008
 ),
 
df2 AS (
SELECT DISTINCT person_id , count (distinct value_source_concept_id ) AS countp
FROM `{{project_id}}.{{deid_base_cdr}}.observation`
WHERE  observation_source_value = 'Race_WhatRaceEthnicity' 
GROUP BY person_id
 )
 
SELECT COUNT (*) AS n_row_not_pass FROM df2
WHERE person_id IN (SELECT person_id FROM df1) AND countp <2 
""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_base_cdr=deid_base_cdr)
df1 = execute(client, q)
if df1.eq(0).any().any():
    df = df.append(
        {
            'query':
                'Query1.4 these person_ids have 2 or more rows in observation',
            'result':
                'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query':
                'Query1.4 these person_ids have 2 or more rows in observation',
            'result':
                'Failure'
        },
        ignore_index=True)
df1

# ## Query 1.5  GR_01_2	"Race Ethnicity:  Race non-responses DC-618
#
# Verify that if race_name / race_source_value field in deid_base_cdr PERSON table populates AS "AoUDRC_NoneIndicated", the race_concept_id field in deid_base_cdr person table should populate : 2100000001
#
# this test case can be verified only after the PERSON table is repopulated.

# has to be deid_base
query = JINJA_ENV.from_string("""

WITH df1 AS (
SELECT distinct race_source_value,race_source_concept_id ,race_concept_id
FROM `{{project_id}}.{{deid_base_cdr}}.person`
WHERE race_concept_id = 2100000001 )

SELECT COUNT (*) AS n_row_not_pass FROM df1
WHERE  race_source_concept_id !=0 AND race_source_value !='AoUDRC_NoneIndicated'
""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_base_cdr=deid_base_cdr)
df1 = execute(client, q)
if df1.eq(0).any().any():
    df = df.append(
        {
            'query':
                'Query1.5 Race_source_concept_id suppresion in observation',
            'result':
                'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query':
                'Query1.5 Race_source_concept_id suppresion in observation',
            'result':
                'Failure'
        },
        ignore_index=True)
df1

# # Query 2 Gender Generalization Rule
#
# objective: Account for new gender identify response option (DC-654)
# The new concept ID in the gender question, “CloserGenderDescription_TwoSpirit” (value_source_concept_id=701374) needs to be generalized to value_source_concept_id = 2000000002 (GenderIdentity_GeneralizedDiffGender)

# has to be deid_base
query = JINJA_ENV.from_string("""


SELECT COUNT (distinct p.person_id) AS n_PERSON_ID_not_pass
FROM  `{{project_id}}.{{com_cdr}}.observation` com
JOIN  `{{project_id}}.{{pipeline}}.pid_rid_mapping` m 
ON com.person_id=m.person_id
JOIN  `{{project_id}}.{{deid_base_cdr}}.observation` p
ON p.person_id=m.research_id
WHERE com.value_source_concept_id = 701374
AND p.observation_source_value='Gender_GenderIdentity'
AND p.value_source_concept_id !=2000000002
""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_base_cdr=deid_base_cdr)
df1 = execute(client, q)
if df1.eq(0).any().any():
    df = df.append(
        {
            'query':
                'Query2 Gender_value_source_concept_id matched value_as_concept_id in observation',
            'result':
                'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query':
                'Query2 Gender_value_source_concept_id matched value_as_concept_id in observation',
            'result':
                'Failure'
        },
        ignore_index=True)
df1

# # Query 3 Sex/gender mismatch
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

# check in deid_base_cdr.person
query = JINJA_ENV.from_string("""

WITH df1 AS (
SELECT m.research_id AS person_id
FROM  `{{project_id}}.{{com_cdr}}.observation` ob
JOIN  `{{project_id}}.{{pipeline}}.pid_rid_mapping` m 
on ob.person_id=m.person_id
WHERE value_source_value IN ('SexAtBirth_Female' ,'GenderIdentity_Man')
GROUP BY m.research_id
HAVING count (distinct value_source_value)=2 
)

  
SELECT COUNT (*) AS n_row_not_pass FROM `{{project_id}}.{{deid_base_cdr}}.person` 
JOIN `{{project_id}}.{{deid_base_cdr}}.person_ext` using (person_id)
WHERE person_id IN (SELECT person_id FROM df1)
AND (sex_at_birth_source_value !='SexAtBirth_Female' AND gender_source_concept_id !=2000000002)
""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_base_cdr=deid_base_cdr)
df1 = execute(client, q)
if df1.eq(0).any().any():
    df = df.append(
        {
            'query':
                'Query3 Sex_female/gender_man mismatch in deid_base_cdr.person table',
            'result':
                'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query':
                'Query3 Sex_female/gender_man mismatch in deid_base_cdr.person table',
            'result':
                'Failure'
        },
        ignore_index=True)
df1

# +
# check mismatched sex_male/gender_woman in deid_base_cdr.person

query = JINJA_ENV.from_string("""

WITH df1 AS (

SELECT m.research_id AS person_id
FROM  `{{project_id}}.{{com_cdr}}.observation` ob
JOIN  `{{project_id}}.{{pipeline}}.pid_rid_mapping` m 
ON ob.person_id=m.person_id
WHERE value_source_value IN ('SexAtBirth_Male' ,'GenderIdentity_Woman')
GROUP BY m.research_id
HAVING count (distinct value_source_value)=2 
  )
  
SELECT COUNT (*) AS n_row_not_pass FROM `{{project_id}}.{{deid_base_cdr}}.person` 
JOIN `{{project_id}}.{{deid_base_cdr}}.person_ext` using (person_id)
WHERE person_id IN (SELECT person_id FROM df1) 
AND (sex_at_birth_source_value !='SexAtBirth_Male' AND gender_source_concept_id !=2000000002) 
""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_base_cdr=deid_base_cdr)
df1 = execute(client, q)
if df1.eq(0).any().any():
    df = df.append(
        {
            'query':
                'Query3.2 Sex_male/gender_woman mismatch in deid_base_cdr.person',
            'result':
                'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query':
                'Query3.2 Sex_male/gender_woman mismatch in deid_base_cdr.person',
            'result':
                'Failure'
        },
        ignore_index=True)
df1
# -

#
# # Query 4  [DC-938] Verify that the COPE survey date is not date-shifted
#
# related deid_report7_cope notebook
#
# observation_concept_id = 1333342, which is just the concept_id for COPE, nothing to do with detailed topics/questions
# This test case will run with the De-id base cleaning rules
#
# expected result: no dateshift is applied
#
# result: pass if not shifted.
#
# ## in new cdr, need to fix survey_version_concept_id first.

# has to be deid_base
query = JINJA_ENV.from_string("""

WITH cope_survey_id AS (
SELECT
DISTINCT e.survey_version_concept_id, c.concept_name
FROM `{{project_id}}.{{deid_base_cdr}}.observation_ext` e
JOIN `{{project_id}}.{{deid_base_cdr}}.concept` c
ON c.concept_id = e.survey_version_concept_id),

df1 as (
SELECT
d.observation_date AS date_D,
i.observation_date AS date_i,
DATE_DIFF(DATE(i.observation_date), DATE(d.observation_date),day) AS diff,
 e.survey_version_concept_id
FROM `{{project_id}}.{{com_cdr}}.observation` i
JOIN `{{project_id}}.{{deid_base_cdr}}.observation` d
ON i.observation_id = d.observation_id
JOIN `{{project_id}}.{{deid_base_cdr}}.observation_ext` e
ON e.observation_id = d.observation_id
WHERE e.survey_version_concept_id IN (SELECT survey_version_concept_id from cope_survey_id)
)

SELECT COUNT (*) AS n_row_not_pass FROM df1
WHERE diff !=0
""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_base_cdr=deid_base_cdr)
df1 = execute(client, q)
if df1.loc[0].sum() == 0:
    df = df.append({
        'query': 'Query4 date not shifited',
        'result': 'PASS'
    },
                   ignore_index=True)
else:
    df = df.append({
        'query': 'Query4 date not shifited',
        'result': 'Failure'
    },
                   ignore_index=True)
df1

# ##  Query 4.2 [DC-1051] Verify that "PPI Drop Duplicates" Rule is excluded COPE responses
#
# steps:
#
# 1. look up for person_id that have completed all the versions of COPE survey by joining observation and _ext table. (query1: col I)
# 2. In Observation table of the person_id, look up for the COPE survey questions.  (query2: col J)
#  (Observation_source value  =
# 'overallhealth_14b'
# 'basics_12'
# 'basics_11a'
# 'cu_covid'
# 'copect_58'
#
# 3. validate that the duplicate responses of those survey questions are retained for all the survey version
#
#
# results: Found duplicate rows for survey QR.
#
# <font color='red'>
#
# this part has to be done in deid_base_cdr, can not use deid, which has no results.
#
# in new cdr, need to fix survey_version_concept_id first.

# has to be deid_base
query = JINJA_ENV.from_string("""

WITH df1 AS (
SELECT d.person_id, COUNT(*) AS countp
FROM `{{project_id}}.{{deid_base_cdr}}.observation` d
JOIN  `{{project_id}}.{{deid_base_cdr}}.observation_ext` e
ON e.observation_id = d.observation_id
WHERE e.survey_version_concept_id IN (2100000004, 2100000003, 2100000002)
AND d.observation_source_value IN ('overallhealth_14b' , 'basics_12' ,'basics_11a' ,'cu_covid', 'copect_58')
GROUP BY person_id
)

SELECT COUNT (*) AS n_row_not_pass FROM df1
WHERE countp=0
""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_base_cdr=deid_base_cdr)
df1 = execute(client, q)
if df1.loc[0].sum() == 0:
    df = df.append(
        {
            'query': 'Query4.2 PPI Drop Duplicates rule exclusion',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query4.2 PPI Drop Duplicates rule exclusion',
            'result': 'Failure'
        },
        ignore_index=True)
df1

# # Qury 5 equal counts for sex_at_birth columns
#
# to ensure that there are equal counts FROM both the observation and person_ext tables for the sex_at_birth_* columns that can be added to the RT validation notebook:
#
# **extra**
#
# https://precisionmedicineinitiative.atlassian.net/browse/DC-1404
#
# **RT CDR Base generation**
# https://precisionmedicineinitiative.atlassian.net/browse/DC-1402

query = JINJA_ENV.from_string("""

WITH df1 AS (
SELECT 
sex_at_birth_source_value AS sex_at_birth_value, count(*) AS countp1
FROM `{{project_id}}.{{deid_base_cdr}}.person_ext`
JOIN `{{project_id}}.{{deid_base_cdr}}.person` USING (person_id)
WHERE sex_at_birth_concept_id !=0
GROUP BY sex_at_birth_source_value
-- order by sex_at_birth_source_value --
),

df2 AS (
SELECT  value_source_value AS sex_at_birth_value, count(*) AS countp2
FROM `{{project_id}}.{{deid_base_cdr}}.observation`
WHERE observation_source_concept_id = 1585845
GROUP BY value_source_value
-- order by value_source_value --
)

SELECT COUNT (*) AS n_row_not_pass FROM df1
FULL JOIN df2 USING (sex_at_birth_value)
WHERE countp1 !=countp2
""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_base_cdr=deid_base_cdr)
df1 = execute(client, q)
if df1.loc[0].sum() == 0:
    df = df.append(
        {
            'query': 'Query5 equal counts for sex_at_birth columns',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query5 equal counts for sex_at_birth columns',
            'result': 'Failure'
        },
        ignore_index=True)
df1

# # Summary_deid_base_validation


# +
def highlight_cells(val):
    color = 'red' if 'Failure' in val else 'white'
    return f'background-color: {color}'


df.style.applymap(highlight_cells).set_properties(**{'text-align': 'left'})
# -
