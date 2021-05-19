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

# # RT_CDR_QA_report_generalization_rule

import urllib
import pandas as pd
pd.options.display.max_rows = 120

# + tags=["parameters"]
project_id = ""
com_cdr = ""
deid_cdr=""
deid_base_cdr=""
pipeline=""
# -

# df will have a summary in the end
df = pd.DataFrame(columns = ['query', 'result']) 

# # 1 GR_01 Race Generalization Rule
#
# Objective:
#     
# Verify that the field identified to follow the race generalization rule AS de-identification action in OBSERVATION table displays the generalized group concept id in the table for de-id dataset.

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

query = f'''
SELECT 

SUM(CASE WHEN value_as_string IS NOT NULL THEN 1 ELSE 0 END) AS n_value_as_string_not_null,
SUM(CASE WHEN value_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_value_source_value_not_null

FROM `{project_id}.{deid_cdr}.observation`

'''
df1=pd.read_gbq(query, dialect='standard')  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query1 GR01 Race_col_suppressoin in observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query1 GR01 Race_col_suppression in observation', 'result' : ''},  
                ignore_index = True) 
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

query = f'''

WITH df1 AS (
SELECT distinct value_source_concept_id,value_as_concept_id
 FROM `{project_id}.{deid_cdr}.observation`
 WHERE value_source_concept_id in (2000000001,2000000008,1586142,1586143,1586146,1586147,1586148,903079,903096)
 )

SELECT count (*) FROM df1
WHERE (value_source_concept_id=2000000001 AND value_as_concept_id !=2000000001)
 OR (value_source_concept_id=2000000008 AND value_as_concept_id !=2000000008)
 OR (value_source_concept_id=1586142 AND value_as_concept_id !=45879439)
 OR (value_source_concept_id=1586143 AND value_as_concept_id !=1586143)
 OR (value_source_concept_id=1586146 AND value_as_concept_id !=45877987)
 OR (value_source_concept_id=1586147 AND value_as_concept_id !=1586147)
 OR (value_source_concept_id=1586148 AND value_as_concept_id !=45882607)
 OR (value_source_concept_id=903079 AND value_as_concept_id !=1177221)
 OR (value_source_concept_id=903096 AND value_as_concept_id !=903096)
 
'''
df1=pd.read_gbq(query, dialect='standard')  
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query1.2 GR01 Race_value_source_concept_id matched value_as_concept_id in observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query1.2 GR01 Race_value_source_concept_id matched value_as_concept_id in observation', 'result' : ''},  
                ignore_index = True) 
df1

# # 1.3  GR_01_1 Verify that if a person hAS multiple SELECTion(Hispanic + other race) in pre_deid_com_cdr, the output in deid_base_cdr observation table should result in two rows - one for Ethnicity AND one for race. 
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

# ## 1.3.1 step1
# - Verify the following columns in the deid_cdr Observation table have been set to null:
# o   value_as_string
# o   value_source_value
#
# done in first sql

# # 1.3.2 step2  Find person_ids in pre_dedi_com_cdr person table who have ethnicity_source_concept_id values AS 1586147  & race_source_concept_id AS ( 1586146 OR 1586142 OR 1586143) , then verify that the output in the deid_base_cdr observation table for that person_id after mapping  will results in 2-rows .
#
#   step 3
# Verify that the 2-rows have 2-different value_source_concept_id values in the deid_base_cdr Observation table.

query=f''' 

WITH df1 AS (
SELECT m.research_id AS person_id
FROM `{project_id}.{pipeline}.pid_rid_mapping` m 
JOIN `{project_id}.{com_cdr}.person` com 
ON m.person_id = com.person_id 
WHERE com.ethnicity_source_concept_id = 1586147
AND com.race_source_concept_id in (1586142, 1586143, 1586146 )
 ),
 
df2 AS (
SELECT DISTINCT person_id , count (distinct value_source_concept_id ) AS countp
FROM `{project_id}.{deid_base_cdr}.observation`
WHERE  observation_source_value = 'Race_WhatRaceEthnicity' 
GROUP BY person_id
 )
 
 SELECT count (*) FROM df2
 WHERE person_id IN (SELECT person_id FROM df1) AND countp !=2 

    '''
df1=pd.read_gbq(query, dialect='standard')
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query 1.3.2 GR01_1 these person_ids have 2-rows in observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query1.3.2 GR01_1 these person_ids have 2-rows in observation', 'result' : ''},  
                ignore_index = True) 
df1

# ## one error in new cdr

# +
query=f''' 

WITH df1 AS (
SELECT m.research_id AS person_id
FROM `{project_id}.{pipeline}.pid_rid_mapping` m 
JOIN `{project_id}.{com_cdr}.person` com 
ON m.person_id = com.person_id 
WHERE com.ethnicity_source_concept_id = 1586147
AND com.race_source_concept_id in (1586142, 1586143, 1586146 )
 ),
 
df2 AS (
SELECT *
FROM `{project_id}.{deid_base_cdr}.observation`
WHERE  observation_source_value = 'Race_WhatRaceEthnicity' 

 )
 
 SELECT * FROM df2
 WHERE person_id IN (1611212) 

    '''
df1=pd.read_gbq(query, dialect='standard')

df1
# -

# # 1.3.3 step : Verify that if a person_id hAS ethnicity_source_concept_id values AS 1586147  & race_source_concept_id AS ( 1586145 OR 1586144)  in the pre_deid_com_cdr person table, the output in the deid_cdr observation table for that person_id  after mapping will result in 2-rows AND the 2000000001 race value is populated in value_source_concept_id field in the other row of observation table in the deid dataset.

# +

query=f''' 

WITH df1 AS (
SELECT distinct m.research_id AS person_id
FROM `{project_id}.{pipeline}.pid_rid_mapping` m 
join `{project_id}.{com_cdr}.person` com 
ON m.person_id = com.person_id 
WHERE com.ethnicity_source_concept_id = 1586147
AND com.race_source_concept_id IN (1586145, 1586144) 
 ),
 
df2 AS (
SELECT  person_id , count (distinct value_source_concept_id) AS countp
FROM `{project_id}.{deid_base_cdr}.observation`
WHERE  observation_source_value = 'Race_WhatRaceEthnicity' 
AND value_source_concept_id IN (2000000001 ,1586147)
GROUP BY person_id
)
 
SELECT count (*) FROM df1
WHERE person_id NOT IN (SELECT person_id FROM df2 WHERE countp=2)
    '''
df1=pd.read_gbq(query, dialect='standard')
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query1.3.3 GR01_1 these person_ids have 2-rows in observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query1.3.3 GR01_1 these person_ids have 2-rows in observation', 'result' : ''},  
                ignore_index = True) 
df1
# -

# # 1.3.4 Verify that if a person_id hAS ethnicity_source_concept_id values AS 1586147  & race_source_concept_id AS multiple SELECTions (2 or more) in the pre_deid_com_cdr person table, the output in the deid_base_cdr observation table for that person_id  will result in 2 OR MORE rows .
#
# observation_source_value = 'Race_WhatRaceEthnicity' AND value_source_concept_id

query=f''' 

WITH df1 AS (
SELECT distinct person_id
FROM `{project_id}.{deid_base_cdr}.person` 
WHERE ethnicity_source_concept_id = 1586147
AND race_source_concept_id=2000000008
 ),
 
df2 AS (
SELECT DISTINCT person_id , count (distinct value_source_concept_id ) AS countp
FROM `{project_id}.{deid_base_cdr}.observation`
WHERE  observation_source_value = 'Race_WhatRaceEthnicity' 
GROUP BY person_id
 )
 
SELECT count (*) FROM df2
WHERE person_id IN (SELECT person_id FROM df1) AND countp <2 

    '''
df1=pd.read_gbq(query, dialect='standard')
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query1.3.4 GR01_1 these person_ids have 2 or more rows in observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query1.3.4 GR01_1 these person_ids have 2 or more rows in observation', 'result' : ''},  
                ignore_index = True) 
df1

# # 1.4  GR_01_2	"Race Ethnicity:  Race non-responses DC-618
#
# Verify that if race_name / race_source_value field in deid_base_cdr PERSON table populates AS "None Indicated" , the race_concept_id field in deid_base_cdr person table should populate : 2100000001
#
# this test case can be verified only after the PERSON table is repopulated.

# correct version, can be jsut in deid_base_cdr person table
# correct version
query=f''' 

WITH df1 AS (
SELECT distinct race_source_value,race_source_concept_id ,race_concept_id
FROM `{project_id}.{deid_base_cdr}.person`
WHERE race_concept_id = 2100000001 )

SELECT count (*) FROM df1
WHERE  race_source_concept_id !=0 OR race_source_value !='None Indicated'

'''
df1=pd.read_gbq(query, dialect='standard')
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query1.4 GR01_2 Race_source_concept_id suppresion in observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query1.4 GR01_2 Race_source_concept_id suppresion in observation', 'result' : ''},  
                ignore_index = True) 
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

# final version 
query = f'''

WITH df1 AS (
SELECT distinct value_source_concept_id,value_as_concept_id
 FROM `{project_id}.{deid_cdr}.observation`
 WHERE value_source_concept_id in (2000000003,1585900)
 )
 
SELECT count (*) FROM df1
WHERE (value_source_concept_id=2000000003 AND value_as_concept_id !=2000000003)
OR (value_source_concept_id=1585900 AND value_as_concept_id !=4069091)

'''
df1=pd.read_gbq(query, dialect='standard')  
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query2.2 GR02 TheBasics_SexualOrientation matched value_source_concept_id in observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query2.2 GR02 TheBasics_SexualOrientation matched value_source_concept_id in observation', 'result' : ''},  
                ignore_index = True) 
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

# +

query=f''' 

WITH df1 AS (
SELECT m.research_id AS person_id,count (distinct ob.value_source_value) AS countp
FROM `{project_id}.{com_cdr}.observation` ob
JOIN `{project_id}.{pipeline}.pid_rid_mapping` m
ON ob.person_id = m.person_id
WHERE REGEXP_CONTAINS(ob.observation_source_value, 'TheBasics_SexualOrientation')
GROUP BY 1),

df2 AS (
SELECT person_id 
FROM `{project_id}.{deid_base_cdr}.observation` 
WHERE value_as_concept_id =2000000003
)

SELECT count (distinct person_id) FROM df1
WHERE countp >1
AND person_id NOT IN (SELECT distinct person_id FROM df2 )

'''
df1=pd.read_gbq(query, dialect='standard')  
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query2.3 GR02 multiple_SexualOrientation matched value_source_concept_id in observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query2.3 GR02 multiple_SexualOrientation matched value_source_concept_id in observation', 'result' : ''},  
                ignore_index = True) 
df1
# -

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

# final version 
query = f'''

WITH df1 AS (
SELECT distinct value_source_concept_id,value_as_concept_id
 FROM `{project_id}.{deid_cdr}.observation`
 WHERE value_source_concept_id IN (2000000002,1585840,1585839)
 )

SELECT count (*) FROM df1
WHERE (value_source_concept_id=2000000002 AND value_as_concept_id !=2000000002)
OR (value_source_concept_id=1585840 AND value_as_concept_id !=45878463)
OR (value_source_concept_id=1585839 AND value_as_concept_id !=45880669)

'''
df1=pd.read_gbq(query, dialect='standard')  
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query3 GR03 Gender_value_source_concept_id matched value_as_concept_id in observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query3 GR03 Gender_value_source_concept_id matched value_as_concept_id in observation', 'result' : ''},  
                ignore_index = True) 
df1

# ## 3.2 objective: Account for new gender identify response option (DC-654)
# The new concept ID in the gender question, “CloserGenderDescription_TwoSpirit” (value_source_concept_id=701374) needs to be generalized to value_source_concept_id = 2000000002 (GenderIdentity_GeneralizedDiffGender)

# final version
query=f''' 


SELECT count (distinct p.person_id)
FROM  `{project_id}.{com_cdr}.observation` com
JOIN  `{project_id}.{pipeline}.pid_rid_mapping` m 
ON com.person_id=m.person_id
JOIN  `{project_id}.{deid_base_cdr}.observation` p
ON p.person_id=m.research_id
WHERE com.value_source_concept_id = 701374
AND p.observation_source_value='Gender_GenderIdentity'
AND p.value_source_concept_id !=2000000002


 '''
df1=pd.read_gbq(query, dialect='standard')  
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query3.2 GR03 Gender_value_source_concept_id matched value_as_concept_id in observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query3.2 GR03 Gender_value_source_concept_id matched value_as_concept_id in observation', 'result' : ''},  
                ignore_index = True) 
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
# check in deid_base_cdr.person

query=f''' 

WITH df1 AS (
SELECT m.research_id AS person_id
FROM  `{project_id}.{com_cdr}.observation` ob
JOIN  `{project_id}.{pipeline}.pid_rid_mapping` m 
on ob.person_id=m.person_id
WHERE value_source_value IN ('SexAtBirth_Female' ,'GenderIdentity_Man')
GROUP BY m.research_id
HAVING count (distinct value_source_value)=2 
)

  
SELECT COUNT (*) FROM `{project_id}.{deid_base_cdr}.person` 
JOIN `{project_id}.{deid_base_cdr}.person_ext` using (person_id)
WHERE person_id IN (SELECT person_id FROM df1)
AND (sex_at_birth_source_value !='SexAtBirth_Female' AND gender_source_concept_id !=2000000002)

'''
df1=pd.read_gbq(query, dialect='standard')  
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query3.3.1 GR_03_1 Sex_female/gender_man mismatch in deid_base_cdr.person table', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query3.3.1 GR_03_1 Sex_female/gender_man mismatch in deid_base_cdr.person table', 'result' : ''},  
                ignore_index = True) 
df1

# +
# in deid_cdr observation table, should be same if in deid_base_cdr

query=f''' 

WITH df1 AS (
SELECT m.research_id AS person_id
FROM  `{project_id}.{com_cdr}.observation` ob
JOIN  `{project_id}.{pipeline}.pid_rid_mapping` m 
ON ob.person_id=m.person_id
WHERE value_source_value IN ('SexAtBirth_Female' ,'GenderIdentity_Man')
GROUP BY m.research_id
HAVING count (distinct value_source_value)=2 
  
  )
  
SELECT count (*) FROM `{project_id}.{deid_cdr}.observation` 
WHERE person_id IN (SELECT person_id FROM df1)
AND observation_source_value LIKE 'Gender_GenderIdentity'
AND (value_as_concept_id !=2000000002 AND value_source_concept_id !=2000000002)

 '''
df1=pd.read_gbq(query, dialect='standard')  
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query3.3.2 GR_03_1 Sex_female/gender_man mismatch in deid_cdr.observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query3.3.2 GR_03_1 Sex_female/gender_man mismatch in deid_cdr.observation', 'result' : ''},  
                ignore_index = True) 
df1

# +
# check mismatched sex_male/gender_woman in deid_base_cdr.person 

query=f''' 

WITH df1 AS (

SELECT m.research_id AS person_id
FROM  `{project_id}.{com_cdr}.observation` ob
JOIN  `{project_id}.{pipeline}.pid_rid_mapping` m 
ON ob.person_id=m.person_id
WHERE value_source_value IN ('SexAtBirth_Male' ,'GenderIdentity_Woman')
GROUP BY m.research_id
HAVING count (distinct value_source_value)=2 
  )
  
SELECT count (*) FROM `{project_id}.{deid_base_cdr}.person` 
JOIN `{project_id}.{deid_base_cdr}.person_ext` using (person_id)
WHERE person_id IN (SELECT person_id FROM df1) 
AND (sex_at_birth_source_value !='SexAtBirth_Male' AND gender_source_concept_id !=2000000002) 

 '''
df1=pd.read_gbq(query, dialect='standard')  
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query3.3.3 GR_03_1 Sex_male/gender_woman mismatch in deid_base_cdr.person', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query3.3.3 GR_03_1 Sex_male/gender_woman mismatch in deid_base_cdr.person', 'result' : ''},  
                ignore_index = True) 
df1

# +
# in deid_cdr obs

query=f''' 

WITH df1 AS (

SELECT m.research_id AS person_id
FROM `{project_id}.{com_cdr}.observation` ob
JOIN  `{project_id}.{pipeline}.pid_rid_mapping` m 
ON ob.person_id=m.person_id
WHERE value_source_value IN ('SexAtBirth_Male' ,'GenderIdentity_Woman')
GROUP BY m.research_id
HAVING count (distinct value_source_value)=2 
)
  
SELECT count (*) FROM  `{project_id}.{deid_cdr}.observation`
WHERE person_id IN (SELECT person_id FROM df1)
AND observation_source_value LIKE 'Gender_GenderIdentity'
AND (value_as_concept_id !=2000000002 AND value_source_concept_id !=2000000002)
'''
df1=pd.read_gbq(query, dialect='standard')  
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query3.3.4 GR_03_1 Sex_male/gender_woman mismatch in deid_cdr.observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query3.3.4 GR_03_1 Sex_male/gender_woman mismatch in deid_cdr.observation', 'result' : ''},  
                ignore_index = True) 
df1
# -

# # 4 GR_04 Biological Sex Generalization Rule
#
# Verify that the field identified for de-identification action in OBSERVATION table follow the Biological Sex Generalization Rule for the de-id table.
#
# "Verify the following columns in the Observation table have been set to null:
# o   value_as_string
# o   value_source_value"
#
# which hAS been done in query 1
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

# final version 
query = f'''

WITH df1 AS (
SELECT distinct value_source_concept_id,value_as_concept_id
FROM `{project_id}.{deid_cdr}.observation`
WHERE value_source_concept_id IN (2000000009,1585847,1585846)
 )

SELECT count (*) FROM df1
WHERE (value_source_concept_id=2000000009 AND value_as_concept_id !=2000000009)
OR (value_source_concept_id=1585847 AND value_as_concept_id !=45878463)
OR (value_source_concept_id=1585846 AND value_as_concept_id !=45880669)

'''
df1=pd.read_gbq(query, dialect='standard')  
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query4.2 GR_04 Biological Sex Generalization Rule in observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query4.2 GR_04 Biological Sex Generalization Rule in observation', 'result' : ''},  
                ignore_index = True) 
df1

# # 5 GR_05 Education Status Generalization Rule
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

# final version 
query = f'''

WITH df1 AS (
SELECT distinct value_source_concept_id,value_as_concept_id
FROM `{project_id}.{deid_cdr}.observation`
WHERE value_source_concept_id IN (2000000007, 2000000006,1585945,1585946,903079,903096)
 )
  
SELECT count (*) FROM df1
WHERE (value_source_concept_id=2000000007 AND value_as_concept_id !=2000000007)
OR (value_source_concept_id=2000000006 AND value_as_concept_id !=2000000006)
OR (value_source_concept_id=1585945 AND value_as_concept_id !=43021808)
OR (value_source_concept_id=1585946 AND value_as_concept_id !=4260980)
OR (value_source_concept_id=903096 AND value_as_concept_id !=903096)
OR (value_source_concept_id=903079 AND value_as_concept_id !=1177221)

'''
df1=pd.read_gbq(query, dialect='standard')  
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query5.2 GR_05 Education Generalization Rule in observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query5.2 GR_05 Education Sex Generalization Rule in observation', 'result' : ''},  
                ignore_index = True) 
df1

# # 6 GR_06 Employment Generalization Rule
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

# final version 
query = f'''

WITH df1 AS (
SELECT distinct value_source_concept_id,value_as_concept_id
FROM `{project_id}.{deid_cdr}.observation`
WHERE value_source_concept_id IN (2000000005, 2000000004,903079,903096)
 )
 
SELECT count (*) FROM df1
WHERE (value_source_concept_id=2000000005 AND value_as_concept_id !=2000000005)
OR (value_source_concept_id=2000000004 AND value_as_concept_id !=2000000004)
OR (value_source_concept_id=903079 AND value_as_concept_id !=1177221)
OR (value_source_concept_id=903096 AND value_as_concept_id !=903096)


'''
df1=pd.read_gbq(query, dialect='standard')  
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query6.2 GR_06 Employment Generalization Rule in observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query6.2 GR_06 Employment Generalization Rule in observation', 'result' : ''},  
                ignore_index = True) 
df1

# # Summary_Generalization_rule

# if not pass, will be highlighted in red
df = df.mask(df.isin(['Null','']))
df.style.highlight_null(null_color='red').set_properties(**{'text-align': 'left'})
