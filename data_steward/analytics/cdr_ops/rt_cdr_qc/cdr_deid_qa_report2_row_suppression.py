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

# # QA queries on new CDR row suppression
#
# Verify all rows identified for suppression in the deid dataset have been set to null.
#
# (Query results: ThIS query returned no results.)

import urllib
import pandas as pd
pd.options.display.max_rows = 120

# + tags=["parameters"]
# Parameters
project_id = ""
deid_cdr = ""
com_cdr ="" 
# -

# df will have a summary in the end
df = pd.DataFrame(columns = ['query', 'result']) 

# # 1 Verify all fields identified for suppression in the OBSERVATION table have been removed from the table in the deid dataset.

query = f'''
WITH df1 AS (
SELECT observation_id
FROM    `{project_id}.{com_cdr}.observation`
WHERE observation_source_value LIKE '%SitePairing%'
OR observation_source_value LIKE '%ArizonaSpecific%'
OR observation_source_value LIKE 'EastSoutheastMichigan%'
OR observation_source_value LIKE 'SitePairing_%' 
)

SELECT 
SUM(CASE WHEN value_as_string IS NOT NULL THEN 1 ELSE 0 END) AS n_value_as_string_not_null,
SUM(CASE WHEN value_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_value_source_value_not_null,
SUM(CASE WHEN observation_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_observation_source_value_not_null
FROM `{project_id}.{deid_cdr}.observation`
WHERE observation_id IN (SELECT observation_id FROM df1)
AND ((observation_source_value IS NOT NULL) 
OR (value_source_value IS NOT NULL)
OR (value_as_string IS NOT NULL))
 '''
df1=pd.read_gbq(query, dialect='standard')  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query1 three colmns suppression in observation table', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query1 observation', 'result' : ''},  
                ignore_index = True) 
df1

# # 2 Verify all fields identified for suppression in the OBSERVATION table have been removed from the table in the deid dataset.

# ## error in new cdr

query=f''' 
WITH df1 AS (
SELECT observation_id
FROM    `{project_id}.{com_cdr}.observation`
WHERE observation_source_value LIKE 'PIIName_%'
OR observation_source_value LIKE 'PIIAddress_%'
OR observation_source_value LIKE 'StreetAddress_%'
OR observation_source_value LIKE 'ConsentPII_%'
OR observation_source_value LIKE 'TheBasics_CountryBornTextBox'
OR observation_source_value LIKE 'PIIContactInformation_Phone'
OR observation_source_value LIKE 'Language_SpokenWrittenLanguage'
OR observation_source_value LIKE 'SocialSecurity_%' 
)

SELECT 
SUM(CASE WHEN value_as_string IS NOT NULL THEN 1 ELSE 0 END) AS n_value_as_string_not_null,
SUM(CASE WHEN value_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_value_source_value_not_null,
SUM(CASE WHEN observation_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_observation_source_value_not_null

FROM `{project_id}.{deid_cdr}.observation`
WHERE observation_id IN (SELECT observation_id FROM df1) 
AND ((observation_source_value IS NOT NULL) 
OR (value_source_value IS NOT NULL)
OR (value_as_string IS NOT NULL))
'''
df1=pd.read_gbq(query, dialect='standard')  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query2 observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query2 observation', 'result' : ''},  
                ignore_index = True) 
df1

# +
query=f''' 
WITH df1 AS (
SELECT observation_id
FROM    `{project_id}.{com_cdr}.observation`
WHERE observation_source_value LIKE 'PIIName_%'
OR observation_source_value LIKE 'PIIAddress_%'
OR observation_source_value LIKE 'StreetAddress_%'
OR observation_source_value LIKE 'ConsentPII_%'
OR observation_source_value LIKE 'TheBasics_CountryBornTextBox'
OR observation_source_value LIKE 'PIIContactInformation_Phone'
OR observation_source_value LIKE 'Language_SpokenWrittenLanguage'
OR observation_source_value LIKE 'SocialSecurity_%' 
)

SELECT  distinct observation_source_value,value_source_value, value_as_string
FROM `{project_id}.{deid_cdr}.observation`
WHERE observation_id IN (SELECT observation_id FROM df1) 
AND ((observation_source_value IS NOT NULL) 
OR (value_source_value IS NOT NULL) 
OR (value_as_string IS NOT NULL)) 
'''
df1=pd.read_gbq(query, dialect='standard')  

df1
# -

# # 3 Verify all fields identified for suppression in the OBSERVATION table have been removed from the table in the deid dataset.

# ## error in new cdr

query=f''' 
WITH df1 AS (
SELECT  observation_id
FROM    `{project_id}.{com_cdr}.observation`
WHERE observation_source_value LIKE '%_Signature'
OR observation_source_value LIKE 'ExtraConsent__%' 
)

SELECT 
SUM(CASE WHEN value_as_string IS NOT NULL THEN 1 ELSE 0 END) AS n_value_as_string_not_null,
SUM(CASE WHEN value_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_value_source_value_not_null,
SUM(CASE WHEN observation_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_observation_source_value_not_null
FROM `{project_id}.{deid_cdr}.observation`
WHERE observation_id IN (SELECT observation_id FROM df1) 
AND ((observation_source_value IS NOT NULL) 
OR (value_source_value IS NOT NULL)
OR (value_as_string IS NOT NULL))
'''
df1=pd.read_gbq(query, dialect='standard')  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query3 observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query3 observation', 'result' : ''},  
                ignore_index = True) 
df1

# +
query=f''' 
WITH df1 AS (
SELECT  observation_id
FROM    `{project_id}.{com_cdr}.observation`
WHERE observation_source_value LIKE '%_Signature'
OR observation_source_value LIKE 'ExtraConsent__%' 
)

SELECT 
distinct observation_source_value,value_source_value, value_as_string
FROM `{project_id}.{deid_cdr}.observation`
WHERE observation_id IN (SELECT observation_id FROM df1) 
AND ((observation_source_value IS NOT NULL) 
OR (value_source_value IS NOT NULL)
OR (value_as_string IS NOT NULL))
'''
df1=pd.read_gbq(query, dialect='standard')  

df1
# -

# # 4 Verify all fields identified for suppression in the OBSERVATION table have been removed from the table in the deid dataset.

query=f''' 
WITH df1 AS (
SELECT observation_id
FROM `{project_id}.{com_cdr}.observation`
WHERE observation_source_value LIKE '%Specific' 
OR observation_source_value LIKE '%NoneOfTheseDescribeMe%' 
OR observation_source_value LIKE '%RaceEthnicityNoneOfThese_%' 
OR observation_source_value LIKE 'NoneOfTheseDescribeMe%'
OR observation_source_value LIKE 'WhatTribeAffiliation_%'
)

SELECT 
SUM(CASE WHEN value_as_string IS NOT NULL THEN 1 ELSE 0 END) AS n_value_as_string_not_null,
SUM(CASE WHEN value_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_value_source_value_not_null,
SUM(CASE WHEN observation_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_observation_source_value_not_null
FROM `{project_id}.{deid_cdr}.observation`
WHERE observation_id IN (SELECT observation_id FROM df1) 
AND ((observation_source_value IS NOT NULL) 
OR (value_source_value IS NOT NULL)
OR (value_as_string IS NOT NULL))
'''
df1=pd.read_gbq(query, dialect='standard')  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query4 observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query4 observation', 'result' : ''},  
                ignore_index = True) 
df1

# # 5 Verify all fields identified for suppression in the OBSERVATION table have been removed from the table in the deid dataset.

# ## error in new cdr

query=f''' 
WITH df1 AS (
SELECT observation_id
FROM `{project_id}.{com_cdr}.observation`
WHERE observation_source_value LIKE '%Gender%' 
OR observation_source_value LIKE '%Sexuality%' 
OR observation_source_value LIKE '%SexAtBirthNoneOfThese_%' 
)
SELECT 
SUM(CASE WHEN value_as_string IS NOT NULL THEN 1 ELSE 0 END) AS n_value_as_string_not_null,
SUM(CASE WHEN value_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_value_source_value_not_null,
SUM(CASE WHEN observation_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_observation_source_value_not_null
 FROM `{project_id}.{deid_cdr}.observation`
WHERE observation_id IN (SELECT observation_id FROM df1) 
AND ((observation_source_value IS NOT NULL) 
OR (value_source_value IS NOT NULL)
OR (value_as_string IS NOT NULL))
'''
df1=pd.read_gbq(query, dialect='standard') 
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query5 Observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query5 Observation', 'result' : ''},  
                ignore_index = True) 
df1

# +
query=f''' 
WITH df1 AS (
SELECT observation_id
FROM `{project_id}.{com_cdr}.observation`
WHERE observation_source_value LIKE '%Gender%' 
OR observation_source_value LIKE '%Sexuality%' 
OR observation_source_value LIKE '%SexAtBirthNoneOfThese_%' 
)
SELECT distinct observation_source_value,value_source_value, value_as_string
FROM `{project_id}.{deid_cdr}.observation`
WHERE observation_id IN (SELECT observation_id FROM df1) 
AND ((observation_source_value IS NOT NULL) 
OR (value_source_value IS NOT NULL)  
OR (value_as_string IS NOT NULL))  
'''
df1=pd.read_gbq(query, dialect='standard') 

df1
# -

# # 6 Verify all fields identified for suppression in the OBSERVATION table have been removed from the table in the deid dataset.

query=f''' 
WITH df1 AS (
SELECT observation_id
FROM  `{project_id}.{com_cdr}.observation`
WHERE  observation_source_value LIKE '%ContactInfo_%' 
OR observation_source_value LIKE 'PersonOneAddress_%' 
OR observation_source_value LIKE 'SecondContactsAddress_%'  
)
SELECT 
SUM(CASE WHEN value_as_string IS NOT NULL THEN 1 ELSE 0 END) AS n_value_as_string_not_null,
SUM(CASE WHEN value_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_value_source_value_not_null,
SUM(CASE WHEN observation_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_observation_source_value_not_null
FROM `{project_id}.{deid_cdr}.observation`
WHERE observation_id IN (SELECT observation_id FROM df1) 
AND ((observation_source_value IS NOT NULL) 
OR (value_source_value IS NOT NULL)
OR (value_as_string IS NOT NULL))
'''
df1=pd.read_gbq(query, dialect='standard') 
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query6 Observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query6 Observation', 'result' : ''},  
                ignore_index = True) 
df1

# # 7 Verify all fields identified for suppression in the OBSERVATION table have been removed from the table in the deid dataset.

query=f''' 
WITH df1 AS (
SELECT observation_id
FROM  `{project_id}.{com_cdr}.observation`
WHERE observation_source_value LIKE 'EmploymentWorkAddress_%'  
)
SELECT 
SUM(CASE WHEN value_as_string IS NOT NULL THEN 1 ELSE 0 END) AS n_value_as_string_not_null,
SUM(CASE WHEN value_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_value_source_value_not_null,
SUM(CASE WHEN observation_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_observation_source_value_not_null
FROM `{project_id}.{deid_cdr}.observation`
WHERE observation_id IN (SELECT observation_id FROM df1) 
AND ((observation_source_value IS NOT NULL) 
OR (value_source_value IS NOT NULL)
OR (value_as_string IS NOT NULL))
 '''
df1=pd.read_gbq(query, dialect='standard') 
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query7 observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query7 observation', 'result' : ''},  
                ignore_index = True) 
df1

# # 8 Verify all fields identified for suppression in the OBSERVATION table have been removed from the table in the deid dataset.

query=f''' 
WITH df1 AS (
SELECT observation_id
FROM  `{project_id}.{com_cdr}.observation`
WHERE  observation_source_value LIKE 'PersonalMedicalHistory_AdditionalDiagnosis' 
OR observation_source_value LIKE 'ActiveDuty_AvtiveDutyServeStatus' 
OR observation_source_value LIKE 'OtherSpecify_OtherDrugsTextBox' 
OR observation_source_value LIKE 'notes')

SELECT 
SUM(CASE WHEN value_as_string IS NOT NULL THEN 1 ELSE 0 END) AS n_value_as_string_not_null,
SUM(CASE WHEN value_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_value_source_value_not_null,
SUM(CASE WHEN observation_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_observation_source_value_not_null 
FROM `{project_id}.{deid_cdr}.observation`
WHERE observation_id IN (SELECT observation_id FROM df1) 
AND ((observation_source_value IS NOT NULL) 
OR (value_source_value IS NOT NULL)
OR (value_as_string IS NOT NULL))
'''
df1=pd.read_gbq(query, dialect='standard') 
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query8 observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query8 observation', 'result' : ''},  
                ignore_index = True) 
df1

# # 9 Verify all fields identified for suppression in the OBSERVATION table have been removed from the table in the deid dataset.

query=f''' 
WITH df1 AS (
SELECT observation_id
FROM `{project_id}.{com_cdr}.observation`
WHERE  observation_source_value LIKE 'OrganTransplantDescription_OtherOrgan'
OR observation_source_value LIKE 'OrganTransplantDescription_OtherTissue')

SELECT 
SUM(CASE WHEN value_as_string IS NOT NULL THEN 1 ELSE 0 END) AS n_value_as_string_not_null,
SUM(CASE WHEN value_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_value_source_value_not_null,
SUM(CASE WHEN observation_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_observation_source_value_not_null
FROM `{project_id}.{deid_cdr}.observation`
WHERE observation_id IN (SELECT observation_id FROM df1) 
AND ((observation_source_value IS NOT NULL) 
OR (value_source_value IS NOT NULL)
OR (value_as_string IS NOT NULL))
'''
df1=pd.read_gbq(query, dialect='standard') 
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query9 observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query9 observation', 'result' : ''},  
                ignore_index = True) 
df1

# # 10 Verify all fields identified for suppression in the OBSERVATION table have been removed from the table in the deid dataset.

query=f''' 
WITH df1 AS (
SELECT observation_id
FROM `{project_id}.{com_cdr}.observation`
WHERE  observation_source_value LIKE 'LivingSituation_CurrentLiving'
OR observation_source_value LIKE 'LivingSituation_LivingSituationFreeText')

SELECT 
SUM(CASE WHEN value_as_string IS NOT NULL THEN 1 ELSE 0 END) AS n_value_as_string_not_null,
SUM(CASE WHEN value_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_value_source_value_not_null,
SUM(CASE WHEN observation_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_observation_source_value_not_null
FROM `{project_id}.{deid_cdr}.observation`
WHERE observation_id IN (SELECT observation_id FROM df1) 
AND ((observation_source_value IS NOT NULL) 
OR (value_source_value IS NOT NULL)
OR (value_as_string IS NOT NULL))

'''
df1=pd.read_gbq(query, dialect='standard') 
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query10 observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query10 observation', 'result' : ''},  
                ignore_index = True) 
df1

# # 11 Verify all fields identified for suppression in the OBSERVATION table have been removed from the table in the deid dataset.

query=f''' 
WITH df1 AS (
SELECT observation_id
FROM  `{project_id}.{com_cdr}.observation`
WHERE  observation_source_value LIKE 'OutsideTravel6Month_OutsideTravel6MonthWhereTravel'
OR observation_source_value LIKE 'OutsideTravel6Month_OutsideTravel6MonthWhere')

SELECT 
SUM(CASE WHEN value_as_string IS NOT NULL THEN 1 ELSE 0 END) AS n_value_as_string_not_null,
SUM(CASE WHEN value_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_value_source_value_not_null,
SUM(CASE WHEN observation_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_observation_source_value_not_null
FROM `{project_id}.{deid_cdr}.observation`
WHERE observation_id IN (SELECT observation_id FROM df1) 
AND ((observation_source_value IS NOT NULL) 
OR (value_source_value IS NOT NULL)
OR (value_as_string IS NOT NULL))


    '''
df1=pd.read_gbq(query, dialect='standard') 
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query11 observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query11 observation', 'result' : ''},  
                ignore_index = True) 
df1

# # 12 Verify all fields identified for suppression in the OBSERVATION table have been removed from the table in the deid dataset.

# ## error in new cdr

query=f''' 
WITH df1 AS (
SELECT observation_id
FROM `{project_id}.{com_cdr}.observation`
WHERE  observation_source_value LIKE 'HowOldWereYou%FreeTextBox' 
OR observation_source_value LIKE '%_WhichConditions%' 
OR observation_source_value LIKE '%_OtherCancer%' 
OR observation_source_value LIKE 'OtherCondition_%' 
OR observation_source_value LIKE 'Other%FreeTextBox' 
OR observation_source_value LIKE 'Other%FreeText' )

SELECT 
SUM(CASE WHEN value_as_string IS NOT NULL THEN 1 ELSE 0 END) AS n_value_as_string_not_null,
SUM(CASE WHEN value_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_value_source_value_not_null,
SUM(CASE WHEN observation_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_observation_source_value_not_null
FROM `{project_id}.{deid_cdr}.observation`
WHERE observation_id IN (SELECT observation_id FROM df1) 
AND ((observation_source_value IS NOT NULL)  
OR (value_source_value IS NOT NULL) 
OR (value_as_string IS NOT NULL))  
 '''
df1=pd.read_gbq(query, dialect='standard') 
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query12 observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query12 observation', 'result' : ''},  
                ignore_index = True) 
df1

# +
query=f''' 
WITH df1 AS (
SELECT observation_id
FROM `{project_id}.{com_cdr}.observation`
WHERE  observation_source_value LIKE 'HowOldWereYou%FreeTextBox' 
OR observation_source_value LIKE '%_WhichConditions%' 
OR observation_source_value LIKE '%_OtherCancer%' 
OR observation_source_value LIKE 'OtherCondition_%' 
OR observation_source_value LIKE 'Other%FreeTextBox' 
OR observation_source_value LIKE 'Other%FreeText' )

SELECT distinct observation_source_value,value_source_value,value_as_string
FROM `{project_id}.{deid_cdr}.observation`
WHERE observation_id IN (SELECT observation_id FROM df1) 
AND ((observation_source_value IS NOT NULL) 
OR (value_source_value IS NOT NULL)
OR (value_as_string IS NOT NULL))
 '''
df1=pd.read_gbq(query, dialect='standard') 

df1
# -

# # 13 Verify all fields identified for suppression in the OBSERVATION table have been removed from the table in the deid dataset.

query=f''' 

SELECT 
SUM(CASE WHEN observation_concept_id IS NOT NULL THEN 1 ELSE 0 END) AS n_observation_concept_id_not_null
FROM `{project_id}.{deid_cdr}.observation`
WHERE    observation_concept_id IN (4013886,4271761,4135376,1585559,43529714,1585917,1585913,43529731,43529729,
43529730,1585933,1585929,1585965)
'''
df1=pd.read_gbq(query, dialect='standard') 
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query13 observation_concept_id suppression in observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query13 observation_concept_id suppression in observation', 'result' : ''},  
                ignore_index = True) 
df1

# # Summary_row_suppression

# if not pass, will be highlighted in red
df = df.mask(df.isin(['Null','']))
df.style.highlight_null(null_color='red').set_properties(**{'text-align': 'left'})
