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

import urllib
import pandas as pd
from common import JINJA_ENV
from utils import auth
from gcloud.bq import BigQueryClient
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES
pd.options.display.max_rows = 120

# + tags=["parameters"]
project_id = ""
combine=""
pipeline=""
rt_cdr_deid = ""
deid_sand=""
rdr_dataset=""
rdr_sandbox=""
run_as=""

# +
impersonation_creds = auth.get_impersonation_credentials(
    run_as, target_scopes=IMPERSONATION_SCOPES)

client = BigQueryClient(project_id, credentials=impersonation_creds)
# -

# df will have a summary in the end
df = pd.DataFrame(columns = ['query', 'result']) 

# # Query 1, whether AIAN pts are present AND are properly generalized 
#
# DST-692
#
# Someone selecting AI/AN should be generalized into Race 2000000008 if they have multiple race selections in the combined dataset.  They should be generalized into Race 2000000001 if they only have a single race selection.  
#
#

query = JINJA_ENV.from_string("""
SELECT 
COUNT (*) row_counts_failure
FROM   `{{project_id}}.{{rt_cdr_deid}}.observation` 
WHERE observation_source_concept_id IN (1586140) 
AND person_id IN 

(SELECT DISTINCT research_id 
FROM  `{{project_id}}.{{combine}}.observation` com
JOIN `{{project_id}}.{{deid_sand}}._deid_map` m ON com.person_id = m.person_id
WHERE observation_source_concept_id = 1586140
AND value_source_concept_id = 1586141
)

AND value_source_concept_id NOT IN (2000000008, 2000000001,1586147)
""")
q = query.render(project_id=project_id,rt_cdr_deid=rt_cdr_deid,combine=combine,deid_sand=deid_sand)
df1=execute(client, q)
df1

if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query1 AIAN are properly generalized', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query1 AIAN are properly generalized', 'result' : 'Failure'},  
                ignore_index = True) 
df

# # Query 2 new checks ON survey_conduct table
#
#
# DST-690 new CDR deid check ON survey_conduct tables
# **Background**
# The survey_conduct AND survey_conduct_ext tables are new to the CDR for the Winter 2022 release.  The tables are related to each other with a foreign key ON survey_conduct_id.  The survey_conduct table is also related to the observation table via a foreign key relationship.  This relationship is survey_conduct.survey_conduct_id = observation.questionnaire_response_id.   The survey_conduct table entries should meet basic data quality checks, as well as privacy checks.
#
# **Scope**
# Add notebook checks to verify the following data quality issues exist in the CDR.
#
# check 1 every non-null observation.questionnaire_response_id value maps to an existing survey_conduct.survey_conduct_id value.
#
# check 2 every survey_conduct.survey_conduct_id value maps to one or more observation.questionnaire_response_id values.
#
# check 3 every non-null survey_conduct_ext.survey_conduct_id value maps to an existing survey_conduct.survey_conduct_id value.
#
# check 4 every survey_conduct.survey_conduct_id value maps to one or more survey_conduct_ext.survey_conduct_id values.
#
# check 5 language:  survey_conduct_ext.language is “es” for participants who took the survey in Spanish AND “en” for participants who took the survey in English. 
#
# check 6 CATI:  Computer Assisted Telephone Interview.  42530794 is used to indicate this in the survey_conduct.assisted_concept_id table AND column for participants who had assistance.  Otherwise, 0.
#
# survey_conduct.collection_method_concept_id in (42530794, 42531021)  WHERE 42530794 is used if the survey was assisted AND 42531021 is used if the survey was unassisted.
#
# **Add or verify checks exist to de-identify the table**
#
# check 7 survey_conduct.survey_conduct_id must map to a research id value defined in {rdr_sandbox}._deid_questionnaire_response_map.research_response_id. 
#
# check 8 dates AND date times in survey_conduct are shifted in the Registered Tier deid dataset.  The COPE survey date values will be unshifted in the Registered Tier deid base dataset.
#
# check 9 vperson_ids are remapped in the survey_conduct table.

query = JINJA_ENV.from_string("""

#check 1 every non-null observation.questionnaire_response_id value maps to an existing survey_conduct.survey_conduct_id value.

WITH df1 AS (
SELECT 'check1' check , 'observation.questionnaire_response_id matches survey_conduct.survey_conduct_id' check_name,
    COUNT (DISTINCT questionnaire_response_id) row_counts_failure
    FROM  `{{project_id}}.{{rt_cdr_deid}}.observation`
    WHERE questionnaire_response_id IS NOT null
    AND questionnaire_response_id NOT IN (SELECT DISTINCT survey_conduct_id
    FROM   `{{project_id}}.{{rt_cdr_deid}}.survey_conduct`
    WHERE survey_conduct_id IS NOT null)
    
   ),

#every survey_conduct.survey_conduct_id value maps to one or more observation.questionnaire_response_id values

df2 AS (
SELECT 'check2' check , 'survey_conduct.survey_conduct_id matches observation.questionnaire_response_id' check_name,

COUNT (DISTINCT survey_conduct_id) row_counts_failure
    FROM   `{{project_id}}.{{rt_cdr_deid}}.survey_conduct`
    WHERE survey_conduct_id IS NOT null
    AND survey_conduct_id NOT IN (SELECT DISTINCT questionnaire_response_id
    FROM  `{{project_id}}.{{rt_cdr_deid}}.observation`
    WHERE questionnaire_response_id IS NOT null)
)  ,

#every non-null survey_conduct_ext.survey_conduct_id value maps to an existing survey_conduct.survey_conduct_id value.

df3 AS (
SELECT 'check3' check , 'survey_conduct_ext.survey_conduct_id matches survey_conduct.survey_conduct_id' check_name,

COUNT (DISTINCT ext.survey_conduct_id) row_counts_failure
    FROM   `{{project_id}}.{{rt_cdr_deid}}.survey_conduct_ext` ext
    WHERE survey_conduct_id IS NOT null
    AND survey_conduct_id NOT IN (SELECT DISTINCT survey_conduct_id
    FROM  `{{project_id}}.{{rt_cdr_deid}}.survey_conduct`
    WHERE survey_conduct_id IS NOT null)
    ),
    
#every survey_conduct.survey_conduct_id value maps to one or more survey_conduct_ext.survey_conduct_id values.¶
df4 AS (
SELECT 'check4' check , 'survey_conduct.survey_conduct_id matches survey_conduct_ext.survey_conduct_id' check_name,

COUNT (DISTINCT s.survey_conduct_id) row_counts_failure
    FROM   `{{project_id}}.{{rt_cdr_deid}}.survey_conduct` s
    WHERE survey_conduct_id IS NOT null
    AND survey_conduct_id NOT IN (SELECT DISTINCT survey_conduct_id
    FROM  `{{project_id}}.{{rt_cdr_deid}}.survey_conduct_ext`
    WHERE survey_conduct_id IS NOT null)
    ),
    
    
df5_1 AS (
SELECT 'check5.1' check , 'survey_conduct_language_english' check_name,
 COUNT (DISTINCT m.person_id) AS row_counts_failure
    FROM  `{{project_id}}.{{rt_cdr_deid}}.survey_conduct` deid
   JOIN `{{project_id}}.{{rt_cdr_deid}}.survey_conduct_ext` using (survey_conduct_id)
   JOIN `{{project_id}}.{{deid_sand}}._deid_map` m ON deid.person_id = m.research_id
   WHERE language ='en'
   AND m.person_id NOT IN (SELECT DISTINCT person_id
    FROM  `{{project_id}}.{{rdr_dataset}}.observation`
   JOIN  `{{project_id}}.{{rdr_dataset}}.questionnaire_response_additional_info` using (questionnaire_response_id)
WHERE type='LANGUAGE' AND value='en')
),

df5_2 AS (
SELECT 'check5.2' check , 'survey_conduct_language_spanish' check_name,

COUNT (DISTINCT m.person_id) AS row_counts_failure
    FROM  `{{project_id}}.{{rt_cdr_deid}}.survey_conduct` deid
   JOIN `{{project_id}}.{{rt_cdr_deid}}.survey_conduct_ext` using (survey_conduct_id)
   JOIN `{{project_id}}.{{deid_sand}}._deid_map` m ON deid.person_id = m.research_id
   WHERE language ='es'
   AND m.person_id NOT IN (SELECT DISTINCT person_id
    FROM  `{{project_id}}.{{rdr_dataset}}.observation`
   JOIN  `{{project_id}}.{{rdr_dataset}}.questionnaire_response_additional_info` using (questionnaire_response_id)
WHERE type='LANGUAGE' AND value='es')
),

df6 AS (
SELECT 'check6' check , 'survey_conduct_CATI' check_name,


COUNT (DISTINCT m.person_id) AS row_counts_failure
    FROM  `{{project_id}}.{{rt_cdr_deid}}.survey_conduct` deid
   JOIN `{{project_id}}.{{rt_cdr_deid}}.survey_conduct_ext` using (survey_conduct_id)
   JOIN `{{project_id}}.{{deid_sand}}._deid_map` m ON deid.person_id = m.research_id
   WHERE ( assisted_source_value ='Telephone' or assisted_concept_id=42530794)
   AND m.person_id NOT IN (SELECT DISTINCT person_id
    FROM  `{{project_id}}.{{rdr_dataset}}.observation`
   JOIN  `{{project_id}}.{{rdr_dataset}}.questionnaire_response_additional_info` using (questionnaire_response_id)
WHERE value='CATI')
),

#survey_conduct.survey_conduct_id must map to a research id 


df7 AS (
SELECT 'check7' check , 'survey_conduct.survey_conduct_id maps a research_response_id' check_name,

COUNT (DISTINCT survey_conduct_id) row_counts_failure
    FROM   `{{project_id}}.{{rt_cdr_deid}}.survey_conduct`
    WHERE survey_conduct_id IS NOT null
    AND survey_conduct_id NOT IN (SELECT DISTINCT research_response_id
    FROM  `{{project_id}}.{{rdr_sandbox}}._deid_questionnaire_response_map`
    WHERE research_response_id IS NOT null)
    ),
    
#check 8 dates AND date times in survey_conduct are shifted in the Registered Tier deid dataset.
    
df8 AS (


SELECT 'check8' check , 'date are shifted' check_name,
COUNT(*) AS row_counts_failure FROM  

(
SELECT 
DATE_DIFF(DATE(i.survey_start_date), DATE(d.survey_start_date),day)-m.shift AS diff_start_date,
DATE_DIFF(DATE(i.survey_end_date), DATE(d.survey_end_date),day)-m.shift AS diff_end_date,
DATE_DIFF(DATE(i.survey_start_datetime), DATE(d.survey_start_datetime),day)-m.shift AS diff_start_datetime,
DATE_DIFF(DATE(i.survey_end_datetime), DATE(d.survey_end_datetime),day)-m.shift AS diff_end_datetime

FROM  `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
JOIN `{{project_id}}.{{combine}}.survey_conduct` i
ON m.person_id = i.person_id
JOIN `{{project_id}}.{{rt_cdr_deid}}.survey_conduct` d
ON d.survey_conduct_id = i.survey_conduct_id)

WHERE diff_start_date !=0 or
diff_end_date !=0 or
diff_start_datetime !=0 or
diff_end_datetime !=0
),

df9 AS (
SELECT 'check9' check , 'person_ids are remapped' check_name,
COUNT (DISTINCT person_id) AS row_counts_failure
    FROM  `{{project_id}}.{{rt_cdr_deid}}.survey_conduct` d
   WHERE person_id NOT IN (SELECT research_id  
   FROM  `{{project_id}}.{{deid_sand}}._deid_map` ))
   
SELECT * FROM  df1
UNION DISTINCT 
SELECT * FROM  df2
UNION DISTINCT 
SELECT * FROM  df3
UNION DISTINCT 
SELECT * FROM  df4
UNION DISTINCT 
SELECT * FROM  df5_1
UNION DISTINCT 
SELECT * FROM  df5_2
UNION DISTINCT 
SELECT * FROM  df6
UNION DISTINCT 
SELECT * FROM  df7
UNION DISTINCT 
SELECT * FROM  df8
UNION DISTINCT 
SELECT * FROM  df9
ORDER BY check

""")
q = query.render(project_id=project_id,rt_cdr_deid=rt_cdr_deid,combine=combine,deid_sand=deid_sand,rdr_dataset=rdr_dataset,rdr_sandbox=rdr_sandbox,pipeline=pipeline)
df1=execute(client, q)
df1.shape

df1

if df1.iloc[:,2].sum()==0:
 df = df.append({'query' : 'Query2 new checks on survey_conduct table', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query2 new checks on survey_conduct table', 'result' : 'Failure'},  
                ignore_index = True) 
df


# # Summary

# +
def highlight_cells(val):
    color = 'red' if 'Failure' in val else 'white'
    return f'background-color: {color}' 

df.style.applymap(highlight_cells).set_properties(**{'text-align': 'left'})
# -


