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

# # QA queries on new CDR_deid  Row Suppression-ICD10ICD9 Snome
#
# see [DC-852] AND [DC-732] for more details

import urllib
import pandas as pd
from common import JINJA_ENV
from utils import auth
from gcloud.bq import BigQueryClient
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES
pd.options.display.max_rows = 120

# + tags=["parameters"]
project_id = ""
deid_cdr=""
run_as=""

# +
impersonation_creds = auth.get_impersonation_credentials(
    run_as, target_scopes=IMPERSONATION_SCOPES)

client = BigQueryClient(project_id, credentials=impersonation_creds)
# -

# df will have a summary in the end
df = pd.DataFrame(columns = ['query', 'result']) 

# # 1 PRC_1 Verify all ICD9(764 -779)/ICD10(P) concept_codes used to specify other conditions originating In the perinatal period (including birth trauma),are not generated/displayed as condition_source_value in the CONDITION_OCCURENCE table

query = JINJA_ENV.from_string("""

WITH ICD_suppressions AS (
SELECT concept_id 
FROM   `{{project_id}}.{{deid_cdr}}.concept`
WHERE 
(vocabulary_id='ICD9CM' AND 
    (concept_code LIKE '764%' OR concept_code LIKE '765%' OR concept_code LIKE '766%' OR 
     concept_code LIKE '767%' OR concept_code LIKE '768%' OR concept_code LIKE '769%' OR concept_code LIKE '770%' OR 
     concept_code LIKE '771%' OR concept_code LIKE '772%' OR concept_code LIKE '773%' OR concept_code LIKE '774%' OR 
     concept_code LIKE '775%' OR concept_code LIKE '776%' OR concept_code LIKE '777%' OR concept_code LIKE '778%' OR 
     concept_code LIKE '779%')) 
OR (vocabulary_id='ICD10CM' AND 
    concept_code LIKE 'P%')
)
    
SELECT COUNT (*) AS n_row_not_pass
FROM   `{{project_id}}.{{deid_cdr}}.condition_occurrence` p1
JOIN ICD_suppressions p2
ON p1.condition_source_concept_id=p2.concept_id
WHERE condition_source_value IS NOT NULL
""")
q = query.render(project_id=project_id,deid_cdr=deid_cdr)  
df1=execute(client, q)  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query1 ICD9(764 -779)/ICD10(P) in condition', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query1 ICD9(764 -779)/ICD10(P) in condition', 'result' : 'Failure'},  
                ignore_index = True) 
df1

# # 2 PRC_2 Verify all ICD9(764 -779)/ICD10(P) concept_codes used to specify other conditions originating In the perinatal period (including birth trauma),are not generated/displayed as observation_source_value in the OBSERVATION table

query = JINJA_ENV.from_string("""
WITH ICD_suppressions AS (
SELECT concept_id 
FROM   `{{project_id}}.{{deid_cdr}}.concept`
WHERE 
(vocabulary_id='ICD9CM' AND 
    (concept_code LIKE '765%' OR concept_code LIKE '766%' OR 
     concept_code LIKE '767%' OR concept_code LIKE '768%' OR concept_code LIKE '769%' OR concept_code LIKE '770%' OR 
     concept_code LIKE '771%' OR concept_code LIKE '772%' OR concept_code LIKE '773%' OR concept_code LIKE '774%' OR 
     concept_code LIKE '775%' OR concept_code LIKE '776%' OR concept_code LIKE '777%' OR concept_code LIKE '778%' OR 
     concept_code LIKE '779%')) 
OR (vocabulary_id='ICD10CM' AND concept_code LIKE 'P%')
)
    
SELECT COUNT (*) AS n_row_not_pass
FROM  `{{project_id}}.{{deid_cdr}}.observation` p1
JOIN ICD_suppressions p2
ON p1.observation_source_concept_id=p2.concept_id
WHERE observation_source_value IS NOT NULL
""")
q = query.render(project_id=project_id,deid_cdr=deid_cdr)  
df1=execute(client, q)  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query2 ICD9(764 -779)/ICD10(P)', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query2 ICD9(764 -779)/ICD10(P)', 'result' : 'Failure'},  
                ignore_index = True) 
df1

# # 3 PRC_3 Verify all CD9(V3)/ICD10(Z38) concept_codes  used to specify Liveborn infants according to type of birth are not generated/displayed as observation_source_value in the OBSERVATION table

query = JINJA_ENV.from_string("""

WITH ICD_suppressions AS (
SELECT concept_id 
FROM   `{{project_id}}.{{deid_cdr}}.concept`
WHERE 
(vocabulary_id='ICD9CM' AND (concept_code LIKE 'V3%' )) 
OR (vocabulary_id='ICD10CM' AND concept_code LIKE 'Z38%')
)
SELECT COUNT (*) AS n_row_not_pass
FROM   `{{project_id}}.{{deid_cdr}}.observation` p1
JOIN ICD_suppressions p2
ON p1.observation_source_concept_id=p2.concept_id
WHERE observation_source_value IS NOT NULL
""")
q = query.render(project_id=project_id,deid_cdr=deid_cdr)  
df1=execute(client, q)  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query3 CD9(V3)/ICD10(Z38) in obs', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query3 CD9(V3)/ICD10(Z38) in obs', 'result' : 'Failure'},  
                ignore_index = True) 
df1

# # 4 PRC_4 Verify all CD9(V3)/ICD10(Z38) concept_codes  used to specify Liveborn infants according to type of birth  are not generated/displayed as condition_source_value in the CONDITION_OCCURENCE table

query = JINJA_ENV.from_string("""

WITH ICD_suppressions AS (
SELECT concept_id 
FROM   `{{project_id}}.{{deid_cdr}}.concept`
WHERE 
(vocabulary_id='ICD9CM' AND (concept_code LIKE 'V3%' )) 
OR (vocabulary_id='ICD10CM' AND concept_code LIKE 'Z38%')
 )
    
SELECT COUNT (*) AS n_row_not_pass
FROM   `{{project_id}}.{{deid_cdr}}.condition_occurrence` p1
JOIN ICD_suppressions p2
ON p1.condition_source_concept_id=p2.concept_id
WHERE condition_source_value IS NOT NULL
""")
q = query.render(project_id=project_id,deid_cdr=deid_cdr)  
df1=execute(client, q)  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query4 CD9(V3)/ICD10(Z38) in condition ', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query4 CD9(V3)/ICD10(Z38) in condition ', 'result' : 'Failure'},  
                ignore_index = True) 
df1

# # 5 PRC_5 Verify all ICD9(798)/ICD10(R99) concept_codes  used to specify Unknown cause of death are not generated/displayed as observation_source_value in the OBSERVATION table

query = JINJA_ENV.from_string("""

WITH ICD_suppressions AS (
SELECT concept_id 
FROM   `{{project_id}}.{{deid_cdr}}.concept`
WHERE 
(vocabulary_id='ICD9CM' AND (concept_code LIKE '798%' )) 
OR (vocabulary_id='ICD10CM' AND concept_code LIKE 'R99%')
)
    
SELECT COUNT (*) AS n_row_not_pass
FROM   `{{project_id}}.{{deid_cdr}}.observation` p1
JOIN ICD_suppressions p2
ON p1.observation_source_concept_id=p2.concept_id
WHERE observation_source_value IS NOT NULL

""")
q = query.render(project_id=project_id,deid_cdr=deid_cdr)  
df1=execute(client, q)  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query5 ICD9(798)/ICD10(R99) in obs', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query5 ICD9(798)/ICD10(R99) in obs', 'result' : 'Failure'},  
                ignore_index = True) 
df1

# # 6 PRC_6 Verify all ICD9(799)/ICD10(R99) concept_codes  used to specify Unknown cause of death  are not generated/displayed as condition_source_value in the CONDITION_OCCURENCE table
#
# <font color='red'> question, ICD9(798) is in the title but in the note, it is 799. ICD10 (R99) in the title but not in the note though. confused here. 
#  
# after test in the new cdr, should be ICD798 not 799. The original title was wrong. 

query = JINJA_ENV.from_string("""

WITH ICD_suppressions AS (
SELECT concept_id 
FROM   `{{project_id}}.{{deid_cdr}}.concept`
WHERE (vocabulary_id='ICD9CM' AND (concept_code LIKE '798%' )) 
OR (vocabulary_id='ICD10CM' AND concept_code LIKE 'R99%')
 )
    
SELECT COUNT (*) AS n_row_not_pass
FROM   `{{project_id}}.{{deid_cdr}}.condition_occurrence` p1
JOIN ICD_suppressions p2
ON p1.condition_source_concept_id=p2.concept_id
WHERE condition_source_value IS NOT NULL

""")
q = query.render(project_id=project_id,deid_cdr=deid_cdr)  
df1=execute(client, q)  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query6 ICD9(799)/ICD10(R99) in condition', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query6 ICD9(799)/ICD10(R99) in condition', 'result' : 'Failure'},  
                ignore_index = True) 
df1

# # 7 PRC_7 Verify all ICD10(Y36) codes used to specify Injury due to war operations are not generated/displayed as condition_source_value in the CONDITION_OCCURENCE table

query = JINJA_ENV.from_string("""

WITH ICD_suppressions AS (
SELECT concept_id 
FROM 
   `{{project_id}}.{{deid_cdr}}.concept`
WHERE (vocabulary_id='ICD9CM' AND (concept_code LIKE 'E99%' )) 
OR (vocabulary_id='ICD10CM' AND concept_code LIKE 'Y36%')
)
    
SELECT COUNT (*) AS n_row_not_pass
FROM  `{{project_id}}.{{deid_cdr}}.condition_occurrence` p1
JOIN ICD_suppressions p2
ON p1.condition_source_concept_id=p2.concept_id
WHERE condition_source_value IS NOT NULL

""")
q = query.render(project_id=project_id,deid_cdr=deid_cdr)  
df1=execute(client, q)  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query7 ICD10(Y36) in condition', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query7 ICD10(Y36) in condition', 'result' : 'Failure'},  
                ignore_index = True) 
df1

# # 8 PRC_8 Verify all ICD10(Y36) codes used to specify Injury due to war operations  are not generated/displayed as observation_source_value in the OBSERVATION table

query = JINJA_ENV.from_string("""

WITH ICD_suppressions AS (
SELECT concept_id 
FROM   `{{project_id}}.{{deid_cdr}}.concept`
WHERE (vocabulary_id='ICD9CM' AND ( concept_code LIKE 'E100%' )) 
OR (vocabulary_id='ICD10CM' AND  concept_code LIKE 'Y36%')
)
    
SELECT COUNT (*) AS n_row_not_pass
FROM   `{{project_id}}.{{deid_cdr}}.observation` p1
JOIN ICD_suppressions p2
ON p1.observation_source_concept_id=p2.concept_id
WHERE observation_source_value IS NOT NULL

""")
q = query.render(project_id=project_id,deid_cdr=deid_cdr)  
df1=execute(client, q)  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query8 ICD10(Y36) in obs', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query8 ICD10(Y36) in obs', 'result' : 'Failure'},  
                ignore_index = True) 
df1

# # 9 PRC_9 Verify all ICD10(Y37) codes used to specify Military operations are not generated/displayed as observation_source_value in the OBSERVATION table

query = JINJA_ENV.from_string("""

WITH ICD_suppressions AS (
SELECT concept_id 
FROM    `{{project_id}}.{{deid_cdr}}.concept`
WHERE (vocabulary_id='ICD10CM' AND  concept_code LIKE 'Y37%')
)
SELECT COUNT (*) AS n_row_not_pass
FROM   `{{project_id}}.{{deid_cdr}}.observation` p1
JOIN ICD_suppressions p2
ON p1.observation_source_concept_id=p2.concept_id
WHERE observation_source_value IS NOT NULL
""")
q = query.render(project_id=project_id,deid_cdr=deid_cdr)  
df1=execute(client, q)  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query9 ICD10(Y37)  in observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query9 ICD10(Y37)  in observation', 'result' : 'Failure'},  
                ignore_index = True) 
df1

# # 10 PRC_10 Verify all ICD10(Y37) codes used to specify Military operations are not generated/displayed as condition_source_value in the CONDITION_OCCURENCE table

query = JINJA_ENV.from_string("""

WITH ICD_suppressions AS (
SELECT concept_id 
FROM   `{{project_id}}.{{deid_cdr}}.concept`
WHERE (vocabulary_id='ICD10CM' AND   concept_code LIKE 'Y37%')
    )
SELECT COUNT (*) AS n_row_not_pass
FROM   `{{project_id}}.{{deid_cdr}}.condition_occurrence` p1
JOIN ICD_suppressions p2
ON p1.condition_source_concept_id=p2.concept_id
WHERE condition_source_value IS NOT NULL
""")
q = query.render(project_id=project_id,deid_cdr=deid_cdr)  
df1=execute(client, q)  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query10 ICD10(Y37) in condition', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query10 ICD10(Y37) in condition', 'result' : 'Failure'},  
                ignore_index = True) 
df1

# # 11 PRC_11 Verify all ICD10(Y35) codes used to specify Legal intervention are not generated/displayed as condition_source_value in the CONDITION_OCCURENCE table

query = JINJA_ENV.from_string("""

WITH ICD_suppressions AS (
SELECT concept_id 
FROM    `{{project_id}}.{{deid_cdr}}.concept`
WHERE (vocabulary_id='ICD9CM' AND ( concept_code LIKE 'E97%' )) 
OR (vocabulary_id='ICD10CM' AND concept_code LIKE 'Y35%')
)
SELECT COUNT (*) AS n_row_not_pass
FROM   `{{project_id}}.{{deid_cdr}}.condition_occurrence` p1
JOIN ICD_suppressions p2
ON p1.condition_source_concept_id=p2.concept_id
WHERE condition_source_value IS NOT NULL
""")
q = query.render(project_id=project_id,deid_cdr=deid_cdr)  
df1=execute(client, q)  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query11 ICD10(Y35) in condition', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query11 ICD10(Y35) in condition', 'result' : 'Failure'},  
                ignore_index = True) 
df1

# # 12 PRC_12 Verify all ICD10(Y38)/ICD9CM(E979)  codes used to specify Terrorism are not generated/displayed as observation_source_value in the OBSERVATION table

query = JINJA_ENV.from_string("""

WITH ICD_suppressions AS (
SELECT concept_id 
FROM    `{{project_id}}.{{deid_cdr}}.concept`
WHERE (vocabulary_id='ICD9CM' AND ( concept_code LIKE 'E979%' )) 
OR (vocabulary_id='ICD10CM' AND concept_code LIKE 'Y38%')
)
SELECT COUNT (*) AS n_row_not_pass
FROM   `{{project_id}}.{{deid_cdr}}.observation` p1
JOIN ICD_suppressions p2
ON p1.observation_source_concept_id=p2.concept_id
WHERE observation_source_value IS NOT NULL

""")
q = query.render(project_id=project_id,deid_cdr=deid_cdr)  
df1=execute(client, q)  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query12 ICD10(Y38)/ICD9CM(E979) in obs', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query12 ICD10(Y38)/ICD9CM(E979) in obs', 'result' : 'Failure'},  
                ignore_index = True) 
df1

# # 13 PRC_13 Verify all ICD10(Y38)/ICD9CM(E979) codes used to specify Terrorism are not generated/displayed as condition_source_value in the CONDITION_OCCURENCE table

query = JINJA_ENV.from_string("""

WITH ICD_suppressions AS (
SELECT concept_id 
FROM   `{{project_id}}.{{deid_cdr}}.concept`
WHERE (vocabulary_id='ICD9CM' AND ( concept_code LIKE 'E979%' )) 
OR (vocabulary_id='ICD10CM' AND concept_code LIKE 'Y38%')
)
SELECT COUNT (*) AS n_row_not_pass
FROM   `{{project_id}}.{{deid_cdr}}.condition_occurrence` p1
JOIN ICD_suppressions p2
ON p1.condition_source_concept_id=p2.concept_id
WHERE condition_source_value IS NOT NULL

""")
q = query.render(project_id=project_id,deid_cdr=deid_cdr)  
df1=execute(client, q)  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query13 ICD10(Y38)/ICD9CM(E979) in condition', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query13 ICD10(Y38)/ICD9CM(E979) in condition', 'result' : 'Failure'},  
                ignore_index = True) 
df1

# # 14 PRC_14 Verify all ICD9(E96)/ICD10(X92-Y09) codes used to specify Assault/Homicide are not generated/displayed as condition_source_value in the CONDITION_OCCURENCE table

query = JINJA_ENV.from_string("""

WITH ICD_suppressions AS (
SELECT concept_id 
FROM   `{{project_id}}.{{deid_cdr}}.concept`
WHERE (vocabulary_id='ICD9CM' AND (concept_code LIKE 'E96%' )) 
OR (vocabulary_id='ICD10CM' AND (concept_code LIKE 'X92%' OR 
     concept_code LIKE 'X93%' OR concept_code LIKE 'X94%' OR concept_code LIKE 'X95%' OR 
     concept_code LIKE 'X96%' OR concept_code LIKE 'X97%' OR concept_code LIKE 'X98%' OR 
     concept_code LIKE 'X99%' OR concept_code LIKE 'Y00%' OR concept_code LIKE 'Y01%' OR 
     concept_code LIKE 'Y02%' OR concept_code LIKE 'Y03%' OR concept_code LIKE 'Y04%' OR 
     concept_code LIKE 'Y05%' OR concept_code LIKE 'Y06%' OR concept_code LIKE 'Y07%' OR 
     concept_code LIKE 'Y08%' OR concept_code LIKE 'Y09%'))
    )
    
SELECT COUNT (*) AS n_row_not_pass
FROM   `{{project_id}}.{{deid_cdr}}.condition_occurrence` p1
JOIN ICD_suppressions p2
ON p1.condition_source_concept_id=p2.concept_id
WHERE condition_source_value IS NOT NULL

""")
q = query.render(project_id=project_id,deid_cdr=deid_cdr)  
df1=execute(client, q)  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query14 ICD9(E96)/ICD10(X92-Y09) in condition', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query14 ICD9(E96)/ICD10(X92-Y09) in condition', 'result' : 'Failure'},  
                ignore_index = True) 
df1

# # 15 PRC_15 Verify all ICD9(E96)/ICD10(X92-Y09) codes used to specify Assault/Homicide are not generated/displayed as observation_source_value in the OBSERVATION table

query = JINJA_ENV.from_string("""

WITH ICD_suppressions AS (
SELECT concept_id 
FROM    `{{project_id}}.{{deid_cdr}}.concept`
WHERE (vocabulary_id='ICD9CM' AND (concept_code LIKE 'E96%' )) 
OR (vocabulary_id='ICD10CM' AND (concept_code LIKE 'X92%' OR 
     concept_code LIKE 'X93%' OR concept_code LIKE 'X94%' OR concept_code LIKE 'X95%' OR 
     concept_code LIKE 'X96%' OR concept_code LIKE 'X97%' OR concept_code LIKE 'X98%' OR 
     concept_code LIKE 'X99%' OR concept_code LIKE 'Y00%' OR concept_code LIKE 'Y01%' OR 
     concept_code LIKE 'Y02%' OR concept_code LIKE 'Y03%' OR concept_code LIKE 'Y04%' OR 
     concept_code LIKE 'Y05%' OR concept_code LIKE 'Y06%' OR concept_code LIKE 'Y07%' OR 
     concept_code LIKE 'Y08%' OR concept_code LIKE 'Y09%'))
    )
    
SELECT COUNT (*) AS n_row_not_pass
FROM   `{{project_id}}.{{deid_cdr}}.observation` p1
JOIN ICD_suppressions p2
ON p1.observation_source_concept_id=p2.concept_id
WHERE observation_source_value IS NOT NULL

""")
q = query.render(project_id=project_id,deid_cdr=deid_cdr)  
df1=execute(client, q)  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query15 ICD9(E96)/ICD10(X92-Y09) in observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query15 ICD9(E96)/ICD10(X92-Y09) in observation', 'result' : 'Failure'},  
                ignore_index = True) 
df1

# # 16 PRC_16 Verify all ICD9(E95) codes used to specify Suicide are not generated/displayed as observation_source_value in the OBSERVATION table

query = JINJA_ENV.from_string("""

WITH ICD_suppressions AS (
SELECT concept_id 
FROM  `{{project_id}}.{{deid_cdr}}.concept`
WHERE (vocabulary_id='ICD9CM' AND  (concept_code LIKE 'E95%' )) )
    
SELECT COUNT (*) AS n_row_not_pass
FROM  `{{project_id}}.{{deid_cdr}}.observation` p1
JOIN ICD_suppressions p2
ON p1.observation_source_concept_id=p2.concept_id
WHERE observation_source_value IS NOT NULL
""")
q = query.render(project_id=project_id,deid_cdr=deid_cdr)  
df1=execute(client, q)  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query16 ICD9(E95) in observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query16 ICD9(E95) in observation', 'result' : 'Failure'},  
                ignore_index = True) 
df1

# # 17 PRC_17 Verify all ICD9(E95) codes used to specify Suicide are not generated/displayed as condition_source_value in the CONDITION_OCCURENCE table

query = JINJA_ENV.from_string("""

WITH ICD_suppressions AS (
SELECT concept_id 
FROM   `{{project_id}}.{{deid_cdr}}.concept`
WHERE vocabulary_id='ICD9CM' AND concept_code LIKE 'E95%' )
    
SELECT COUNT (*) AS n_row_not_pass
FROM  `{{project_id}}.{{deid_cdr}}.condition_occurrence` p1
JOIN ICD_suppressions p2
ON p1.condition_source_concept_id=p2.concept_id
WHERE condition_source_value IS NOT NULL

""")
q = query.render(project_id=project_id,deid_cdr=deid_cdr)  
df1=execute(client, q)  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query17 ICD9(E95) in condition', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query17 ICD9(E95) in condition', 'result' : 'Failure'},  
                ignore_index = True) 
df1

# # 18 PRC_18 Verify all ICD9(E928.0)/ICD10(X52) codes used to specify Prolonged stay in weightlessness are not generated/displayed as condition_source_value in the CONDITION_OCCURENCE table

query = JINJA_ENV.from_string("""

WITH ICD_suppressions AS (
SELECT concept_id 
FROM   `{{project_id}}.{{deid_cdr}}.concept`
WHERE (vocabulary_id='ICD9CM' AND (concept_code LIKE 'E928.0' )) 
     OR (vocabulary_id='ICD10CM' AND concept_code LIKE 'X52%')
)
    
SELECT COUNT (*) AS n_row_not_pass
FROM    `{{project_id}}.{{deid_cdr}}.condition_occurrence` p1
JOIN ICD_suppressions p2
ON p1.condition_source_concept_id=p2.concept_id
WHERE condition_source_value IS NOT NULL

""")
q = query.render(project_id=project_id,deid_cdr=deid_cdr)  
df1=execute(client, q)  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query18 ICD9(E928.0)/ICD10(X52)  in condition', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query18 in condition', 'result' : 'Failure'},  
                ignore_index = True) 
df1

# # 19 PRC_19 Verify all ICD9(E928.0)/ICD10(X52) codes used to specify Prolonged stay in weightlessness are not generated/displayed as observation_source_value in the OBSERVATION  table

query = JINJA_ENV.from_string("""

WITH ICD_suppressions AS (
SELECT concept_id 
FROM   `{{project_id}}.{{deid_cdr}}.concept`
WHERE (vocabulary_id='ICD9CM' AND (concept_code LIKE 'E928.0' )) 
OR (vocabulary_id='ICD10CM' AND concept_code LIKE 'X52%')
 )
    
SELECT COUNT (*) AS n_row_not_pass
FROM  `{{project_id}}.{{deid_cdr}}.observation` p1
JOIN ICD_suppressions p2
ON p1.observation_source_concept_id=p2.concept_id
WHERE observation_source_value IS NOT NULL
""")
q = query.render(project_id=project_id,deid_cdr=deid_cdr)  
df1=execute(client, q)  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query19 ICD9(E928.0)/ICD10(X52)  in obs', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query19 ICD9(E928.0)/ICD10(X52)  in obs', 'result' : 'Failure'},  
                ignore_index = True) 
df1

# # 20 PRC_20 Verify all ICD9(E910)/ICD10(W65-W74) codes used to specify Drowning are not generated/displayed as observation_source_value in the OBSERVATION table

query = JINJA_ENV.from_string("""

WITH ICD_suppressions AS (
SELECT concept_id 
FROM  `{{project_id}}.{{deid_cdr}}.concept`
WHERE (vocabulary_id='ICD9CM' AND ( concept_code LIKE 'E910%' )) 
OR (vocabulary_id='ICD10CM' AND (concept_code LIKE 'W65%' OR concept_code LIKE 'W66%' OR 
     concept_code LIKE 'W67%' OR concept_code LIKE 'W68%' OR concept_code LIKE 'W69%' OR concept_code LIKE 'W70%' OR 
     concept_code LIKE 'W71%' OR concept_code LIKE 'W72%' OR concept_code LIKE 'W73%' OR concept_code LIKE 'W74%'))
)
    
SELECT COUNT (*) AS n_row_not_pass
FROM  `{{project_id}}.{{deid_cdr}}.observation` p1
JOIN ICD_suppressions p2
ON p1.observation_source_concept_id=p2.concept_id
WHERE observation_source_value IS NOT NULL

""")
q = query.render(project_id=project_id,deid_cdr=deid_cdr)  
df1=execute(client, q)  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query20 ICD9(E910)/ICD10(W65-W74) in observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query20 ICD9(E910)/ICD10(W65-W74) in observation', 'result' : 'Failure'},  
                ignore_index = True) 
df1

# # 21 PRC_21 Verify all ICD9(E910)/ICD10(W65-W74) codes used to specify Drowning are not generated/displayed as condition_source_value in the CONDITION_OCCURENCE table

query = JINJA_ENV.from_string("""

WITH ICD_suppressions AS (
SELECT concept_id 
FROM  `{{project_id}}.{{deid_cdr}}.concept`
WHERE (vocabulary_id='ICD9CM' AND (concept_code LIKE 'E910%' )) 
OR    (vocabulary_id='ICD10CM' AND (concept_code LIKE 'W65%' OR concept_code LIKE 'W66%' OR 
     concept_code LIKE 'W67%' OR concept_code LIKE 'W68%' OR concept_code LIKE 'W69%' OR concept_code LIKE 'W70%' OR 
     concept_code LIKE 'W71%' OR concept_code LIKE 'W72%' OR concept_code LIKE 'W73%' OR concept_code LIKE 'W74%'))
    )
SELECT COUNT (*) AS n_row_not_pass
FROM  `{{project_id}}.{{deid_cdr}}.condition_occurrence` p1
JOIN ICD_suppressions p2
ON p1.condition_source_concept_id=p2.concept_id
WHERE condition_source_value IS NOT NULL
""")
q = query.render(project_id=project_id,deid_cdr=deid_cdr)  
df1=execute(client, q)  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query21 ICD9(E910)/ICD10(W65-W74) in condition', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query21 ICD9(E910)/ICD10(W65-W74) in condition', 'result' : 'Failure'},  
                ignore_index = True) 
df1

# # 22 PRC_22 Verify all ICD9(E983)/ICD10(T71) codes used to specify Suffocation are not generated/displayed as condition_source_value in the CONDITION_OCCURENCE table

query = JINJA_ENV.from_string("""

WITH ICD_suppressions AS (
SELECT concept_id 
FROM   `{{project_id}}.{{deid_cdr}}.concept`
WHERE (vocabulary_id='ICD9CM' AND (concept_code LIKE 'E913%' )) 
OR (vocabulary_id='ICD10CM' AND concept_code LIKE 'T71%' )
)
SELECT COUNT (*) AS n_row_not_pass
FROM   `{{project_id}}.{{deid_cdr}}.condition_occurrence` p1
JOIN ICD_suppressions p2
ON p1.condition_source_concept_id=p2.concept_id
WHERE condition_source_value IS NOT NULL

""")
q = query.render(project_id=project_id,deid_cdr=deid_cdr)  
df1=execute(client, q)  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query22 ICD9(E983)/ICD10(T71) in condition', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query22 ICD9(E983)/ICD10(T71) in condition', 'result' : 'Failure'},  
                ignore_index = True) 
df1

# # 23 PRC_23 Verify all ICD9(E913)/ICD10(T71) codes used to specify Suffocation  are not generated/displayed as observation_source_value in the OBSERVATION table

query = JINJA_ENV.from_string("""

WITH ICD_suppressions AS (
SELECT concept_id 
FROM   `{{project_id}}.{{deid_cdr}}.concept`
WHERE (vocabulary_id='ICD9CM' AND (concept_code LIKE 'E913%' )) 
OR (vocabulary_id='ICD10CM' AND concept_code LIKE 'T71%' )
)
    
SELECT COUNT (*) AS n_row_not_pass
FROM   `{{project_id}}.{{deid_cdr}}.observation` p1
JOIN ICD_suppressions p2
ON p1.observation_source_concept_id=p2.concept_id
WHERE observation_source_value IS NOT NULL
""")
q = query.render(project_id=project_id,deid_cdr=deid_cdr)  
df1=execute(client, q)  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query23 ICD9(E913)/ICD10(T71) in observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query23 ICD9(E913)/ICD10(T71) in observation', 'result' : 'Failure'},  
                ignore_index = True) 
df1

# # 24 PRC_24 Verify all ICD9(E80-E84)/ICD10(V) codes used to specify Vehicle accident are not generated/displayed as condition_source_value in the CONDITION_OCCURENCE table

query = JINJA_ENV.from_string("""

WITH ICD_suppressions AS (
SELECT concept_id 
FROM  `{{project_id}}.{{deid_cdr}}.concept`
WHERE (vocabulary_id='ICD9CM' AND 
    (concept_code LIKE 'E80%' OR concept_code LIKE 'E81%' OR concept_code LIKE 'E82%' OR 
     concept_code LIKE 'E83%' OR concept_code LIKE 'E84%'  )) 
OR    (vocabulary_id='ICD10CM' AND concept_code LIKE 'V%' )
)
SELECT COUNT (*) AS n_row_not_pass
FROM   `{{project_id}}.{{deid_cdr}}.condition_occurrence` p1
JOIN ICD_suppressions p2
ON p1.condition_source_concept_id=p2.concept_id
WHERE condition_source_value IS NOT NULL
""")
q = query.render(project_id=project_id,deid_cdr=deid_cdr)  
df1=execute(client, q)  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query24 ICD9(E80-E84)/ICD10(V) in condition', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query24 ICD9(E80-E84)/ICD10(V) in condition', 'result' : 'Failure'},  
                ignore_index = True) 
df1

# # 25 PRC_25 Verify all ICD9(E80-E84)/ICD10(V) codes used to specify Vehicle accident are not generated/displayed as observation_source_value in the OBSERVATION table

query = JINJA_ENV.from_string("""

WITH ICD_suppressions AS (
SELECT concept_id 
FROM   `{{project_id}}.{{deid_cdr}}.concept`
WHERE (vocabulary_id='ICD9CM' AND 
    (concept_code LIKE 'E80%' OR concept_code LIKE 'E81%' OR concept_code LIKE 'E82%' OR 
     concept_code LIKE 'E83%' OR concept_code LIKE 'E84%'  )) 
OR (vocabulary_id='ICD10CM' AND concept_code LIKE 'V%' )
)
    
SELECT COUNT (*) AS n_row_not_pass
FROM  `{{project_id}}.{{deid_cdr}}.observation` p1
JOIN ICD_suppressions p2
ON p1.observation_source_concept_id=p2.concept_id
WHERE observation_source_value IS NOT NULL
""")
q = query.render(project_id=project_id,deid_cdr=deid_cdr)  
df1=execute(client, q)  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query25 ICD9(E80-E84)/ICD10(V) in observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query25 ICD9(E80-E84)/ICD10(V) in observation', 'result' : 'Failure'},  
                ignore_index = True) 
df1


# # Summary_Row_Suppression-ICD9/10

# +
def highlight_cells(val):
    color = 'red' if 'Failure' in val else 'white'
    return f'background-color: {color}' 

df.style.applymap(highlight_cells).set_properties(**{'text-align': 'left'})
