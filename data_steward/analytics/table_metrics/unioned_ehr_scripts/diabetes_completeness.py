# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.3.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# + tags=["parameters"]
PROJECT_ID = ""
DATASET = ""
PIPELINE_TABLES = ""
LOOKUP_TABLES = ""
RUN_AS = ""
# -

# +
import warnings

warnings.filterwarnings('ignore')
import pandas as pd
import matplotlib.pyplot as plt
import os

from utils import auth
from gcloud.bq import BigQueryClient
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES

impersonation_creds = auth.get_impersonation_credentials(
    RUN_AS, target_scopes=IMPERSONATION_SCOPES)

client = BigQueryClient(PROJECT_ID, credentials=impersonation_creds)

plt.style.use('ggplot')
pd.options.display.max_rows = 999
pd.options.display.max_columns = 999
pd.options.display.max_colwidth = 999


def cstr(s, color='black'):
    return "<text style=color:{}>{}</text>".format(color, s)


# -

cwd = os.getcwd()
cwd = str(cwd)
print("Current working directory is: {cwd}".format(cwd=cwd))

# ### Get the list of HPO IDs
#
# ### NOTE: This assumes that all of the relevant HPOs have a person table.

hpo_id_query = f"""
SELECT REPLACE(table_id, '_person', '') AS src_hpo_id
FROM
`{DATASET}.__TABLES__`
WHERE table_id LIKE '%person' 
AND table_id 
NOT LIKE '%unioned_ehr_%' 
AND table_id NOT LIKE '\\\_%'
"""

site_df = execute(client, hpo_id_query)

get_full_names = f"""
select * from {LOOKUP_TABLES}.hpo_site_id_mappings
"""

full_names_df = execute(client, get_full_names)

# +
full_names_df.columns = ['org_id', 'src_hpo_id', 'site_name', 'display_order']
columns_to_use = ['src_hpo_id', 'site_name']

full_names_df = full_names_df[columns_to_use]
full_names_df['src_hpo_id'] = full_names_df['src_hpo_id'].str.lower()

# +
cols_to_join = ['src_hpo_id']

site_df = pd.merge(site_df, full_names_df, on=['src_hpo_id'], how='left')
# -

site_df

# # Age of participant should NOT be below 18 and should NOT be too high (Achilles rule_id #20 and 21)

# ## Count number of unique participants with age <18

# +

######################################
print('Getting the data from the database...')
######################################

birth_query = f'''
    SELECT
        COUNT(*) AS total,
        sum(case when ({PIPELINE_TABLES}.calculate_age(CURRENT_DATE, EXTRACT(DATE FROM birth_datetime))<18)
            then 1 else 0 end) as minors_in_dataset
    FROM
       `{DATASET}.unioned_ehr_person` AS t1
'''
birth_df = execute(client, birth_query)
print(birth_df.shape[0], 'records received.')
# -

birth_df

# +
######################################
print('Getting the data from the database...')
######################################

birth_query = f'''
    SELECT
        person_id          
    FROM
       `{DATASET}.unioned_ehr_person` AS t1
    where 
        {PIPELINE_TABLES}.calculate_age(CURRENT_DATE, EXTRACT(DATE FROM birth_datetime))<18
'''
birth_df = execute(client, birth_query)
print(birth_df.shape[0], 'records received.')
# -

birth_df

# ## Count number of unique participants with age >120

# +

######################################
print('Getting the data from the database...')
######################################

birth_query = f'''
    SELECT
        COUNT(*) AS total,
        sum(case when {PIPELINE_TABLES}.calculate_age(CURRENT_DATE, EXTRACT(DATE FROM birth_datetime))>120
            then 1 else 0 end) as over_120_in_dataset
         
    FROM
       `{DATASET}.unioned_ehr_person` AS t1
'''
birth_df = execute(client, birth_query)
print(birth_df.shape[0], 'records received.')
# -

birth_df

# +
######################################
print('Getting the data from the database...')
######################################

birth_query = f'''
    SELECT
        person_id          
    FROM
       `{DATASET}.unioned_ehr_person` AS t1
    where 
        {PIPELINE_TABLES}.calculate_age(CURRENT_DATE, EXTRACT(DATE FROM birth_datetime))>120
'''
birth_df = execute(client, birth_query)
print(birth_df.shape[0], 'records received.')
# -

birth_df

# ## Histogram

# +

######################################
print('Getting the data from the database...')
######################################

birth_query = f'''
    SELECT
        {PIPELINE_TABLES}.calculate_age(CURRENT_DATE, EXTRACT(DATE FROM birth_datetime)) as AGE    
    FROM
        `{DATASET}.unioned_ehr_person` AS t1
'''
birth_df = execute(client, birth_query)
print(birth_df.shape[0], 'records received.')
# -

birth_df.head()

birth_df['AGE'].hist(bins=88)

# # Participant should have supporting data in either lab results or drugs if he/she has a condition code for diabetes.

# ## Determine those who have diabetes according to the 'condition' table

persons_with_conditions_related_to_diabetes_query = """
CREATE TABLE `{DATASET}.persons_with_diabetes_according_to_condition_table`
OPTIONS
(expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
)
AS
SELECT
DISTINCT
mco.src_hpo_id, p.person_id
FROM
`{DATASET}.unioned_ehr_person` p
JOIN
`{DATASET}.unioned_ehr_condition_occurrence` co
ON
p.person_id = co.person_id
JOIN
`{DATASET}.concept` c
ON
co.condition_concept_id = c.concept_id
JOIN
`{DATASET}._mapping_condition_occurrence` mco
ON
co.condition_occurrence_id = mco.condition_occurrence_id 
WHERE
LOWER(c.concept_name) LIKE '%diabetes%'
AND
(invalid_reason is null or invalid_reason = '')
GROUP BY 1, 2
ORDER BY 1, 2 DESC
""".format(DATASET=DATASET)

persons_with_conditions_related_to_diabetes = execute(
    client, persons_with_conditions_related_to_diabetes_query)

num_persons_w_diabetes_query = """
SELECT
DISTINCT
COUNT(p.person_id) as num_with_diab
FROM
`{DATASET}.persons_with_diabetes_according_to_condition_table` p
""".format(DATASET=DATASET)

num_persons_w_diabetes = execute(client, num_persons_w_diabetes_query)

# +
diabetics = num_persons_w_diabetes['num_with_diab'][0]

print("There are {diabetics} persons with diabetes in the total dataset".format(
    diabetics=diabetics))
# -

diabetics_per_site_query = """
SELECT
DISTINCT
p.src_hpo_id, COUNT(DISTINCT p.person_id) as num_with_diab
FROM
`{DATASET}.persons_with_diabetes_according_to_condition_table` p
GROUP BY 1
ORDER BY num_with_diab DESC
""".format(DATASET=DATASET)

diabetics_per_site = execute(client, diabetics_per_site_query)

diabetics_per_site

# ## Drug

create_table_with_substantiating_diabetic_drug_concept_ids = """
CREATE OR REPLACE TABLE `{DATASET}.substantiating_diabetic_drug_concept_ids`
OPTIONS (
expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
) AS
SELECT
DISTINCT
ca.descendant_concept_id 
FROM
`{DATASET}.concept` c
JOIN
`{DATASET}.union_concept_ancestor` ca
ON
c.concept_id = ca.ancestor_concept_id 
WHERE
ca.ancestor_concept_id  IN
(1529331,1530014,1594973,1583722,1597756,1560171,19067100,1559684,1503297,1510202,1502826,
1525215,1516766,1547504,1580747,1502809,1515249)
AND
(c.invalid_reason is NULL 
or 
C.invalid_reason = '')
""".format(DATASET=DATASET)

substantiating_diabetic_drug_concept_ids = execute(
    client, create_table_with_substantiating_diabetic_drug_concept_ids)

# +
######################################
print('Getting the data from the database...')
######################################

persons_w_t2d_by_condition_and_substantiating_drugs_query = """
SELECT
DISTINCT
p.src_hpo_id, COUNT(DISTINCT p.person_id) as num_with_diab_and_drugs
FROM
`{DATASET}.persons_with_diabetes_according_to_condition_table` p
RIGHT JOIN
`{DATASET}.unioned_ehr_drug_exposure` de  -- get the relevant drugs
ON
p.person_id = de.person_id
RIGHT JOIN
`{DATASET}.substantiating_diabetic_drug_concept_ids` t2drugs  -- only focus on the drugs that substantiate diabetes
ON
de.drug_concept_id = t2drugs.descendant_concept_id 
GROUP BY 1
ORDER BY num_with_diab_and_drugs DESC
""".format(DATASET=DATASET)

diabetics_with_substantiating_drugs = execute(
    client, persons_w_t2d_by_condition_and_substantiating_drugs_query)
# -

diabetics_with_substantiating_drugs

diabetics_with_substantiating_drugs.shape

# ## glucose_lab

valid_glucose_labs = """
CREATE OR REPLACE TABLE `{DATASET}.valid_glucose_labs`
OPTIONS (
expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
) AS
SELECT
DISTINCT
c.concept_id, c.concept_name
FROM
`{DATASET}.concept` c
JOIN
`{DATASET}.union_concept_ancestor` ca
ON
c.concept_id = ca.descendant_concept_id
WHERE
ca.ancestor_concept_id IN (40795740)
AND
c.invalid_reason IS NULL
OR
c.invalid_reason = ''
""".format(DATASET=DATASET)

valid_glucose_labs = execute(client, valid_glucose_labs)

# #### diabetic persons who have at least one 'glucose' measurement

diabetics_with_glucose_measurement_query = """
SELECT
DISTINCT
p.src_hpo_id, COUNT(DISTINCT p.person_id) as num_with_diab_and_glucose
FROM
`{DATASET}.persons_with_diabetes_according_to_condition_table` p
RIGHT JOIN
`{DATASET}.unioned_ehr_measurement` m
ON
p.person_id = m.person_id -- get the persons with measurements
RIGHT JOIN
`{DATASET}.valid_glucose_labs` vgl
ON
vgl.concept_id = m.measurement_concept_id -- only get those with the substantiating labs
GROUP BY 1
ORDER BY num_with_diab_and_glucose DESC
""".format(DATASET=DATASET)

diabetics_with_glucose_measurement = execute(
    client, diabetics_with_glucose_measurement_query)

# ## a1c

hemoglobin_a1c_desc_query = """
CREATE OR REPLACE TABLE `{DATASET}.a1c_descendants`
OPTIONS (
expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
)
AS
SELECT
DISTINCT
ca.descendant_concept_id as concept_id
FROM
`{DATASET}.union_concept_ancestor` ca
WHERE
ca.ancestor_concept_id IN (40789263)
""".format(DATASET=DATASET)

hemoglobin_a1c_desc = execute(client, hemoglobin_a1c_desc_query)

diabetics_with_a1c_measurement_query = """
SELECT
DISTINCT
p.src_hpo_id, COUNT(DISTINCT p.person_id) as num_with_diab_and_a1c
FROM
`{DATASET}.persons_with_diabetes_according_to_condition_table` p
RIGHT JOIN
`{DATASET}.unioned_ehr_measurement` m
ON
p.person_id = m.person_id -- get the persons with measurements
RIGHT JOIN
`{DATASET}.a1c_descendants` a1c
ON
a1c.concept_id = m.measurement_concept_id -- only get those with the substantiating labs
GROUP BY 1
ORDER BY num_with_diab_and_a1c DESC
""".format(DATASET=DATASET)

# +
diabetics_with_a1c_measurement = execute(client,
                                         diabetics_with_a1c_measurement_query)
# -

# ## insulin

# +
######################################
print('Getting the data from the database...')
######################################

persons_with_insulin_query = """
SELECT
DISTINCT
p.src_hpo_id, COUNT(DISTINCT p.person_id) as num_with_diab_and_insulin
FROM
`{DATASET}.persons_with_diabetes_according_to_condition_table` p
RIGHT JOIN
`{DATASET}.unioned_ehr_drug_exposure` de
ON
de.person_id = p.person_id -- get the persons with measurements
RIGHT JOIN
`{DATASET}.concept` c
ON
de.drug_concept_id = c.concept_id
WHERE
LOWER(c.concept_name) LIKE '%insulin%'  -- generous for detecting insulin
GROUP BY 1
ORDER BY num_with_diab_and_insulin DESC
""".format(DATASET=DATASET)
# -

diabetics_with_insulin = execute(client, persons_with_insulin_query)

final_diabetic_df = pd.merge(diabetics_per_site,
                             diabetics_with_substantiating_drugs,
                             on='src_hpo_id')

final_diabetic_df['diabetics_w_drugs'] = round(
    final_diabetic_df['num_with_diab_and_drugs'] /
    final_diabetic_df['num_with_diab'] * 100, 2)

final_diabetic_df = pd.merge(final_diabetic_df,
                             diabetics_with_glucose_measurement,
                             on='src_hpo_id')

final_diabetic_df['diabetics_w_glucose'] = round(
    final_diabetic_df['num_with_diab_and_glucose'] /
    final_diabetic_df['num_with_diab'] * 100, 2)

final_diabetic_df = pd.merge(final_diabetic_df,
                             diabetics_with_a1c_measurement,
                             on='src_hpo_id')

final_diabetic_df['diabetics_w_a1c'] = round(
    final_diabetic_df['num_with_diab_and_a1c'] /
    final_diabetic_df['num_with_diab'] * 100, 2)

final_diabetic_df = pd.merge(final_diabetic_df,
                             diabetics_with_insulin,
                             on='src_hpo_id')

final_diabetic_df['diabetics_w_insulin'] = round(
    final_diabetic_df['num_with_diab_and_insulin'] /
    final_diabetic_df['num_with_diab'] * 100, 2)

final_diabetic_df = final_diabetic_df.sort_values(by='diabetics_w_glucose',
                                                  ascending=False)

final_diabetic_df

final_diabetic_df.to_csv("{cwd}/diabetes.csv".format(cwd=cwd))
