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

from google.cloud import bigquery

client = bigquery.Client()

# %load_ext google.cloud.bigquery

# %reload_ext google.cloud.bigquery

# +
from notebooks import parameters
DATASET = parameters.UNIONED_Q3_2019

print("Dataset to use: {DATASET}".format(DATASET = DATASET))

# +
#######################################
print('Setting everything up...')
#######################################

import warnings

warnings.filterwarnings('ignore')
import pandas as pd
import matplotlib.pyplot as plt
# %matplotlib inline

import os

plt.style.use('ggplot')
pd.options.display.max_rows = 999
pd.options.display.max_columns = 999
pd.options.display.max_colwidth = 999


def cstr(s, color='black'):
    return "<text style=color:{}>{}</text>".format(color, s)


print('done.')
# -

cwd = os.getcwd()
cwd = str(cwd)
print("Current working directory is: {cwd}".format(cwd=cwd))

# +
dic = {
    'src_hpo_id': [
        "saou_uab_selma", "saou_uab_hunt", "saou_tul", "pitt_temple",
        "saou_lsu", "trans_am_meyers", "trans_am_essentia", "saou_ummc",
        "seec_miami", "seec_morehouse", "seec_emory", "uamc_banner", "pitt",
        "nyc_cu", "ipmc_uic", "trans_am_spectrum", "tach_hfhs", "nec_bmc",
        "cpmc_uci", "nec_phs", "nyc_cornell", "ipmc_nu", "nyc_hh",
        "ipmc_uchicago", "aouw_mcri", "syhc", "cpmc_ceders", "seec_ufl",
        "saou_uab", "trans_am_baylor", "cpmc_ucsd", "ecchc", "chci", "aouw_uwh",
        "cpmc_usc", "hrhc", "ipmc_northshore", "chs", "cpmc_ucsf", "jhchc",
        "aouw_mcw", "cpmc_ucd", "ipmc_rush", "va", "saou_umc"
    ],
    'HPO': [
        "UAB Selma", "UAB Huntsville", "Tulane University", "Temple University",
        "Louisiana State University",
        "Reliant Medical Group (Meyers Primary Care)",
        "Essentia Health Superior Clinic", "University of Mississippi",
        "SouthEast Enrollment Center Miami",
        "SouthEast Enrollment Center Morehouse",
        "SouthEast Enrollment Center Emory", "Banner Health",
        "University of Pittsburgh", "Columbia University Medical Center",
        "University of Illinois Chicago", "Spectrum Health",
        "Henry Ford Health System", "Boston Medical Center", "UC Irvine",
        "Partners HealthCare", "Weill Cornell Medical Center",
        "Northwestern Memorial Hospital", "Harlem Hospital",
        "University of Chicago", "Marshfield Clinic",
        "San Ysidro Health Center", "Cedars-Sinai", "University of Florida",
        "University of Alabama at Birmingham", "Baylor", "UC San Diego",
        "Eau Claire Cooperative Health Center", "Community Health Center, Inc.",
        "UW Health (University of Wisconsin Madison)",
        "University of Southern California", "HRHCare",
        "NorthShore University Health System", "Cherokee Health Systems",
        "UC San Francisco", "Jackson-Hinds CHC", "Medical College of Wisconsin",
        "UC Davis", "Rush University", 
        "United States Department of Veterans Affairs - Boston",
        "University Medical Center (UA Tuscaloosa)"
    ]
}

site_df = pd.DataFrame(data=dic)
site_df

# + endofcell="--"
# # +
######################################
print('Getting the data from the database...')

site_construct_query = """
select distinct * from (
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{DATASET}._mapping_visit_occurrence`
         
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{DATASET}._mapping_condition_occurrence`  
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{DATASET}._mapping_device_exposure`

    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{DATASET}._mapping_drug_exposure`
         
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{DATASET}._mapping_measurement`               
         
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{DATASET}._mapping_observation`           
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{DATASET}._mapping_procedure_occurrence`         
         
    
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{DATASET}._mapping_visit_occurrence`   
    ) 
""".format(DATASET = DATASET)

######################################

site_map = pd.io.gbq.read_gbq(site_construct_query,
            dialect='standard')
print(site_map.shape[0], 'records received.')
# -

site_map
# --

site_df = pd.merge(site_map, site_df, how='outer', on='src_hpo_id')

site_df

# # Participant should have supporting data in either lab results or drugs if he/she has a condition code for diabetes.

# ## Determine those who have diabetes according to the 'condition' table

persons_with_conditions_related_to_diabetes_query = """
CREATE TABLE `{DATASET}.persons_with_diabetes_according_to_condition_table`
OPTIONS
(expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
)
AS
SELECT
DISTINCT
mco.src_hpo_id, p.person_id
FROM
`{DATASET}.person` p
JOIN
`{DATASET}.condition_occurrence` co
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
AND
LOWER(mco.src_hpo_id) NOT LIKE '%rdr%'
GROUP BY 1, 2
ORDER BY 1, 2 DESC
""".format(DATASET = DATASET)

persons_with_conditions_related_to_diabetes = pd.io.gbq.read_gbq(
    persons_with_conditions_related_to_diabetes_query, dialect = 'standard')

num_persons_w_diabetes_query = """
SELECT
DISTINCT
COUNT(p.person_id) as num_with_diab
FROM
`{DATASET}.persons_with_diabetes_according_to_condition_table` p
""".format(DATASET = DATASET)

num_persons_w_diabetes = pd.io.gbq.read_gbq(num_persons_w_diabetes_query, dialect = 'standard')

# +
diabetics = num_persons_w_diabetes['num_with_diab'][0]

print("There are {diabetics} persons with diabetes in the total dataset".format(diabetics = diabetics))
# -

diabetics_per_site_query = """
SELECT
DISTINCT
p.src_hpo_id, COUNT(DISTINCT p.person_id) as num_with_diab
FROM
`{DATASET}.persons_with_diabetes_according_to_condition_table` p
WHERE
LOWER(p.src_hpo_id) NOT LIKE '%rdr%'
GROUP BY 1
ORDER BY num_with_diab DESC
""".format(DATASET = DATASET)

diabetics_per_site = pd.io.gbq.read_gbq(diabetics_per_site_query, dialect = 'standard')

diabetics_per_site

# ## Drug

create_table_with_substantiating_diabetic_drug_concept_ids = """
CREATE TABLE `{DATASET}.substantiating_diabetic_drug_concept_ids`
OPTIONS (
expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
) AS
SELECT
DISTINCT
ca.descendant_concept_id 
FROM
`{DATASET}.concept` c
JOIN
`{DATASET}.concept_ancestor` ca
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
""".format(DATASET = DATASET)

substantiating_diabetic_drug_concept_ids = pd.io.gbq.read_gbq(create_table_with_substantiating_diabetic_drug_concept_ids, dialect = 'standard')

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
`{DATASET}.drug_exposure` de  -- get the relevant drugs
ON
p.person_id = de.person_id
RIGHT JOIN
`{DATASET}.substantiating_diabetic_drug_concept_ids` t2drugs  -- only focus on the drugs that substantiate diabetes
ON
de.drug_concept_id = t2drugs.descendant_concept_id
WHERE
LOWER(p.src_hpo_id) NOT LIKE '%rdr%'
GROUP BY 1
ORDER BY num_with_diab_and_drugs DESC
""".format(DATASET = DATASET)


diabetics_with_substantiating_drugs = pd.io.gbq.read_gbq(persons_w_t2d_by_condition_and_substantiating_drugs_query, dialect='standard')
# -

diabetics_with_substantiating_drugs

diabetics_with_substantiating_drugs.shape

# ## glucose_lab

valid_glucose_measurements_query = """
CREATE TABLE `{DATASET}.valid_glucose_labs`
OPTIONS (
expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
) AS
SELECT
DISTINCT
c.concept_id, c.concept_name
FROM
`{DATASET}.concept` c
JOIN
`{DATASET}.concept_ancestor` ca
ON
c.concept_id = ca.descendant_concept_id
WHERE
ca.ancestor_concept_id IN (40795740)
AND
c.invalid_reason IS NULL
OR
c.invalid_reason = ''
""".format(DATASET = DATASET)

valid_glucose_measurements = pd.io.gbq.read_gbq(valid_glucose_measurements_query, dialect='standard')

# #### diabetic persons who have at least one 'glucose' measurement

diabetics_with_glucose_measurement_query = """
SELECT
DISTINCT
p.src_hpo_id, COUNT(DISTINCT p.person_id) as num_with_diab_and_glucose
FROM
`{DATASET}.persons_with_diabetes_according_to_condition_table` p
RIGHT JOIN
`{DATASET}.measurement` m
ON
p.person_id = m.person_id -- get the persons with measurements
RIGHT JOIN
`{DATASET}.valid_glucose_labs` vgl
ON
vgl.concept_id = m.measurement_concept_id -- only get those with the substantiating labs
WHERE
LOWER(p.src_hpo_id) NOT LIKE '%rdr%'
GROUP BY 1
ORDER BY num_with_diab_and_glucose DESC
""".format(DATASET = DATASET)

diabetics_with_glucose_measurement = pd.io.gbq.read_gbq(diabetics_with_glucose_measurement_query, dialect='standard')

diabetics_with_glucose_measurement.shape

# ## a1c

hemoglobin_a1c_desc_query = """
CREATE TABLE `{DATASET}.a1c_descendants`
OPTIONS (
expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
)
AS
SELECT
DISTINCT
ca.descendant_concept_id as concept_id
FROM
`{DATASET}.concept_ancestor` ca
WHERE
ca.ancestor_concept_id IN (40789263)
""".format(DATASET = DATASET)

hemoglobin_a1c_desc = pd.io.gbq.read_gbq(hemoglobin_a1c_desc_query, dialect='standard')

diabetics_with_a1c_measurement_query = """
SELECT
DISTINCT
p.src_hpo_id, COUNT(DISTINCT p.person_id) as num_with_diab_and_a1c
FROM
`{DATASET}.persons_with_diabetes_according_to_condition_table` p
RIGHT JOIN
`{DATASET}.measurement` m
ON
p.person_id = m.person_id -- get the persons with measurements
RIGHT JOIN
`{DATASET}.a1c_descendants` a1c
ON
a1c.concept_id = m.measurement_concept_id -- only get those with the substantiating labs
WHERE
p.src_hpo_id NOT LIKE '%rdr%'
GROUP BY 1
ORDER BY num_with_diab_and_a1c DESC
""".format(DATASET = DATASET)

# +
diabetics_with_a1c_measurement = pd.io.gbq.read_gbq(diabetics_with_a1c_measurement_query, dialect='standard')

diabetics_with_a1c_measurement.shape
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
`{DATASET}.drug_exposure` de
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
""".format(DATASET = DATASET)
# -

diabetics_with_insulin = pd.io.gbq.read_gbq(persons_with_insulin_query, dialect='standard')

final_diabetic_df = pd.merge(diabetics_per_site, diabetics_with_substantiating_drugs, on = 'src_hpo_id')

final_diabetic_df['diabetics_w_drugs'] = round(final_diabetic_df['num_with_diab_and_drugs'] / final_diabetic_df['num_with_diab'] * 100, 2)

final_diabetic_df = pd.merge(final_diabetic_df, diabetics_with_glucose_measurement, on = 'src_hpo_id')

final_diabetic_df['diabetics_w_glucose'] = round(final_diabetic_df['num_with_diab_and_glucose'] / final_diabetic_df['num_with_diab'] * 100, 2)

final_diabetic_df = pd.merge(final_diabetic_df, diabetics_with_a1c_measurement, on = 'src_hpo_id')

final_diabetic_df['diabetics_w_a1c'] = round(final_diabetic_df['num_with_diab_and_a1c'] / final_diabetic_df['num_with_diab'] * 100, 2)

final_diabetic_df = pd.merge(final_diabetic_df, diabetics_with_insulin, on = 'src_hpo_id')

final_diabetic_df['diabetics_w_insulin'] = round(final_diabetic_df['num_with_diab_and_insulin'] / final_diabetic_df['num_with_diab'] * 100, 2)

final_diabetic_df = final_diabetic_df.sort_values(by='diabetics_w_glucose', ascending = False)

final_diabetic_df

final_diabetic_df.to_csv("{cwd}/diabetes.csv".format(cwd = cwd))


