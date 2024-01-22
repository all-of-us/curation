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

# ## Notebook is intended to show the efficacy of [cleaning rule #540](https://precisionmedicineinitiative.atlassian.net/browse/DC-540)
#
# #### Cleaning rule:
# - Created
#     - sex_at_birth_concept_id: value_as_concept_id
#     - sex_at_birth_source_value: concept_code associated with the value_source_concept_id
#     - sex_at_birth_source_concept_id: value_source_concept_id for the row
# - Populated fields using the 'sex at birth' PPI question
#
#
#
# - Populated the gender fields with the 'Gender Identity' PPI question
#     - gender_concept_id = value_as_concept_id
#     - gender_source_value = concept_code associated with value_source_concept_id
#     - gender_source_concept_id = value_source_concept_id
import utils.bq
from notebooks import render, parameters
import pandas as pd
import matplotlib.pyplot as plt

# +
pd.set_option('display.max_colwidth', -1)

DEID_DATASET_ID = parameters.DEID_DATASET_ID
SANDBOX = parameters.SANDBOX
COMBINED_ONLY = parameters.COMBINED_ONLY

# +
# Tables
PERSON_TABLE_BEFORE_CLEANING_RULE = '{DATASET}.person'.format(
    DATASET=DEID_DATASET_ID)

PERSON_TABLE_AFTER_CLEANING_RULE = '{SANDBOX}.{COMBINED_ONLY}_dc540_person'.format(
    SANDBOX=SANDBOX, COMBINED_ONLY=COMBINED_ONLY)

CONCEPT_TABLE = '{DATASET}.concept'.format(DATASET=DEID_DATASET_ID)

print("""
DEID_DATASET = {DEID_DATASET_ID}
PERSON_TABLE_BEFORE_CLEANING_RULE = {PERSON_TABLE_BEFORE_CLEANING_RULE}
PERSON_TABLE_AFTER_CLEANING_RULE = {PERSON_TABLE_AFTER_CLEANING_RULE}
""".format(DEID_DATASET_ID=DEID_DATASET_ID,
           PERSON_TABLE_BEFORE_CLEANING_RULE=PERSON_TABLE_BEFORE_CLEANING_RULE,
           PERSON_TABLE_AFTER_CLEANING_RULE=PERSON_TABLE_AFTER_CLEANING_RULE))
# -

# ### Before the cleaning rule - get the counts for the gender concept

gender = """
SELECT
DISTINCT
p.gender_concept_id, c.concept_name, COUNT(*) as count
FROM
`{PERSON_TABLE_BEFORE_CLEANING_RULE}` p
JOIN
`{DEID_DATASET_ID}.concept` c
ON
p.gender_concept_id = c.concept_id

WHERE
p.person_id IN
(
SELECT
DISTINCT
o.person_id
FROM
`{DEID_DATASET_ID}.observation` o
WHERE
o.observation_concept_id = 1585838
)

GROUP BY 1, 2
ORDER BY count DESC
"""

# +
gender = gender.format(
    DEID_DATASET_ID=DEID_DATASET_ID,
    PERSON_TABLE_BEFORE_CLEANING_RULE=PERSON_TABLE_BEFORE_CLEANING_RULE)

gender_output = utils.bq.query(gender)
render.dataframe(gender_output)
# -

# ### After the cleaning rule - get the counts for the gender concept

gender_cleaned = """
SELECT
DISTINCT
p.gender_concept_id, c.concept_name, COUNT(*) as count
FROM
`{PERSON_TABLE_AFTER_CLEANING_RULE}` p
JOIN
`{CONCEPT_TABLE}` c
ON
p.gender_concept_id = c.concept_id

-- need to ensure that we are only looking at people who answered the question
WHERE
p.person_id IN
(
SELECT
DISTINCT
o.person_id
FROM
`{DEID_DATASET_ID}.observation` o
WHERE
o.observation_concept_id = 1585838
)

GROUP BY 1, 2
ORDER BY count DESC
""".format(PERSON_TABLE_AFTER_CLEANING_RULE=PERSON_TABLE_AFTER_CLEANING_RULE,
           CONCEPT_TABLE=CONCEPT_TABLE,
           DEID_DATASET_ID=DEID_DATASET_ID)

gender_cleaned_output = utils.bq.query(gender_cleaned)
render.dataframe(gender_cleaned_output)

# +
indexes = gender_cleaned_output['concept_name']

count_pre_cr = gender_output['count'].tolist()

count_post_cr = gender_cleaned_output['count'].tolist()

df = pd.DataFrame({
    'before-cr': count_pre_cr,
    'post-cr': count_post_cr
},
                  index=indexes)

ax = df.plot.bar(rot=0, color=['orange', 'blue'])
plt.xticks(rotation=45)

plt.title("Gender Concepts By Cleaning Rule")
plt.ylabel("Count")
plt.xlabel('concept_name')

# +
a = gender_cleaned_output['count'].tolist()
b = gender_output['count'].tolist()

cr_difference = []

for x, y in zip(a, b):
    cr_difference.append(x - y)
# -

gender_cleaned_output['original'] = gender_output['count']
gender_cleaned_output['difference'] = cr_difference
gender_cleaned_output

# ### Want to also see if the new post-cleaning rule matches the information from the "Gender Identity"  question from the observation table

gender_from_observation = """
SELECT
DISTINCT
o.observation_source_value, o.observation_source_concept_id, c1.concept_name as source_concept_name,
o.value_as_concept_id,  c2.concept_name as value_concept_name,
o.value_source_concept_id, c3.concept_name as value_source_name,
COUNT(*) as count

FROM
`{DATASET}.observation` o -- original observation table

JOIN
`{DATASET}.concept` c1
ON
o.observation_source_concept_id = c1.concept_id

JOIN
`{DATASET}.concept` c2
ON
o.value_as_concept_id = c2.concept_id

JOIN
`{DATASET}.concept` c3
ON
o.value_source_concept_id = c3.concept_id

WHERE
o.observation_concept_id = 1585838  -- want to see "Sex at Birth PPI"

GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY count DESC
""".format(DATASET=DEID_DATASET_ID)

gender_from_observation = utils.bq.query(gender_from_observation)
render.dataframe(gender_from_observation)

# +
indexes = gender_cleaned_output['concept_name']

count_from_person = gender_cleaned_output['count'].tolist()

count_from_observation = gender_from_observation['count'].tolist()

df = pd.DataFrame(
    {
        'from person': count_from_person,
        'from observation': count_from_observation
    },
    index=indexes)

ax = df.plot.bar(rot=0, color=['orange', 'blue'])
plt.xticks(rotation=45)

plt.title("Gender Concepts - from Observation (pre-CR) and Person (post-Cr)")
plt.ylabel("Count")
plt.xlabel('concept_name')

# +
a = gender_cleaned_output['count'].tolist()
b = gender_from_observation['count'].tolist()

cr_difference = []

for x, y in zip(a, b):
    cr_difference.append(x - y)
# -

gender_cleaned_output['original'] = gender_from_observation['count']
gender_cleaned_output['difference'] = cr_difference
gender_cleaned_output

# ### Want to see the sex_at_birth values; want to see that the information from the observation table (PPI) made it over to the person table after the cleaning rule

sex_at_birth_pre_cr = """
SELECT
DISTINCT
o.observation_source_value, o.observation_source_concept_id, c1.concept_name as source_concept_name,
o.value_as_concept_id,  c2.concept_name as value_concept_name,
o.value_source_concept_id, c3.concept_name as value_source_name,
COUNT(*) as count

FROM
`{DATASET}.observation` o -- original observation table

JOIN
`{DATASET}.concept` c1
ON
o.observation_source_concept_id = c1.concept_id

JOIN
`{DATASET}.concept` c2
ON
o.value_as_concept_id = c2.concept_id

JOIN
`{DATASET}.concept` c3
ON
o.value_source_concept_id = c3.concept_id

WHERE
o.observation_concept_id = 1585845  -- want to see "Sex at Birth PPI"

GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY count DESC
""".format(DATASET=DEID_DATASET_ID)

# +
observation_ppi = utils.bq.query(sex_at_birth_pre_cr)

render.dataframe(observation_ppi)
# -

sex_at_birth_post_cr = """
SELECT
DISTINCT
p.sex_at_birth_concept_id, -- should be same as value_as_concept_id
c1.concept_name as sex_at_birth,

p.sex_at_birth_source_value, -- should be the same as value_source_concept_id

p.sex_at_birth_source_concept_id, -- should contain the value_source_concept_id
c3.concept_name as sex_at_birth_source_concept_name,

COUNT(*) as count


FROM
`{PERSON}` p

JOIN
`{DEID_DATASET_ID}.concept` c1
ON
p.sex_at_birth_concept_id = c1.concept_id

JOIN
`{DEID_DATASET_ID}.concept` c3
ON
p.sex_at_birth_source_concept_id = c3.concept_id

-- need to ensure that we are only looking at people who answered the question
WHERE
p.person_id IN
(
SELECT
DISTINCT
o.person_id
FROM
`{DEID_DATASET_ID}.observation` o
WHERE
o.observation_concept_id = 1585845
)

GROUP BY 1, 2, 3, 4, 5
ORDER BY count DESC
""".format(DEID_DATASET_ID=DEID_DATASET_ID,
           PERSON=PERSON_TABLE_AFTER_CLEANING_RULE)

person_sab = utils.bq.query(sex_at_birth_post_cr)
render.dataframe(person_sab)

# +
indexes = person_sab['sex_at_birth_source_value']

count_pre_cr = observation_ppi['count'].tolist()

count_post_cr = person_sab['count'].tolist()

df = pd.DataFrame(
    {
        'Observation Table PPI': count_pre_cr,
        'Person Table Post-Cleaning Rule': count_post_cr
    },
    index=indexes)

ax = df.plot.bar(rot=0, color=['orange', 'blue'])
plt.xticks(rotation=45)

plt.title("Sex at Birth Distribution - Observation to Person Mapping")
plt.ylabel("Count")
plt.xlabel('concept_name')

# +
a = person_sab['count'].tolist()
b = observation_ppi['count'].tolist()

cr_difference = []

for x, y in zip(a, b):
    cr_difference.append(x - y)
# -

person_sab['original'] = observation_ppi['count']
person_sab['difference'] = cr_difference
person_sab

# ## You may notice the slight increase in the number across all of the 'sex at birth' values across the observation table and the person table. The following query can prove informative:

duplicate_sab_concepts = """
SELECT
DISTINCT
p.person_id,

p.sex_at_birth_concept_id, -- should be same as value_as_concept_id
c1.concept_name as sex_at_birth,

p.sex_at_birth_source_value, -- should be the same as value_source_concept_id

p.sex_at_birth_source_concept_id, -- should contain the value_source_concept_id
c3.concept_name as sex_at_birth_source_concept_name,

COUNT(*) as count


FROM
`{PERSON}` p

JOIN
`{DEID_DATASET_ID}.concept` c1
ON
p.sex_at_birth_concept_id = c1.concept_id

JOIN
`{DEID_DATASET_ID}.concept` c3
ON
p.sex_at_birth_source_concept_id = c3.concept_id

-- need to ensure that we are only looking at people who answered the question
WHERE
p.person_id IN
(
SELECT
DISTINCT
o.person_id
FROM
`{DEID_DATASET_ID}.observation` o
WHERE
o.observation_concept_id = 1585845
)

GROUP BY 1, 2, 3, 4, 5, 6
HAVING count > 1
ORDER BY count DESC
""".format(DEID_DATASET_ID=DEID_DATASET_ID,
           PERSON=PERSON_TABLE_AFTER_CLEANING_RULE)

duplicate_sab = utils.bq.query(duplicate_sab_concepts)
render.dataframe(duplicate_sab)

# ## As you can see, bringing the PPI from the observation table over to the person table created artificial duplicates
