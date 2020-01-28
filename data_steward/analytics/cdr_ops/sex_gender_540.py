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

from notebooks import bq, render, parameters
import pandas as pd
import matplotlib.pyplot as plt
import json
import numpy as np

# +
pd.set_option('display.max_colwidth', -1)

DATASET = parameters.LATEST_DATASET
DEID_DATASET_ID = parameters.DEID_DATASET_ID
SANDBOX = parameters.SANDBOX
COMBINED_ONLY = parameters.COMBINED_ONLY

# +
# Tables
PERSON_TABLE_BEFORE_CLEANING_RULE = '{DATASET}.person'.format(DATASET = DEID_DATASET_ID)

PERSON_TABLE_AFTER_CLEANING_RULE = '{SANDBOX}.{COMBINED_ONLY}_dc540_person'.format(
    SANDBOX = SANDBOX, COMBINED_ONLY = COMBINED_ONLY)

CONCEPT_TABLE = '{DATASET}.concept'.format(DATASET = DEID_DATASET_ID)


print("""
DATASET = {DATASET}
PERSON_TABLE_BEFORE_CLEANING_RULE = {PERSON_TABLE_BEFORE_CLEANING_RULE}
PERSON_TABLE_AFTER_CLEANING_RULE = {PERSON_TABLE_AFTER_CLEANING_RULE}
""".format(
    DATASET = DATASET,
    PERSON_TABLE_BEFORE_CLEANING_RULE = PERSON_TABLE_BEFORE_CLEANING_RULE,
    PERSON_TABLE_AFTER_CLEANING_RULE = PERSON_TABLE_AFTER_CLEANING_RULE))
# -

gender = """
SELECT
DISTINCT
p.gender_concept_id, c.concept_name, COUNT(*) as count
FROM
`{PERSON_TABLE_BEFORE_CLEANING_RULE}` p
JOIN
`{DATASET}.concept` c
ON
p.gender_concept_id = c.concept_id
GROUP BY 1, 2
ORDER BY count DESC
"""

# +
gender = gender.format(DATASET = DATASET,
    PERSON_TABLE_BEFORE_CLEANING_RULE = PERSON_TABLE_BEFORE_CLEANING_RULE)

gender_output = bq.query(gender)
render.dataframe(gender_output)
# -

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
GROUP BY 1, 2
ORDER BY count DESC
""".format(
    PERSON_TABLE_AFTER_CLEANING_RULE = PERSON_TABLE_AFTER_CLEANING_RULE,
    CONCEPT_TABLE = CONCEPT_TABLE
)

print(gender_cleaned)

gender_cleaned_output = bq.query(gender_cleaned)
render.dataframe(gender_cleaned_output)

# +
indexes = gender_cleaned_output['concept_name']

count_pre_cr = gender_output['count'].tolist()
count_pre_cr.append(0)
#need to add a row - last concept not represented

count_post_cr = gender_cleaned_output['count'].tolist()

df = pd.DataFrame({'before-cr': count_pre_cr,
                   'post-cr': count_post_cr}, index = indexes)

ax = df.plot.bar(rot=0, color = ['orange', 'blue'])
plt.xticks(rotation = 45)

plt.title("Gender Concepts By Cleaning Rule")
plt.ylabel("Count")
plt.xlabel('concept_name')
# -

gender_query = """
SELECT
DISTINCT
p.gender_concept_id, c.concept_name as gender_concept,
COUNT(*) as count

FROM
`{PERSON_TABLE_BEFORE_CLEANING_RULE}` p
JOIN
`{DATASET}.concept` c
ON
p.gender_concept_id = c.concept_id

GROUP BY 1, 2
ORDER BY count DESC
"""

# +
gender_query = gender_query.format(
    PERSON_TABLE_BEFORE_CLEANING_RULE = PERSON_TABLE_BEFORE_CLEANING_RULE,
    DATASET = DATASET
)

gender = bq.query(gender_query)
render.dataframe(gender)
# -

gender_cleaned = """
SELECT
DISTINCT
p.sex_at_birth_concept_id, c2.concept_name as sex_concept,
p.gender_concept_id, c.concept_name as gender_concept,
COUNT(*) as count

FROM
`{PERSON_TABLE_AFTER_CLEANING_RULE}` p
JOIN
`{DATASET}.concept` c
ON
p.gender_concept_id = c.concept_id

JOIN
`{DATASET}.concept` c2
ON
p.sex_at_birth_concept_id = c2.concept_id

GROUP BY 1, 2, 3, 4
ORDER BY count DESC
"""

# +
gender_cleaned = gender_cleaned.format(
    PERSON_TABLE_AFTER_CLEANING_RULE = PERSON_TABLE_AFTER_CLEANING_RULE,
    DATASET = DATASET)

gender_cleaned_output = bq.query(gender_cleaned)
render.dataframe(gender_cleaned_output)
# -

