# ---
# jupyter:
#   jupytext:
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

# ### Modeling different disease cohorts over time using AoU data
#
# #### Background:
# - Atlas cohorts for different diseases (e.g. hepatatis) have varied dramatically from quarter to quarter in the following dimensions:
#     - Number of patients
#     - Record count (condition_occurrence)
#
# - We want to determine if these fluctations are potentially caused by OMOP vocabulary issues. If this is the case, we should be able to determine similar trends in AoU data.

from notebooks import bq, render, parameters

# #### Starting Cohort Instructions:
#
# Hypertrophic Obstructive Cardiomyopathy (#1418) has decreasing counts from 2018 -> 2019.  2x drop betweeen 2018q4 and 2019q2

# +
Q4_2018 = parameters.Q4_2018
Q2_2019 = parameters.Q2_2019


print(
"""
Quarter 4 2018 Dataset: {Q4_2018}
Quarter 2 2019 Dataset: {Q2_2019}
""".format(Q4_2018 = Q4_2018, Q2_2019 = Q2_2019))
# -

q4_2018_hypo_obs_card_query = """
SELECT
DISTINCT
co.condition_concept_id, c.concept_name, COUNT(DISTINCT p.person_id) AS num_persons, 
COUNT(DISTINCT co.condition_occurrence_id) as num_records, 
ROUND(COUNT(DISTINCT co.condition_occurrence_id) / COUNT(DISTINCT p.person_id), 2) as records_per_capita

FROM
`{Q4_2018}.person` p
JOIN
`{Q4_2018}.condition_occurrence` co
ON
co.person_id = p.person_id

JOIN
`{Q4_2018}.concept` c
ON
co.condition_concept_id = c.concept_id

WHERE
co.condition_concept_id IN (4270625, 316428)

GROUP BY 1, 2
ORDER BY num_persons DESC
""".format(Q4_2018 = Q4_2018)

# +
q4_2018_hypo_obs_card = bq.query(q4_2018_hypo_obs_card_query)

q4_2018_hypo_obs_card
# -

q2_2019_hypo_obs_card_query = """
SELECT
DISTINCT
co.condition_concept_id, c.concept_name, COUNT(DISTINCT p.person_id) AS num_persons, 
COUNT(DISTINCT co.condition_occurrence_id) as num_records, 
ROUND(COUNT(DISTINCT co.condition_occurrence_id) / COUNT(DISTINCT p.person_id), 2) as records_per_capita

FROM
`{Q2_2019}.person` p
JOIN
`{Q2_2019}.condition_occurrence` co
ON
co.person_id = p.person_id

JOIN
`{Q2_2019}.concept` c
ON
co.condition_concept_id = c.concept_id

WHERE
co.condition_concept_id IN (4270625, 316428)

GROUP BY 1, 2
ORDER BY num_persons DESC
""".format(Q2_2019 = Q2_2019)

# +
q2_2019_hypo_obs_card = bq.query(q2_2019_hypo_obs_card_query)

q2_2019_hypo_obs_card
# -


