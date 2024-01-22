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

# ### Modeling different disease cohorts over time using AoU data
#
# #### Background:
# - Atlas cohorts for different diseases (e.g. hepatatis) have varied dramatically from quarter to quarter in the following dimensions:
#     - Number of patients
#     - Record count (condition_occurrence)
#
# - We want to determine if these fluctations are potentially caused by OMOP vocabulary issues. If this is the case, we should be able to determine similar trends in AoU data.
import utils.bq
from notebooks import parameters

# #### Starting Cohort Instructions:
#
# Hypertrophic Obstructive Cardiomyopathy (#1418) has decreasing counts from 2018 -> 2019.  2x drop betweeen 2018q4 and 2019q2

# +
Q4_2018 = parameters.Q4_2018
Q2_2019 = parameters.Q2_2019

print("""
Quarter 4 2018 Dataset: {Q4_2018}
Quarter 2 2019 Dataset: {Q2_2019}
""".format(Q4_2018=Q4_2018, Q2_2019=Q2_2019))
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
co.condition_concept_id IN (46273025,
46269951,
46269848,
46269838,
45757423,
44783784,
42709934,
42537757,
42537756,
40482865,
40482241,
40481367,
40479839,
40479837,
37398953,
37117749,
37016198,
36717493,
36717219,
36717199,
36715902,
36715901,
36715899,
36715275,
36713809,
36686551,
4345356,
4342767,
4342751,
4342748,
4342664,
4342663,
4342656,
4341634,
4341633,
4341632,
4340799,
4340798,
4340797,
4340509,
4340260,
4340259,
4340258,
4340257,
4322739,
4320492,
4308227,
4307981,
4302002,
4301738,
4301601,
4298577,
4298552,
4289089,
4288677,
4287783,
4280894,
4280893,
4272488,
4262170,
4259504,
4252916,
4248863,
4240850,
4232598,
4230258,
4230196,
4217186,
4216673,
4208262,
4201583,
4200644,
4198680,
4193657,
4192856,
4192561,
4187900,
4187875,
4185618,
4184503,
4182678,
4179201,
4177488,
4171367,
4168036,
4166480,
4160054,
4151446,
4146762,
4094237,
4093449,
4086978,
4079845,
4079225,
4069295,
4057803,
4057801,
4057682,
4057235,
4057233,
4031048,
4029372,
4025853,
4000167,
443865,
442339,
442257,
201957,
195461,
194077,
192674,
81893,
78799,
77923,
77317,
77025,
76022)

GROUP BY 1, 2
ORDER BY num_persons DESC
""".format(Q4_2018=Q4_2018)

# +
q4_2018_hypo_obs_card = utils.bq.query(q4_2018_hypo_obs_card_query)

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
co.condition_concept_id IN (46273025,
46269951,
46269848,
46269838,
45757423,
44783784,
42709934,
42537757,
42537756,
40482865,
40482241,
40481367,
40479839,
40479837,
37398953,
37117749,
37016198,
36717493,
36717219,
36717199,
36715902,
36715901,
36715899,
36715275,
36713809,
36686551,
4345356,
4342767,
4342751,
4342748,
4342664,
4342663,
4342656,
4341634,
4341633,
4341632,
4340799,
4340798,
4340797,
4340509,
4340260,
4340259,
4340258,
4340257,
4322739,
4320492,
4308227,
4307981,
4302002,
4301738,
4301601,
4298577,
4298552,
4289089,
4288677,
4287783,
4280894,
4280893,
4272488,
4262170,
4259504,
4252916,
4248863,
4240850,
4232598,
4230258,
4230196,
4217186,
4216673,
4208262,
4201583,
4200644,
4198680,
4193657,
4192856,
4192561,
4187900,
4187875,
4185618,
4184503,
4182678,
4179201,
4177488,
4171367,
4168036,
4166480,
4160054,
4151446,
4146762,
4094237,
4093449,
4086978,
4079845,
4079225,
4069295,
4057803,
4057801,
4057682,
4057235,
4057233,
4031048,
4029372,
4025853,
4000167,
443865,
442339,
442257,
201957,
195461,
194077,
192674,
81893,
78799,
77923,
77317,
77025,
76022)

GROUP BY 1, 2
ORDER BY num_persons DESC
""".format(Q2_2019=Q2_2019)

# +
q2_2019_hypo_obs_card = utils.bq.query(q2_2019_hypo_obs_card_query)

q2_2019_hypo_obs_card
# -
combination_query = """

SELECT
DISTINCT
q4.*, q2.*, (SUM(q2.num_persons) - SUM(q4.old_num_persons)) as person_difference,
(SUM(q2.num_records) - SUM(q4.old_num_records)) as record_difference
FROM

    (SELECT
    DISTINCT
    co.condition_concept_id as old_condition_concept_id, c.concept_name as old_concept_name,
    COUNT(DISTINCT p.person_id) AS old_num_persons,
    COUNT(DISTINCT co.condition_occurrence_id) as old_num_records,
    ROUND(COUNT(DISTINCT co.condition_occurrence_id) / COUNT(DISTINCT p.person_id), 2) as old_records_per_capita

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
    co.condition_concept_id IN (45766208,
        45766206,
        45766153,
        45766103,
        45766100,
        43020955,
        4339544,
        4263510,
        4176099,
        4111414,
        4111413,
        4108085,
        312728)

    GROUP BY 1, 2
    ORDER BY old_num_persons DESC) q4

    LEFT JOIN

    (SELECT
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
    co.condition_concept_id IN (45766208,
        45766206,
        45766153,
        45766103,
        45766100,
        43020955,
        4339544,
        4263510,
        4176099,
        4111414,
        4111413,
        4108085,
        312728)

    GROUP BY 1, 2
    ORDER BY num_persons DESC) q2

    ON
    q4.old_condition_concept_id = q2.condition_concept_id

    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    ORDER BY old_num_persons DESC

""".format(Q2_2019=Q2_2019, Q4_2018=Q4_2018)

# +
combo = utils.bq.query(combination_query)

combo.append(combo.sum().rename('Total'))

# +
show = combo[[
    'condition_concept_id', 'concept_name', 'person_difference',
    'record_difference'
]]

show.append(show.sum().rename('Total'))
# -
