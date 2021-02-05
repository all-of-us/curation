# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.4.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# ## This notebook is used to investigate the racial and gender demographics for the EHR data for the AoU program. This notebook will compare the racial and gender demographics for the following groups:
#
# - The overall program
# - Those who have received COVID tests (both positive and negative)
# - Those who have tested positive for COVID

from google.cloud import bigquery
# %reload_ext google.cloud.bigquery
client = bigquery.Client()
# %load_ext google.cloud.bigquery

import bq_utils
import utils.bq
from notebooks import parameters
# %matplotlib inline
import matplotlib.pyplot as plt
import numpy as np
import six
import scipy.stats
import pandas as pd
import math
from operator import add
import os

# +
DATASET = parameters.LATEST_DATASET
VOCAB_DATASET = parameters.MAY_2020_VOCABULARY

RDR = parameters.JUNE_2020_RDR
covid_testing_sandbox = parameters.covid_testing_sandbox
LOOKUP_TABLES = parameters.LOOKUP_TABLES

cwd = os.getcwd()
cwd = str(cwd)

print(f"""
DATASET TO USE: {DATASET}""")

print(f"""
VOCAB DATASET TO USE: {VOCAB_DATASET}""")

print(f"""
RDR TO USE: {RDR}""")

print(f"""
COVID TESTING SANDBOX TO USE: {covid_testing_sandbox}""")

print(f"""
LOOKUP TABLES: {LOOKUP_TABLES}""")

print(f"""
Current working directory is: {cwd}""")
# -

# # Constants

alternate_ancestor_concept = 0
alternate_race_name = 'Not Specified'
person_column_name = 'person_id'


# # Functions

def create_pie_chart(dataframe, title, img_name):
    """
    Function is used to create a pie chart that can show how many persons are in
    each racial group based on the EHR.
    
    Function also saves the outputted pie chart to the current directory
    
    Parameters
    ----------
    dataframe (df): dataframe for a particular table. shows the following:
        a. each of the ancestor_concept_ids for the different racial categories
        b. the name of each of the racial categories
        c. the number of persons in the category           
    
    title (str): title of the graph
    
    img_name (str): title of the image to be saved
    """
    race_name = dataframe['concept_name'].tolist()
    number_persons = dataframe['number_persons_in_racial_category'].tolist()
    percentages = dataframe['percentage_of_total'].tolist()

    labels = []

    # creating the labels for the graph
    for race, percentage, number in zip(race_name, percentages, number_persons):
        string = f'{race}, {percentage}%, {number} persons'
        labels.append(string)

    wedges = [0.1] * len(labels)

    plt.pie(percentages,
            labels=None,
            shadow=True,
            startangle=140,
            explode=wedges)

    plt.axis('equal')
    plt.title(title)
    plt.legend(bbox_to_anchor=(0.9, -0.1, 1.0, 0.85), labels=labels)

    plt.savefig(img_name, bbox_inches="tight")

    plt.show()


def find_people_w_no_race_concept(
        total_persons_df, race_specified_df, alternate_ancestor_concept,
        alternate_race_name):
    """
    Function is used to determine the number of persons whose racial category has
    not been categorized under the canonical ancestor_concept_ids. 
    
    This function then adds the 'other' category to the race_specified_df
    and creates an additional column to show the 'percentage' of each racial group.
      
    
    Parameters
    ----------
    total_persons_df (df): shows the total number of persons to be analyzed as a
        part of the group
        
    race_specified_df (df): the racial distribution of the group to be analyzed. this
        dataframe, however, does not include people whose racial category is not
        under the traditional ancestor_concept_ids
        
    alternate_ancestor_concept (int): number that represents the 'alternate' ancestor
        concept ID for those who do not belong to a canonical racial group. usually
        will be '0' for simplicity.
        
    alternate_race_name (string): name of the 'racial group' that does not fall
        into the 'alternate' category
    
    
    Returns
    -------
    race_specified_df (df): the original race_specified_df but now contains an additional
        row for the people who do not have a canonical race_concept_id ancestor. also
        contains a column to show the proportion of people from the total group that
        belong to each racial group.
    """
    
    persons_w_race = race_specified_df['number_persons_in_racial_category'].sum()
    
    total_persons = int(total_persons_df['num_persons'])
    
    person_wo_race = total_persons - persons_w_race
    
    # may happen when investigating ethnicity; someone may belong to > 1 group
    if person_wo_race > 0:
        idx = len(general_racial_distributions)

        race_specified_df.loc[idx] = [
            alternate_ancestor_concept, alternate_race_name, person_wo_race]
    
    race_specified_df['percentage_of_total'] = \
        round(race_specified_df['number_persons_in_racial_category'] / total_persons * 100, 2)
    
    race_specified_df = race_specified_df.sort_values(
        'concept_name', ascending=False)
    
    return race_specified_df


def df_column_to_strings(person_df, column_name):
    """
    Function is intended to take a particular column from a dataframe and convert
    the values in said column into a string that can be used for a query (e.g. only
    searching amongst a specific set of persons).
    
    Parameters
    ----------
    person_df (df): dataframe that contains a columnt that can be converted
        into a longer string for use in future queries
    
    column_name (str): name of the column to be converted into a longer string
    
    Returns
    -------
    column_string (str): the string that contains all of the values previously
        contained within the dataframe's columns
    """
    
    column_results = person_df[column_name].tolist()
    
    column_string = "("

    column_length = len(column_results)

    for idx, val in enumerate(column_results):
        column_string += str(val)

        if idx < column_length - 1:
            column_string += ", "
        else:
            column_string += ")"
            
    return column_string


# # Look at the number of persons belonging to each racial category - based on the 'Person' table

# #### Want to identify the appropriate groups to look at - likely at the top of the concept ancestor table

identify_ancestors_for_race_query = f"""
SELECT
  DISTINCT p.race_concept_id,
  c.concept_name,
  COUNT(DISTINCT ca.descendant_concept_id) OVER (PARTITION BY p.race_concept_id) AS number_descendants,
  (ca1.ancestor_concept_id IN (8515, 8516, 8557, 8527, 8657, 0)) AS captured_by_another_ancestor
FROM
  `{DATASET}.unioned_ehr_person` p
JOIN
  `{VOCAB_DATASET}.concept` c
ON
  p.race_concept_id = c.concept_id
JOIN
  `{VOCAB_DATASET}.concept_ancestor` ca
ON
  p.race_concept_id = ca.ancestor_concept_id
JOIN
  `{VOCAB_DATASET}.concept_ancestor` ca1
ON
  p.race_concept_id = ca1.descendant_concept_id
GROUP BY
  p.race_concept_id,
  c.concept_name,
  ca.descendant_concept_id,
  ca1.ancestor_concept_id
ORDER BY
  number_descendants DESC,
  race_concept_id DESC,
  captured_by_another_ancestor DESC
"""

identify_ancestors_for_race = pd.io.gbq.read_gbq(
    identify_ancestors_for_race_query, dialect='standard')

# ### NOTE: If the ancestor has both 'True' and 'False' that means that it will be captured by selected ancestor concept IDs (and that there may be another ancestor not detected). Based on our findings, the ancestor IDs of 8515, 8516, 8557, 8527, and 8657 should suffice for the racial categories.

identify_ancestors_for_race.head(12)

general_racial_distributions_query = f"""
SELECT
DISTINCT
ca.ancestor_concept_id, c.concept_name, 
COUNT(DISTINCT p.person_id) OVER (PARTITION BY ca.ancestor_concept_id) as number_persons_in_racial_category
FROM
`{DATASET}.unioned_ehr_person` p
JOIN
`{VOCAB_DATASET}.concept_ancestor` ca
ON
p.race_concept_id = ca.descendant_concept_id
JOIN
`{VOCAB_DATASET}.concept` c
ON
ca.ancestor_concept_id = c.concept_id
WHERE
ca.ancestor_concept_id IN (8515, 8516, 8557, 8527, 8657, 0)
"""

general_racial_distributions = pd.io.gbq.read_gbq(
    general_racial_distributions_query, dialect='standard')

general_racial_distributions

get_total_person_count = f"""
SELECT
count(*) as num_persons
from
`{DATASET}.unioned_ehr_person` p
"""

get_total_person_count = pd.io.gbq.read_gbq(
    get_total_person_count, dialect='standard')

general_racial_distributions = find_people_w_no_race_concept(
    total_persons_df=get_total_person_count, race_specified_df=general_racial_distributions,
    alternate_ancestor_concept=alternate_ancestor_concept,
    alternate_race_name=alternate_race_name)

create_pie_chart(
    dataframe = general_racial_distributions, 
    title = 'Racial Distributions - All Participants',
    img_name = 'racial_distributions_all_participants.jpg')

# ## Now let's look at the number of persons who have been TESTED for COVID based on the measurement table

persons_w_covid_tests_query = f"""
SELECT
DISTINCT
  person_id,
  MIN(status) AS status,
  COUNT(*) n_rows,
  STRING_AGG(status) statuses
FROM (
  SELECT
    person_id,
    src_hpo_id,
    concept_name,
    value_source_value,
    CASE
      WHEN value_as_concept_id IN (45880296, 9190, 9189, 45878583, 4069590, 36309158) THEN "1 Not detected"
      WHEN value_as_concept_id IN (45877985,
      45884084,
      4126681,
      4183448,
      45876384) THEN "0 Detected"
      WHEN REGEXP_CONTAINS(LOWER(value_source_value), "inv|dup|credit|incon|pend|indet|suffi|not given|see|not test|cance|sent") THEN "2 Other"
      WHEN REGEXP_CONTAINS(LOWER(value_source_value), "not|neg|undete|non") THEN "1 Not detected"
      WHEN REGEXP_CONTAINS(LOWER(value_source_value), "pos|detect|abnormal") THEN "0 Detected"
    ELSE
    "2 Other"
  END
    AS status
  FROM
    `{DATASET}.unioned_ehr_measurement`
  JOIN
    `{DATASET}._mapping_measurement`
  USING
    (measurement_id)
  JOIN
    `{VOCAB_DATASET}.concept_ancestor`
  ON
    (descendant_concept_id=measurement_concept_id)
  JOIN
    `{VOCAB_DATASET}.concept`
  ON
    (value_as_concept_id=concept_id)
  WHERE
    ancestor_concept_id=756055 )
GROUP BY
  person_id
ORDER BY status ASC
"""

persons_w_covid_tests = pd.io.gbq.read_gbq(
    persons_w_covid_tests_query, dialect='standard')

persons_w_covid_tests_string = df_column_to_strings(
    person_df = persons_w_covid_tests, column_name = person_column_name)

tested_racial_distributions_query = f"""
SELECT
DISTINCT
ca.ancestor_concept_id, c.concept_name, 
COUNT(DISTINCT p.person_id) OVER (PARTITION BY ca.ancestor_concept_id) as number_persons_in_racial_category
FROM
`{DATASET}.unioned_ehr_person` p
JOIN
`{VOCAB_DATASET}.concept_ancestor` ca
ON
p.race_concept_id = ca.descendant_concept_id
JOIN
`{VOCAB_DATASET}.concept` c
ON
ca.ancestor_concept_id = c.concept_id
WHERE
ca.ancestor_concept_id IN (8515, 8516, 8557, 8527, 8657, 0)
AND
p.person_id IN {persons_w_covid_tests_string}
"""

tested_racial_distributions = pd.io.gbq.read_gbq(
    tested_racial_distributions_query, dialect='standard')

tested_racial_distributions

get_total_tested_count = f"""
SELECT
COUNT(DISTINCT a.person_id) as num_persons
FROM
(
{persons_w_covid_tests_query}
) a
"""

persons_tested = pd.io.gbq.read_gbq(
    get_total_tested_count, dialect='standard')

persons_tested

tested_racial_distributions = find_people_w_no_race_concept(
    total_persons_df=persons_tested, race_specified_df=tested_racial_distributions,
    alternate_ancestor_concept=alternate_ancestor_concept,
    alternate_race_name=alternate_race_name)

create_pie_chart(
    dataframe = tested_racial_distributions, 
    title = 'Racial Distributions - Tested Participants',
    img_name = 'racial_distributions_tested_participants.jpg')

# ## Now let's look at the number of persons who have been diagnosed with COVID based on the measurement table

persons_w_covid_positives_query = f"""
SELECT
DISTINCT
  person_id,
  MIN(status) AS status,
  COUNT(*) n_rows,
  STRING_AGG(status) statuses
FROM (
  SELECT
    person_id,
    src_hpo_id,
    concept_name,
    value_source_value,
    CASE
      WHEN value_as_concept_id IN (45880296, 9190, 9189, 45878583, 4069590, 36309158) THEN "1 Not detected"
      WHEN value_as_concept_id IN (45877985,
      45884084,
      4126681,
      4183448,
      45876384) THEN "0 Detected"
      WHEN REGEXP_CONTAINS(LOWER(value_source_value), "inv|dup|credit|incon|pend|indet|suffi|not given|see|not test|cance|sent") THEN "2 Other"
      WHEN REGEXP_CONTAINS(LOWER(value_source_value), "not|neg|undete|non") THEN "1 Not detected"
      WHEN REGEXP_CONTAINS(LOWER(value_source_value), "pos|detect|abnormal") THEN "0 Detected"
    ELSE
    "2 Other"
  END
    AS status
  FROM
    `{DATASET}.unioned_ehr_measurement`
  JOIN
    `{DATASET}._mapping_measurement`
  USING
    (measurement_id)
  JOIN
    `{VOCAB_DATASET}.concept_ancestor`
  ON
    (descendant_concept_id=measurement_concept_id)
  JOIN
    `{VOCAB_DATASET}.concept`
  ON
    (value_as_concept_id=concept_id)
  WHERE
    ancestor_concept_id=756055 )
WHERE
    LOWER(status) LIKE '%0%'
GROUP BY
  person_id
ORDER BY status ASC
"""

persons_w_covid_positives = pd.io.gbq.read_gbq(
    persons_w_covid_positives_query, dialect='standard')

persons_w_covid_positives_string = df_column_to_strings(
    person_df = persons_w_covid_positives, column_name = person_column_name)

positive_racial_distributions_query = f"""
SELECT
DISTINCT
ca.ancestor_concept_id, c.concept_name, 
COUNT(DISTINCT p.person_id) OVER (PARTITION BY ca.ancestor_concept_id) as number_persons_in_racial_category
FROM
`{DATASET}.unioned_ehr_person` p
JOIN
`{VOCAB_DATASET}.concept_ancestor` ca
ON
p.race_concept_id = ca.descendant_concept_id
JOIN
`{VOCAB_DATASET}.concept` c
ON
ca.ancestor_concept_id = c.concept_id
WHERE
ca.ancestor_concept_id IN (8515, 8516, 8557, 8527, 8657, 0)
AND
p.person_id IN {persons_w_covid_positives_string}
"""

positive_racial_distributions = pd.io.gbq.read_gbq(
    positive_racial_distributions_query, dialect='standard')

positive_racial_distributions

get_total_positive_count = f"""
SELECT
COUNT(DISTINCT a.person_id) as num_persons
FROM
(
{persons_w_covid_positives_query}
) a
"""

persons_positive = pd.io.gbq.read_gbq(
    get_total_positive_count, dialect='standard')

positive_racial_distributions = find_people_w_no_race_concept(
    total_persons_df=persons_positive, race_specified_df=positive_racial_distributions,
    alternate_ancestor_concept=alternate_ancestor_concept,
    alternate_race_name=alternate_race_name)

create_pie_chart(
    dataframe = positive_racial_distributions, 
    title = 'Racial Distributions - COVID Positive Participants',
    img_name = 'racial_distributions_positive_participants.jpg')

# # Now lets explore ethnicity in the RDR and COVID testing manual - this should further differentiate amongst those who are Hispanic (compared to the 'race' queries above)

total_ethnicity_distribution_query = f"""
SELECT
  DISTINCT c_eth.concept_id,
  CASE
    WHEN c_eth.concept_name="Hispanic or Latino" THEN "Hispanic, Latino, or Spanish"
    WHEN c_race.concept_name="White" THEN "White"
    WHEN c_race.concept_name="Black or African American" THEN "Black, African American, or African"
    WHEN c_race.concept_name="Asian" THEN "Asian"
  ELSE
  "AIAN, NHPI, or MENA"
END
  AS concept_name,
  COUNT(DISTINCT p.person_id) AS number_persons_in_racial_category
FROM
  `{RDR}.person` p


JOIN
  `{RDR}.concept` c_eth
ON
  p.ethnicity_concept_id = c_eth.concept_id

JOIN
  `{RDR}.concept` c_race
ON
  p.race_concept_id = c_race.concept_id

GROUP BY 
  1, 2
ORDER BY number_persons_in_racial_category DESC
"""

total_ethnicity_distribution = pd.io.gbq.read_gbq(
    total_ethnicity_distribution_query, dialect='standard')

total_persons_query = f"""
SELECT
  SUM(a.num_persons) as num_persons
FROM (
  SELECT
    COUNT(DISTINCT p.person_id) AS num_persons
  FROM
    `{RDR}.person` p
  GROUP BY
    p.person_id
  ORDER BY
    num_persons DESC) a
"""

total_persons = pd.io.gbq.read_gbq(
    total_persons_query, dialect='standard')

total_persons

total_persons_ethnic_distributions = find_people_w_no_race_concept(
    total_persons_df=total_persons,
    race_specified_df=total_ethnicity_distribution,
    alternate_ancestor_concept=alternate_ancestor_concept,
    alternate_race_name=alternate_race_name)

create_pie_chart(
    dataframe = total_persons_ethnic_distributions, 
    title = 'Ethnic Distributions - All Participants',
    img_name = 'ethnic_distributions_total_participants.jpg')

ethnic_COVID_tested_distribution_query = f"""
SELECT
  c_eth.concept_id,
  CASE
    WHEN c_eth.concept_name="Hispanic or Latino" THEN "Hispanic, Latino, or Spanish"
    WHEN c_race.concept_name="White" THEN "White"
    WHEN c_race.concept_name="Black or African American" THEN "Black, African American, or African"
    WHEN c_race.concept_name="Asian" THEN "Asian"
  ELSE
  "AIAN, NHPI, or MENA"
END
  AS concept_name,
  CEILING(COUNT(DISTINCT person_id)/20)*20 AS number_persons_in_racial_category
FROM
  `{covid_testing_sandbox}`
JOIN
  `{RDR}.person`
USING
  (person_id)
LEFT JOIN
  `{RDR}.concept_ancestor`
ON
  (race_concept_id=descendant_concept_id)
JOIN
  `{RDR}.concept` c_race
ON
  (coalesce(ancestor_concept_id,
      0)=c_race.concept_id)
JOIN
  `{RDR}.concept` c_eth
ON
  (ethnicity_concept_id=c_eth.concept_id)
WHERE
  (ancestor_concept_id IS NULL
    OR ancestor_concept_id IN (8515,
      8557,
      8657,
      8516,
      8527))
  AND status <> "2 Other"
GROUP BY
  1, 2
ORDER BY
  number_persons_in_racial_category DESC
"""

ethnic_COVID_tested_distribution = pd.io.gbq.read_gbq(
    ethnic_COVID_tested_distribution_query, dialect='standard')

ethnic_COVID_tested_distribution

total_persons_tested_query = f"""
SELECT
  CEILING(COUNT(DISTINCT person_id)/20)*20 AS num_persons
FROM
  `{covid_testing_sandbox}`
JOIN
  `{RDR}.person`
USING
  (person_id)
LEFT JOIN
  `{RDR}.concept_ancestor`
ON
  (race_concept_id=descendant_concept_id)
JOIN
  `{RDR}.concept` c_race
ON
  (coalesce(ancestor_concept_id,
      0)=c_race.concept_id)
JOIN
  `{RDR}.concept` c_eth
ON
  (ethnicity_concept_id=c_eth.concept_id)
WHERE
  (ancestor_concept_id IS NULL
    OR ancestor_concept_id IN (8515,
      8557,
      8657,
      8516,
      8527))
ORDER BY
  num_persons DESC
"""

total_persons_tested_distribution = pd.io.gbq.read_gbq(
    total_persons_tested_query, dialect='standard')

total_persons_tested_distribution

tested_ethnic_distributions_tested = find_people_w_no_race_concept(
    total_persons_df=total_persons_tested_distribution,
    race_specified_df=ethnic_COVID_tested_distribution,
    alternate_ancestor_concept=alternate_ancestor_concept,
    alternate_race_name=alternate_race_name)

tested_ethnic_distributions_tested

create_pie_chart(
    dataframe = tested_ethnic_distributions_tested, 
    title = 'Ethnic Distributions - COVID Tested Participants',
    img_name = 'ethnic_distributions_tested_participants.jpg')

ethnic_COVID_positive_status_distribution_query = f"""
SELECT
  c_eth.concept_id,
  CASE
    WHEN c_eth.concept_name="Hispanic or Latino" THEN "Hispanic, Latino, or Spanish"
    WHEN c_race.concept_name="White" THEN "White"
    WHEN c_race.concept_name="Black or African American" THEN "Black, African American, or African"
    WHEN c_race.concept_name="Asian" THEN "Asian"
  ELSE
  "AIAN, NHPI, or MENA"
END
  AS concept_name,
  CEILING(COUNT(DISTINCT person_id)/20)*20 AS number_persons_in_racial_category
FROM
  `{covid_testing_sandbox}`
JOIN
  `{RDR}.person`
USING
  (person_id)
LEFT JOIN
  `{RDR}.concept_ancestor`
ON
  (race_concept_id=descendant_concept_id)
JOIN
  `{RDR}.concept` c_race
ON
  (coalesce(ancestor_concept_id,
      0)=c_race.concept_id)
JOIN
  `{RDR}.concept` c_eth
ON
  (ethnicity_concept_id=c_eth.concept_id)
WHERE
  (ancestor_concept_id IS NULL
    OR ancestor_concept_id IN (8515,
      8557,
      8657,
      8516,
      8527))
  AND status <> "2 Other"
GROUP BY
  1, 2, status
HAVING
LOWER(status) LIKE '%0 detected%'
ORDER BY
  number_persons_in_racial_category DESC
"""

ethnic_COVID_positive_status_distribution = pd.io.gbq.read_gbq(
    ethnic_COVID_positive_status_distribution_query, dialect='standard')

ethnic_COVID_positive_status_distribution

total_persons_positive_query = f"""
SELECT
  CEILING(COUNT(DISTINCT person_id)/20)*20 AS num_persons
FROM
  `{covid_testing_sandbox}`
JOIN
  `{RDR}.person`
USING
  (person_id)
LEFT JOIN
  `{RDR}.concept_ancestor`
ON
  (race_concept_id=descendant_concept_id)
JOIN
  `{RDR}.concept` c_race
ON
  (coalesce(ancestor_concept_id,
      0)=c_race.concept_id)
JOIN
  `{RDR}.concept` c_eth
ON
  (ethnicity_concept_id=c_eth.concept_id)
WHERE
  (ancestor_concept_id IS NULL
    OR ancestor_concept_id IN (8515,
      8557,
      8657,
      8516,
      8527))
    AND
    LOWER(status) LIKE '%0 detected%'
ORDER BY
  num_persons DESC
"""

total_persons_positive_distribution = pd.io.gbq.read_gbq(
    total_persons_positive_query, dialect='standard')

total_persons_positive_distribution

tested_ethnic_distributions_positive = find_people_w_no_race_concept(
    total_persons_df=total_persons_positive_distribution,
    race_specified_df=ethnic_COVID_positive_status_distribution,
    alternate_ancestor_concept=alternate_ancestor_concept,
    alternate_race_name=alternate_race_name)

tested_ethnic_distributions_positive

create_pie_chart(
    dataframe = tested_ethnic_distributions_positive, 
    title = 'Ethnic Distributions - COVID Positive Participants',
    img_name = 'ethnic_distributions_positive_participants.jpg')

# # Alternate means of calculating total counts

measurement_and_condition_covid_counts_query = f"""
WITH
  covid_pos AS (
  SELECT
    map.Site_Name,
    COUNT(DISTINCT m.person_id ) AS num_positive
  FROM
    `{DATASET}.unioned_ehr_measurement` AS m  --unioned_ehr_measurement
  JOIN
    `{DATASET}.concept_ancestor` AS ca
  ON
    m.measurement_concept_id = ca.descendant_concept_id
  JOIN
    `{DATASET}._mapping_measurement` AS mm
  ON
    m.measurement_id = mm.measurement_id
  JOIN
    `{LOOKUP_TABLES}.hpo_site_id_mappings` AS map
  ON
    mm.src_hpo_id = LOWER(map.HPO_ID)
  WHERE
    ca.ancestor_concept_id = 756055 -- measurement of SARS-CoV-2
    AND (m.value_source_value LIKE 'D%'
      OR m.value_as_concept_id IN (4183448, 45877985, 45878745, 45884084, 4126681, 45884084, 45876384)
      OR m.value_source_value LIKE 'P%')
  GROUP BY
    map.Site_Name ),
    

  covid_cond AS (
  SELECT
    map.HPO_ID,
    map.Site_Name,
    COUNT(DISTINCT co.person_id ) AS num_covid_cond
  FROM
    `{DATASET}.unioned_ehr_condition_occurrence` AS co
  JOIN
    `{DATASET}.concept_ancestor` AS ca
  ON
    co.condition_concept_id = ca.descendant_concept_id
  JOIN
    `{DATASET}._mapping_condition_occurrence` AS mc
  ON
    co.condition_occurrence_id = mc.condition_occurrence_id
  JOIN
    `{LOOKUP_TABLES}.hpo_site_id_mappings` AS map
  ON
    mc.src_hpo_id = LOWER(map.HPO_ID)
  WHERE
    ca.ancestor_concept_id = 4100065 -- disease due to coronaviridae
  GROUP BY
    map.HPO_ID,
    map.Site_Name )


SELECT
  map.HPO_ID,
  map.Site_Name,
  x.num_positive,
  x.num_screened,
  ROUND(x.num_positive/x.num_screened*100, 2) AS percent_pos_screened,
  cc.num_covid_cond
FROM
  `{LOOKUP_TABLES}.hpo_site_id_mappings` AS map
LEFT JOIN (
  SELECT
    map.HPO_ID,
    map.Site_Name,
    p.num_positive,
    COUNT(DISTINCT m.person_id ) AS num_screened
  FROM
    `{DATASET}.unioned_ehr_measurement` AS m
  JOIN
    `{DATASET}.concept_ancestor` AS ca
  ON
    m.measurement_concept_id = ca.descendant_concept_id
  JOIN
    `{DATASET}._mapping_measurement` AS mm
  ON
    m.measurement_id = mm.measurement_id
  JOIN
    `{LOOKUP_TABLES}.hpo_site_id_mappings` AS map
  ON
    mm.src_hpo_id = LOWER(map.HPO_ID)
  LEFT JOIN
    covid_pos p
  ON
    map.Site_Name = p.Site_Name
  WHERE
    ca.ancestor_concept_id = 756055 -- measurement of SARS-CoV-2
  GROUP BY
    map.HPO_ID,
    map.Site_Name,
    p.num_positive ) AS x
ON
  map.HPO_ID=x.HPO_ID
LEFT JOIN
  covid_cond AS cc
ON
  map.HPO_ID=cc.HPO_ID
WHERE
  map.HPO_ID IS NOT NULL
  AND LENGTH(map.HPO_ID)<>0
ORDER BY
  HPO_ID ASC
-- ;
"""

measurement_and_condition_covid_counts = pd.io.gbq.read_gbq(
    measurement_and_condition_covid_counts_query, dialect='standard')

measurement_and_condition_covid_counts = measurement_and_condition_covid_counts.fillna(0)
measurement_and_condition_covid_counts


