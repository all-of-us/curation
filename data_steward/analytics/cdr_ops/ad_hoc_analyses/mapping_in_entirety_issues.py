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

# ## Notebook is used to complete with [EDQ-355](https://precisionmedicineinitiative.atlassian.net/browse/EDQ-355)
#
#
# #### Background
#
#
# We want to create a notebook by which to determine the number of records in various tables where:
# - the 'source_concept_id' SHOULD be mapped to AT LEAST 2 values 
# **BUT**
# - NOT all of the 'destination' values were properly mapped

from google.cloud import bigquery
# %reload_ext google.cloud.bigquery
client = bigquery.Client()
# %load_ext google.cloud.bigquery

# %matplotlib inline
from notebooks import parameters
from utils import bq
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# +
DATASET = parameters.LATEST_DATASET

print("""
DATASET TO USE: {}
""".format(DATASET))
# -

# # Part I: 
#
# ### a. The following is used to determine the HPOs where the 'mapping' from either a source concept ID or a source value did not work well.
#
# - NOTE: This specifically looks at the relationship of 'qualifier values'

get_source_concepts_that_have_qualifier_value = f"""
WITH source_concepts_w_qualifier_value AS
    (
    SELECT
      DISTINCT cr.concept_id_1,
      c1.standard_concept AS c1_standard,
      c1.concept_name AS c1_name,
      c1.concept_class_id,
      cr.relationship_id,
      cr.concept_id_2,
      c2.standard_concept AS c2_standard,
      c2.concept_name AS c2_name
    FROM
      `{DATASET}.concept_relationship` cr
    JOIN
      `{DATASET}.concept` c1
    ON
      cr.concept_id_1 = c1.concept_id
    JOIN
      `{DATASET}.concept` c2
    ON
      cr.concept_id_2 = c2.concept_id
    WHERE
      (LOWER(c1.concept_class_id) LIKE '%qualifier value')
      AND LOWER(cr.relationship_id) LIKE '%value mapped from%')
"""

get_source_values_that_have_qualifier_value = f"""
source_values_w_qualifier_value AS
(
    SELECT
      DISTINCT cr.concept_id_1,
      c1.standard_concept AS c1_standard,
      c1.concept_name AS c1_name,
      c1.concept_class_id,
      cr.relationship_id,
      cr.concept_id_2,
      c2.standard_concept AS c2_standard,
      c2.concept_name AS c2_name
    FROM
      `{DATASET}.concept_relationship` cr
    JOIN
      `{DATASET}.concept` c1
    ON
      cr.concept_id_1 = c1.concept_id
    JOIN
      `{DATASET}.concept` c2
    ON
      cr.concept_id_2 = c2.concept_id
    WHERE
      (LOWER(c1.concept_class_id) LIKE '%qualifier value')
      AND LOWER(cr.relationship_id) LIKE '%value mapped from%')
"""

get_number_of_concepts_w_failed_mapping = f"""
SELECT
  DISTINCT mm.src_hpo_id,
  m.measurement_concept_id,
  c.concept_name AS measurement_concept_name,
  c1.concept_name AS value_as_concept_name,
  m.measurement_source_concept_id,
  c2.concept_name AS source_concept_name,
  m.measurement_source_value,
  COUNT(*) AS count_incomplete_mappings

FROM
  `{DATASET}.unioned_ehr_measurement` m
JOIN
  `{DATASET}._mapping_measurement` mm
ON
  m.measurement_id = mm.measurement_id
JOIN
  `{DATASET}.concept` c
ON
  m.measurement_concept_id = c.concept_id

JOIN
  `{DATASET}.concept` c1
ON
  m.value_as_concept_id = c1.concept_id

JOIN
  `{DATASET}.concept` c2
ON
  m.measurement_source_concept_id = c2.concept_id

WHERE

m.measurement_source_concept_id IN (
SELECT
DISTINCT source_concepts_w_qualifier_value.concept_id_2
FROM  source_concepts_w_qualifier_value)
  

OR

m.measurement_source_value IN (
SELECT
DISTINCT source_values_w_qualifier_value.c2_name
FROM  source_values_w_qualifier_value)
  
AND 

(
-- value_as_concept_id was not mapped
(m.value_as_concept_id = 0
OR m.value_as_concept_id IS NULL)

OR

-- measurement_concept_id was not mapped
(m.measurement_concept_id = 0
OR m.measurement_concept_id IS NULL))

AND

-- check that at least one source field is populated
((
m.measurement_source_concept_id <> 0
AND
m.measurement_source_concept_id IS NOT NULL
)
OR
(m.measurement_source_value IS NOT NULL))


GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7
ORDER BY
  count_incomplete_mappings DESC
"""

final_query = (get_source_concepts_that_have_qualifier_value + "," +
get_source_values_that_have_qualifier_value +
get_number_of_concepts_w_failed_mapping)

failed_mapping = pd.io.gbq.read_gbq(final_query, dialect='standard')

failed_mapping

# ### b. Now we want to consolodate the different HPOs and see how many records with 'improper mapping' belong to each HPO

number_incomplete_maps_per_hpo = failed_mapping[['src_hpo_id', 'count_incomplete_mappings']]

number_incomplete_maps_per_hpo = number_incomplete_maps_per_hpo.groupby('src_hpo_id')['count_incomplete_mappings'].sum()

number_incomplete_maps_per_hpo = number_incomplete_maps_per_hpo.to_frame()

number_incomplete_maps_per_hpo

# #### Also want to compare to the overall number of records from those HPOs

measurement_rows_per_hpo_query = f"""
SELECT
DISTINCT
mm.src_hpo_id, COUNT(*) as measurement_rows
FROM
`{DATASET}.unioned_ehr_measurement` m
JOIN
`{DATASET}._mapping_measurement` mm
ON
m.measurement_id = mm.measurement_id
GROUP BY mm.src_hpo_id
ORDER BY measurement_rows DESC
"""

measurement_rows_per_hpo = pd.io.gbq.read_gbq(measurement_rows_per_hpo_query, dialect='standard')

final_df = number_incomplete_maps_per_hpo.merge(measurement_rows_per_hpo, how='left', on='src_hpo_id')

final_df['percentage_incomplete_mappings'] = round(final_df['count_incomplete_mappings'] / final_df['measurement_rows'] * 100, 2)

final_df

# ## Now let's look at `source_concept_id` values that should have a "maps to value" relationship to a particular value_concept_id but fail to do so:

supposed_maps_to_value_query = f"""
SELECT
  DISTINCT 
  mm.src_hpo_id,
  m.measurement_source_concept_id,
  c.concept_name AS source_concept_name,
  cr.relationship_id,
  c4.concept_name AS supposed_value_as_concept_name,
  c3.concept_id,
  c3.concept_name AS value_as_concept_name,
  m.measurement_concept_id,
  c2.concept_name AS measurement_concept_name,
  COUNT(*) as count_failed_value_mappings
FROM
  `{DATASET}.unioned_ehr_measurement` m
JOIN
  `{DATASET}._mapping_measurement` mm
ON
  m.measurement_id = mm.measurement_id 
JOIN
  `{DATASET}.concept` c
ON
  m.measurement_source_concept_id = c.concept_id
JOIN
  `{DATASET}.concept` c2
ON
  m.measurement_concept_id = c2.concept_id
JOIN
  `{DATASET}.concept` c3
ON
  m.value_as_concept_id = c3.concept_id
JOIN
  `{DATASET}.concept_relationship` cr
ON
  cr.concept_id_1 = m.measurement_source_concept_id
JOIN
  `{DATASET}.concept` c4
ON
  cr.concept_id_2 = c4.concept_id
WHERE
  LOWER(cr.relationship_id) LIKE '%maps to value%'
AND
  c4.concept_id <> m.value_as_concept_id
GROUP BY
  1, 2, 3, 4, 5, 6, 7, 8, 9
ORDER BY
  count_failed_value_mappings DESC
"""

supposed_maps_to_value = pd.io.gbq.read_gbq(supposed_maps_to_value_query, dialect='standard')

supposed_maps_to_value

number_failed_value_mappings_per_hpo = supposed_maps_to_value[['src_hpo_id', 'count_failed_value_mappings']]

number_failed_value_mappings_per_hpo = number_failed_value_mappings_per_hpo.groupby('src_hpo_id')['count_failed_value_mappings'].sum().to_frame()

number_failed_value_mappings_per_hpo

final_failed_value_mappings_df = number_failed_value_mappings_per_hpo.merge(
    measurement_rows_per_hpo, how='left', on='src_hpo_id')

final_failed_value_mappings_df['percentage_incomplete_value_mappings'] = round(
    final_failed_value_mappings_df['count_failed_value_mappings'] / 
    final_failed_value_mappings_df['measurement_rows'] * 100, 2)

final_failed_value_mappings_df


