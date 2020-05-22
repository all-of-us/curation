# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
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

from google.cloud import bigquery
# %reload_ext google.cloud.bigquery
client = bigquery.Client()
# %load_ext google.cloud.bigquery

# +
from notebooks import parameters
DATASET = parameters.DEID_Q4_2019
LOOKUP_TABLES = parameters.LOOKUP_TABLES

print(f"Dataset to use: {DATASET}")
print(f"Lookup tables: {LOOKUP_TABLES}")

# +
import warnings

warnings.filterwarnings('ignore')
import pandas as pd
import matplotlib.pyplot as plt
import os

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

Lipid = (40782589, 40795800, 40772572)

CBC = (40789356, 40789120, 40789179, 40772748, 40782735, 40789182, 40786033,
       40779159)

CBCwDiff = (40785788, 40785796, 40779195, 40795733, 40795725, 40772531,
            40779190, 40785793, 40779191, 40782561, 40789266)

CMP = (3049187, 3053283, 40775801, 40779224, 40782562, 40782579, 40785850,
       40785861, 40785869, 40789180, 40789190, 40789527, 40791227, 40792413,
       40792440, 40795730, 40795740, 40795754)

Physical_Measurement = (40654163, 40655804, 40654162, 40655805, 40654167,
                        40654164)

measurement_codes = Lipid + CBC + CBCwDiff + CMP + Physical_Measurement

# # Integration of Units for All Measurements:
#

# #### Getting the numbers for all of the unit concept IDs by site

# +
unit_concept_ids_by_site_query = f"""
CREATE OR REPLACE TABLE `{DATASET}.sites_unit_counts`
OPTIONS (
expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
)
AS
SELECT
1 as row_num, SUM(a.number_total_rows) as number_total_rows FROM (
SELECT
COUNT(m.measurement_id) AS number_total_rows
FROM
`{DATASET}.measurement` m
GROUP BY
m.measurement_id
ORDER BY
number_total_rows DESC) a
"""

unit_concept_ids_by_site = pd.io.gbq.read_gbq(unit_concept_ids_by_site_query, dialect='standard')

# +
unit_concept_ids_by_site_query = f"""
SELECT
*
FROM
`{DATASET}.sites_unit_counts`
"""

unit_concept_ids_by_site = pd.io.gbq.read_gbq(unit_concept_ids_by_site_query, dialect='standard')
# -

unit_concept_ids_by_site

# #### Below are the percentage of rows that could feasibly convert its source data into floats

# +
source_values_that_can_be_converted_to_floats_by_site_query = f"""
CREATE OR REPLACE TABLE `{DATASET}.sites_successful_unit_counts`
OPTIONS (
expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
)
AS
SELECT
1 as row_num, SUM(a.number_of_float_source_values) AS number_of_float_source_values
FROM
(
SELECT
DISTINCT
COUNT(m.measurement_id) AS number_of_float_source_values
FROM
`{DATASET}.measurement` m
LEFT JOIN (
SELECT
  DISTINCT m.measurement_id,
  SAFE_CAST(m.value_source_value AS FLOAT64) AS value_source_is_number
FROM
  `{DATASET}.measurement` m) a
ON
m.measurement_id = a.measurement_id

LEFT JOIN
`{DATASET}.concept` c
ON
m.unit_concept_id = c.concept_id
WHERE
a.value_source_is_number IS NOT NULL
GROUP BY
m.measurement_id
ORDER BY
number_of_float_source_values DESC) a
"""

potential_floats = pd.io.gbq.read_gbq(source_values_that_can_be_converted_to_floats_by_site_query, dialect='standard')
# -

potential_floats

# #### Below are the "successful" unit concept IDs

# +
successful_unit_concept_ids_by_site_query = f"""
CREATE OR REPLACE TABLE `{DATASET}.sites_successful_unit_counts`
OPTIONS (
expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
)
AS
SELECT
1 as row_num, SUM(a.number_successful_units) AS number_successful_units
FROM
(
  SELECT
    DISTINCT
    COUNT(m.measurement_id) AS number_successful_units
  FROM
    `{DATASET}.measurement` m
  LEFT JOIN (
    SELECT
      DISTINCT m.measurement_id,
      SAFE_CAST(m.value_source_value AS FLOAT64) AS value_source_is_number
    FROM
      `{DATASET}.measurement` m) a
  ON
    m.measurement_id = a.measurement_id

  LEFT JOIN
    `{DATASET}.concept` c
  ON
    m.unit_concept_id = c.concept_id
  WHERE
    a.value_source_is_number IS NOT NULL
  AND
    LOWER(c.standard_concept) LIKE '%s%'
  AND
    LOWER(c.domain_id) LIKE '%unit%'
  GROUP BY
    m.measurement_id
  ORDER BY
    number_successful_units DESC) a
"""

successful_unit_concept_ids_by_site = pd.io.gbq.read_gbq(successful_unit_concept_ids_by_site_query, dialect='standard')

# +
successful_unit_concept_ids_by_site_query = f"""
SELECT
*
FROM
`{DATASET}.sites_successful_unit_counts`
"""

successful_unit_concept_ids_by_site = pd.io.gbq.read_gbq(successful_unit_concept_ids_by_site_query, dialect='standard')
# -

successful_unit_concept_ids_by_site

final_all_units_df = pd.merge(unit_concept_ids_by_site, potential_floats, on = 'row_num', how = 'left')

final_all_units_df = pd.merge(final_all_units_df, successful_unit_concept_ids_by_site, on = 'row_num', how = 'left')

final_all_units_df['percentage_float_rows'] = round(final_all_units_df['number_of_float_source_values'] / final_all_units_df['number_total_rows'] * 100, 2)

final_all_units_df['total_unit_success_rate'] = round(final_all_units_df['number_successful_units']/ final_all_units_df['number_of_float_source_values'] * 100, 2)


final_all_units_df = final_all_units_df.fillna(0)

final_all_units_df = final_all_units_df.sort_values(by='total_unit_success_rate', ascending = False)

final_all_units_df

# # Integration of Units for Selected Measurements
#
# #### making the distinction because - according to the [AoU EHR Operations](https://sites.google.com/view/ehrupload/omop-tables/measurement?authuser=0)  page (as of 03/11/2020) - the unit_concept_id are only required for the 'required labs'

selected_measurements = str(measurement_codes)

# +
selected_unit_concept_ids_by_site_query = f"""
CREATE OR REPLACE TABLE `{DATASET}.sites_unit_counts_selected_measurements`
OPTIONS (
expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
)
AS
SELECT
1 as row_num, SUM(a.number_total_selected_measurements) AS number_total_selected_measurements
FROM
(
SELECT
DISTINCT
COUNT(m.measurement_id) AS number_total_selected_measurements
FROM
`{DATASET}.measurement` m
LEFT JOIN
`{DATASET}.concept_ancestor` ca
ON
ca.descendant_concept_id = m.measurement_concept_id
JOIN
`{DATASET}.concept` c
ON
m.measurement_concept_id = c.concept_id
WHERE
ca.ancestor_concept_id IN {selected_measurements}
GROUP BY
m.measurement_id
ORDER BY
number_total_selected_measurements DESC) a
"""

selected_unit_concept_ids_by_site = pd.io.gbq.read_gbq(selected_unit_concept_ids_by_site_query, dialect='standard')
# -

selected_unit_concept_ids_by_site

# #### Now we want to see what kinds of source values should be converted to values with floats (which should thereby be put into units)

# +
selected_source_values_that_can_be_converted_to_floats_by_site_query = f"""
CREATE OR REPLACE TABLE `{DATASET}.sites_successful_unit_counts_selected`
OPTIONS (
expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
)
AS
SELECT
1 as row_num, SUM(a.number_of_float_source_values_selected_measures) as number_of_float_source_values_selected_measures
FROM
(
SELECT
DISTINCT
COUNT(m.measurement_id) AS number_of_float_source_values_selected_measures
FROM
`{DATASET}.measurement` m
LEFT JOIN (
SELECT
  DISTINCT m.measurement_id,
  SAFE_CAST(m.value_source_value AS FLOAT64) AS value_source_is_number
FROM
  `{DATASET}.measurement` m) a
ON
m.measurement_id = a.measurement_id

LEFT JOIN
`{DATASET}.concept` c
ON
m.unit_concept_id = c.concept_id
LEFT JOIN
`{DATASET}.concept_ancestor` ca
ON
ca.descendant_concept_id = m.measurement_concept_id
WHERE
a.value_source_is_number IS NOT NULL
AND
ca.ancestor_concept_id IN {selected_measurements}
GROUP BY
m.measurement_id
ORDER BY
number_of_float_source_values_selected_measures DESC) a
"""

potential_floats_selected = pd.io.gbq.read_gbq(selected_source_values_that_can_be_converted_to_floats_by_site_query, dialect='standard')
# -

potential_floats_selected

# #### Below are the 'successful' unit_concept_ids

# +
successful_unit_concept_ids_by_site_query_selected_meas = f"""
CREATE OR REPLACE TABLE `{DATASET}.sites_successful_unit_counts_selected_meas`
OPTIONS (
expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
)
AS
SELECT
1 as row_num, SUM(a.number_successful_units_selected_measures) AS number_successful_units_selected_measures
FROM(
  SELECT
    DISTINCT
    COUNT(m.measurement_id) AS number_successful_units_selected_measures
  FROM
    `{DATASET}.measurement` m
  LEFT JOIN (
    SELECT
      DISTINCT m.measurement_id,
      SAFE_CAST(m.value_source_value AS FLOAT64) AS value_source_is_number
    FROM
      `{DATASET}.measurement` m) a
  ON
    m.measurement_id = a.measurement_id
  LEFT JOIN
    `{DATASET}.concept` c
  ON
    m.unit_concept_id = c.concept_id
LEFT JOIN
`{DATASET}.concept_ancestor` ca
ON
ca.descendant_concept_id = m.measurement_concept_id
  WHERE
    a.value_source_is_number IS NOT NULL
  AND
    LOWER(c.standard_concept) LIKE '%s%'
  AND
    LOWER(c.domain_id) LIKE '%unit%'
AND
ca.ancestor_concept_id IN {selected_measurements}
  GROUP BY
    m.measurement_id
  ORDER BY
    number_successful_units_selected_measures DESC) a
"""

successful_selected_unit_concept_ids_by_site = pd.io.gbq.read_gbq(
    successful_unit_concept_ids_by_site_query_selected_meas, dialect='standard')
# -

successful_selected_unit_concept_ids_by_site

# +
final_all_units_df = pd.merge(final_all_units_df, selected_unit_concept_ids_by_site, on = 'row_num', how = 'left')

final_all_units_df = pd.merge(final_all_units_df, potential_floats_selected, on = 'row_num', how = 'left')

final_all_units_df = pd.merge(final_all_units_df, successful_selected_unit_concept_ids_by_site, on = 'row_num', how = 'left')
# -

final_all_units_df

final_all_units_df['proportion_sel_meas'] = round(final_all_units_df['number_total_selected_measurements'] / final_all_units_df['number_total_rows'] * 100, 2)





final_all_units_df['percentage_float_rows_selected_measurements'] = round(final_all_units_df['number_of_float_source_values_selected_measures'] / final_all_units_df['number_total_selected_measurements'] * 100, 2)


final_all_units_df['selected_measurements_unit_success_rate'] = round(final_all_units_df['number_successful_units_selected_measures']/ final_all_units_df['number_of_float_source_values_selected_measures'] * 100, 2)





# +
final_all_units_df = final_all_units_df.fillna(0)

final_all_units_df = final_all_units_df.sort_values(by='total_unit_success_rate', ascending = False)

final_all_units_df
# -

final_all_units_df.to_csv("{cwd}/unit_success_rate.csv".format(cwd = cwd))
