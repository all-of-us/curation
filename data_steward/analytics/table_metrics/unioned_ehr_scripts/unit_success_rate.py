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
# %reload_ext google.cloud.bigquery
client = bigquery.Client()
# %load_ext google.cloud.bigquery

# +
from notebooks import parameters
DATASET = parameters.LATEST_DATASET
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

hpo_id_query = f"""
SELECT REPLACE(table_id, '_person', '') AS src_hpo_id
FROM
`{DATASET}.__TABLES__`
WHERE table_id LIKE '%person' 
AND table_id 
NOT LIKE '%unioned_ehr_%' 
AND table_id NOT LIKE '\\\_%'
"""

site_df = pd.io.gbq.read_gbq(hpo_id_query, dialect='standard')

get_full_names = f"""
select * from {LOOKUP_TABLES}.hpo_site_id_mappings
"""

full_names_df = pd.io.gbq.read_gbq(get_full_names, dialect='standard')

# +
full_names_df.columns = ['org_id', 'src_hpo_id', 'site_name', 'display_order']
columns_to_use = ['src_hpo_id', 'site_name']

full_names_df = full_names_df[columns_to_use]
full_names_df['src_hpo_id'] = full_names_df['src_hpo_id'].str.lower()

# +
cols_to_join = ['src_hpo_id']

site_df = pd.merge(site_df, full_names_df, on=['src_hpo_id'], how='left')
# -

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
DISTINCT mm.src_hpo_id,
COUNT(*) AS number_total_rows
FROM
`{DATASET}.unioned_ehr_measurement` m
LEFT JOIN
`{DATASET}._mapping_measurement` mm
ON
m.measurement_id = mm.measurement_id
GROUP BY
1
ORDER BY
number_total_rows DESC
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
DISTINCT mm.src_hpo_id,
COUNT(*) AS number_of_float_source_values
FROM
`{DATASET}.unioned_ehr_measurement` m
LEFT JOIN (
SELECT
  DISTINCT m.measurement_id,
  SAFE_CAST(m.value_source_value AS FLOAT64) AS value_source_is_number
FROM
  `{DATASET}.unioned_ehr_measurement` m) a
ON
m.measurement_id = a.measurement_id
LEFT JOIN
`{DATASET}._mapping_measurement` mm
ON
m.measurement_id = mm.measurement_id
LEFT JOIN
`{DATASET}.concept` c
ON
m.unit_concept_id = c.concept_id
GROUP BY
1
ORDER BY
number_of_float_source_values DESC
"""

potential_floats = pd.io.gbq.read_gbq(source_values_that_can_be_converted_to_floats_by_site_query, dialect='standard')

# +
sites_successful_unit_counts_query = f"""
SELECT
*
FROM
`{DATASET}.sites_successful_unit_counts`
"""

potential_floats = pd.io.gbq.read_gbq(sites_successful_unit_counts_query, dialect='standard')
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
    DISTINCT mm.src_hpo_id,
    COUNT(*) AS number_successful_units
  FROM
    `{DATASET}.unioned_ehr_measurement` m
  LEFT JOIN
    `{DATASET}._mapping_measurement` mm
  ON
    m.measurement_id = mm.measurement_id
  LEFT JOIN
    `{DATASET}.concept` c
  ON
    m.unit_concept_id = c.concept_id
  WHERE
    LOWER(c.standard_concept) LIKE '%s%'
  AND
    LOWER(c.domain_id) LIKE '%unit%'
  GROUP BY
    1
  ORDER BY
    number_successful_units DESC
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

final_all_units_df = pd.merge(site_df, unit_concept_ids_by_site, on = 'src_hpo_id', how = 'left')

final_all_units_df = pd.merge(final_all_units_df, potential_floats, on = 'src_hpo_id', how = 'left')

final_all_units_df = pd.merge(final_all_units_df, successful_unit_concept_ids_by_site, on = 'src_hpo_id', how = 'left')

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
SELECT
DISTINCT mm.src_hpo_id,
COUNT(*) AS number_total_selected_measurements
FROM
`{DATASET}.unioned_ehr_measurement` m
LEFT JOIN
`{DATASET}._mapping_measurement` mm
ON
m.measurement_id = mm.measurement_id
LEFT JOIN
`{DATASET}.union_concept_ancestor` ca
ON
ca.descendant_concept_id = m.measurement_concept_id
JOIN
`{DATASET}.concept` c
ON
m.measurement_concept_id = c.concept_id
WHERE
ca.ancestor_concept_id IN {selected_measurements}
GROUP BY
1
ORDER BY
number_total_selected_measurements DESC
"""

selected_unit_concept_ids_by_site = pd.io.gbq.read_gbq(selected_unit_concept_ids_by_site_query, dialect='standard')
# -

# #### Now we want to see what kinds of source values should be converted to values with floats (which should thereby be put into units)

# +
selected_source_values_that_can_be_converted_to_floats_by_site_query = f"""
SELECT
DISTINCT mm.src_hpo_id,
COUNT(*) AS number_of_float_source_values_selected_measures
FROM
`{DATASET}.unioned_ehr_measurement` m
LEFT JOIN (
SELECT
  DISTINCT m.measurement_id,
  SAFE_CAST(m.value_source_value AS FLOAT64) AS value_source_is_number
FROM
  `{DATASET}.unioned_ehr_measurement` m) a
ON
m.measurement_id = a.measurement_id
LEFT JOIN
`{DATASET}._mapping_measurement` mm
ON
m.measurement_id = mm.measurement_id
LEFT JOIN
`{DATASET}.concept` c
ON
m.unit_concept_id = c.concept_id
LEFT JOIN
`{DATASET}.union_concept_ancestor` ca
ON
ca.descendant_concept_id = m.measurement_concept_id
WHERE
ca.ancestor_concept_id IN {selected_measurements}
GROUP BY
1
ORDER BY
number_of_float_source_values_selected_measures DESC
"""

potential_floats_selected = pd.io.gbq.read_gbq(selected_source_values_that_can_be_converted_to_floats_by_site_query, dialect='standard')
# -

# #### Below are the 'successful' unit_concept_ids

# +
successful_unit_concept_ids_by_site_query_selected_meas = f"""
  SELECT
    DISTINCT mm.src_hpo_id,
    COUNT(*) AS number_successful_units_selected_measures
  FROM
    `{DATASET}.unioned_ehr_measurement` m
  LEFT JOIN
    `{DATASET}._mapping_measurement` mm
  ON
    m.measurement_id = mm.measurement_id
  LEFT JOIN
    `{DATASET}.concept` c
  ON
    m.unit_concept_id = c.concept_id
  LEFT JOIN
    `{DATASET}.union_concept_ancestor` ca
  ON
    ca.descendant_concept_id = m.measurement_concept_id
  WHERE
    LOWER(c.standard_concept) LIKE '%s%'
  AND
    LOWER(c.domain_id) LIKE '%unit%'
  AND
    ca.ancestor_concept_id IN {selected_measurements}
  GROUP BY
    1
  ORDER BY
    number_successful_units_selected_measures DESC
"""

successful_selected_unit_concept_ids_by_site = pd.io.gbq.read_gbq(
    successful_unit_concept_ids_by_site_query_selected_meas, dialect='standard')

# +
final_all_units_df = pd.merge(final_all_units_df, selected_unit_concept_ids_by_site, on = 'src_hpo_id', how = 'left')

final_all_units_df = pd.merge(final_all_units_df, potential_floats_selected, on = 'src_hpo_id', how = 'left')

final_all_units_df = pd.merge(final_all_units_df, successful_selected_unit_concept_ids_by_site, on = 'src_hpo_id', how = 'left')
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
