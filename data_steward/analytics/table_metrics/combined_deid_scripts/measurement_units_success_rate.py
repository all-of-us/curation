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
DATASET = parameters.UNIONED_Q4_2018
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

get_full_names = f"""
select * from {LOOKUP_TABLES}
"""

full_names_df = pd.io.gbq.read_gbq(get_full_names, dialect='standard')

# +
full_names_df.columns = ['org_id', 'src_hpo_id', 'site_name', 'display_order']
columns_to_use = ['src_hpo_id', 'site_name']

full_names_df = full_names_df[columns_to_use]
full_names_df['src_hpo_id'] = full_names_df['src_hpo_id'].str.lower()

# +
cols_to_join = ['src_hpo_id']

site_df = full_names_df
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
unit_concept_ids_by_site_query = """
CREATE TABLE `{DATASET}.sites_unit_counts`
OPTIONS (
expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
)
AS
SELECT
DISTINCT
mm.src_hpo_id, COUNT(m.measurement_id) as number_total_units
FROM
`{DATASET}.measurement` m
JOIN
`{DATASET}._mapping_measurement` mm
ON
m.measurement_id = mm.measurement_id 
GROUP BY 1
ORDER BY number_total_units DESC
""".format(DATASET = DATASET)

unit_concept_ids_by_site = pd.io.gbq.read_gbq(unit_concept_ids_by_site_query, dialect='standard')

# +
unit_concept_ids_by_site_query = """
SELECT
*
FROM
`{DATASET}.sites_unit_counts`
""".format(DATASET = DATASET)

unit_concept_ids_by_site = pd.io.gbq.read_gbq(unit_concept_ids_by_site_query, dialect='standard')
# -

unit_concept_ids_by_site

# #### Below are the "successful" unit concept IDs

# +
successful_unit_concept_ids_by_site_query = """
CREATE TABLE `{DATASET}.sites_successful_unit_counts`
OPTIONS (
expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
)
AS
SELECT
DISTINCT
mm.src_hpo_id, COUNT(m.measurement_id) as number_valid_units
FROM
`{DATASET}.measurement` m
JOIN
`{DATASET}._mapping_measurement` mm
ON
m.measurement_id = mm.measurement_id 
JOIN
`{DATASET}.concept` c
ON
m.unit_concept_id = c.concept_id 
WHERE
c.standard_concept IN ('S')
AND
LOWER(c.domain_id) LIKE '%unit%'
AND
LOWER(mm.src_hpo_id) NOT LIKE '%rdr%'
GROUP BY 1
ORDER BY number_valid_units DESC
""".format(DATASET = DATASET)

successful_unit_concept_ids_by_site = pd.io.gbq.read_gbq(successful_unit_concept_ids_by_site_query, dialect='standard')

# +
successful_unit_concept_ids_by_site_query = """
SELECT
*
FROM
`{DATASET}.sites_successful_unit_counts`
""".format(DATASET = DATASET)

successful_unit_concept_ids_by_site = pd.io.gbq.read_gbq(successful_unit_concept_ids_by_site_query, dialect='standard')
# -

successful_unit_concept_ids_by_site

final_all_units_df = pd.merge(site_df, unit_concept_ids_by_site, on = 'src_hpo_id', how = 'left')

final_all_units_df = pd.merge(final_all_units_df, successful_unit_concept_ids_by_site, on = 'src_hpo_id', how = 'left')

final_all_units_df['total_unit_success_rate'] = round(final_all_units_df['number_valid_units'] / final_all_units_df['number_total_units'] * 100, 2)

final_all_units_df = final_all_units_df.fillna(0)

final_all_units_df = final_all_units_df.sort_values(by='total_unit_success_rate', ascending = False)

final_all_units_df

# # Integration of Units for Selected Measurements
#
# #### making the distinction because - according to the [AoU EHR Operations](https://sites.google.com/view/ehrupload/omop-tables/measurement?authuser=0)  page (as of 03/11/2020) - the unit_concept_id are only required for the 'required labs'

# +
selected_unit_concept_ids_by_site_query = """
CREATE TABLE `{DATASET}.sites_unit_counts_selected_measurements`
OPTIONS (
expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
)
AS
SELECT
DISTINCT
mm.src_hpo_id, COUNT(m.measurement_id) as number_sel_meas
FROM
`{DATASET}.measurement` m
JOIN
`{DATASET}._mapping_measurement` mm
ON
m.measurement_id = mm.measurement_id 
JOIN
`{DATASET}.concept_ancestor` ca
ON
ca.descendant_concept_id = m.measurement_concept_id
JOIN
`{DATASET}.concept` c
ON
m.unit_concept_id = c.concept_id
WHERE
ca.ancestor_concept_id IN {selected_measurements}
AND
LOWER(mm.src_hpo_id) NOT LIKE '%rdr%'
GROUP BY 1
ORDER BY number_sel_meas DESC
""".format(DATASET = DATASET, selected_measurements = measurement_codes)

selected_unit_concept_ids_by_site = pd.io.gbq.read_gbq(selected_unit_concept_ids_by_site_query, dialect='standard')

# +
selected_unit_concept_ids_by_site_query = """
SELECT
*
FROM
`{DATASET}.sites_unit_counts_selected_measurements`
""".format(DATASET = DATASET)

selected_unit_concept_ids_by_site = pd.io.gbq.read_gbq(selected_unit_concept_ids_by_site_query, dialect='standard')
# -

selected_unit_concept_ids_by_site

# #### Below are the 'successful' unit_concept_ids

# +
successful_selected_unit_concept_ids_by_site_query = """
CREATE TABLE `{DATASET}.sites_successful_unit_counts_sel_meas`
OPTIONS (
expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
)
AS
SELECT
DISTINCT
mm.src_hpo_id, COUNT(m.unit_concept_id) as number_valid_units_sel_meas
FROM
`{DATASET}.measurement` m
JOIN
`{DATASET}._mapping_measurement` mm
ON
m.measurement_id = mm.measurement_id 
JOIN
`{DATASET}.concept_ancestor` ca
ON
ca.descendant_concept_id = m.measurement_concept_id
JOIN
`{DATASET}.concept` c
ON
m.unit_concept_id = c.concept_id
WHERE
c.standard_concept IN ('S')
AND
LOWER(c.domain_id) LIKE '%unit%'
AND
ca.ancestor_concept_id IN {selected_measurements}
AND
LOWER(mm.src_hpo_id) NOT LIKE '%rdr%'
GROUP BY 1
ORDER BY number_valid_units_sel_meas DESC
""".format(DATASET = DATASET, selected_measurements = measurement_codes)

successful_selected_unit_concept_ids_by_site = pd.io.gbq.read_gbq(successful_selected_unit_concept_ids_by_site_query, dialect='standard')

# +
successful_selected_unit_concept_ids_by_site_query = """
SELECT
*
FROM
`{DATASET}.sites_successful_unit_counts_sel_meas`
""".format(DATASET = DATASET)

successful_selected_unit_concept_ids_by_site = pd.io.gbq.read_gbq(successful_selected_unit_concept_ids_by_site_query, dialect='standard')
# -

successful_selected_unit_concept_ids_by_site

# +
final_all_units_df = pd.merge(final_all_units_df, selected_unit_concept_ids_by_site, on = 'src_hpo_id', how = 'left')

final_all_units_df = pd.merge(final_all_units_df, successful_selected_unit_concept_ids_by_site, on = 'src_hpo_id', how = 'left')
# -

final_all_units_df

final_all_units_df['proportion_sel_meas'] = round(final_all_units_df['number_sel_meas'] / final_all_units_df['number_total_units'] * 100, 2)


final_all_units_df['sel_meas_unit_success_rate'] = round(final_all_units_df['number_valid_units_sel_meas'] / final_all_units_df['number_sel_meas'] * 100, 2)


# +
final_all_units_df = final_all_units_df.fillna(0)

final_all_units_df = final_all_units_df.sort_values(by='total_unit_success_rate', ascending = False)

final_all_units_df
# -

final_all_units_df.to_csv("{cwd}/measurement_units.csv".format(cwd = cwd))


