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

# ### This notebook is intended to show the percentage of rows with 'invalid' 'visit_occurrence' (a type of 'foreign key') in each of the 6 canonical tables (minus the visit_occurrence table, as that serves as the reference). 
#
# ### An 'invalid' row would be one where the field value ('visit_occurrence_id') either:
# ###     a. does not exist in the visit_occurrence table
# ###     b. is seemingly associated with another site in the `unioned_ehr_visit_occurrence` table

# ### The notebook will evaluate the following
#
# #### person_id and visit_occurrence_id in the following tables:
#
# - condition occurrence
# - observation
# - drug exposure
# - procedure occurrence
# - measurement
#
# Visit occurrence is excluded as that is the reference

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

site_df = pd.merge(site_df, full_names_df, on=['src_hpo_id'], how='left')
# -

# # visit_occurrence_id evaluations

condition_occurrence_query = f"""
SELECT
DISTINCT
total_rows.src_hpo_id,
-- IFNULL(invalid_row_count.number_rows_w_no_vo, 0) as rows_w_no_valid_vo,
-- total_rows.total_rows,
round(IFNULL(invalid_row_count.number_rows_w_no_valid_vo, 0) / total_rows.total_rows * 100, 2) AS condition_occurrence

FROM

  (SELECT
  DISTINCT
  mco.src_hpo_id, COUNT(mco.src_hpo_id) as total_rows
  FROM
  `{DATASET}.unioned_ehr_condition_occurrence` co
  LEFT JOIN
  `{DATASET}._mapping_condition_occurrence` mco
  ON
  co.condition_occurrence_id = mco.condition_occurrence_id
  GROUP BY 1
  ORDER BY total_rows DESC) total_rows

LEFT JOIN

  (SELECT
  DISTINCT
  mco.src_hpo_id, COUNT(mco.src_hpo_id) as number_rows_w_no_valid_vo

  -- to enable site tracing
  FROM
  `{DATASET}.unioned_ehr_condition_occurrence` co
  LEFT JOIN
  `{DATASET}._mapping_condition_occurrence` mco
  ON
  co.condition_occurrence_id = mco.condition_occurrence_id

  -- to enable visit/src_hpo_id cross-checking
  LEFT JOIN
  `{DATASET}.unioned_ehr_visit_occurrence` vo
  ON
  co.visit_occurrence_id = vo.visit_occurrence_id
  LEFT JOIN
  `{DATASET}._mapping_visit_occurrence` mvo
  ON
  vo.visit_occurrence_id = mvo.visit_occurrence_id


  -- anything dropped by the 'left join'
  WHERE
  co.visit_occurrence_id NOT IN
    (
    SELECT
    DISTINCT vo.visit_occurrence_id
    FROM
    `{DATASET}.unioned_ehr_visit_occurrence` vo
    )

  -- same visit_occurrence_id but traced to different sites
  OR
  mco.src_hpo_id <> mvo.src_hpo_id

  GROUP BY 1
  ORDER BY number_rows_w_no_valid_vo DESC) invalid_row_count

ON
total_rows.src_hpo_id = invalid_row_count.src_hpo_id
ORDER BY condition_occurrence DESC
"""

condition_occurrence_df = pd.io.gbq.read_gbq(condition_occurrence_query, dialect ='standard')

condition_occurrence_df


