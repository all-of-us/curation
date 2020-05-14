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

# ### This notebook is intended to show the percentage of rows where there are 'erroneous dates' in the 6 canonical tables. The 6 canonical tables are as follows:
# - Condition Occurrence
# - Procedure Occurrence
# - Visit Occurrence
# - Drug Exposure
# - Measurement
# - Observation
#
# ### Erroneous dates are those that precede 1900 for the observation table or precede 1980 for all other tables

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

# ## Observation Table

observation_query = """
SELECT
total.src_hpo_id,
-- IFNULL(bad_date.num_bad_rows, 0) AS num_bad_rows, 
-- IFNULL(total.num_rows, 0) AS num_rows,
ROUND(IFNULL(bad_date.num_bad_rows, 0) / IFNULL(total.num_rows, 0) * 100 , 2) as observation

FROM

  (SELECT
  DISTINCT
  mo.src_hpo_id, COUNT(*) as num_rows
  FROM
  `{DATASET}.observation` o
  JOIN
  `{DATASET}._mapping_observation` mo
  ON
  o.observation_id = mo.observation_id
  GROUP BY 1
  ORDER BY num_rows DESC) total

LEFT JOIN

  (SELECT
  DISTINCT
  mo.src_hpo_id, COUNT(*) as num_bad_rows
  FROM
  `{DATASET}.observation` o
  JOIN
  `{DATASET}._mapping_observation` mo
  ON
  o.observation_id = mo.observation_id
  WHERE
  o.observation_datetime < CAST('1900-01-01 00:00:00' AS TIMESTAMP)
  OR
  o.observation_date < CAST('1900-01-01' as DATE)
  GROUP BY 1
  ORDER BY num_bad_rows DESC) bad_date
  
ON
bad_date.src_hpo_id = total.src_hpo_id

WHERE
LOWER(total.src_hpo_id) NOT LIKE '%rdr%'

GROUP BY 1, 2 --, 3, 4
ORDER BY observation DESC
""".format(DATASET=DATASET)

observation_df = pd.io.gbq.read_gbq(observation_query, dialect ='standard')

observation_df

# ## Condition Occurrence Table

condition_query = """
SELECT
total.src_hpo_id,
-- IFNULL(bad_date.num_bad_rows, 0) AS num_bad_rows, 
-- IFNULL(total.num_rows, 0) AS num_rows,
ROUND(IFNULL(bad_date.num_bad_rows, 0) / IFNULL(total.num_rows, 0) * 100 , 2) as condition_occurrence

FROM

  (SELECT
  DISTINCT
  mco.src_hpo_id, COUNT(*) as num_rows
  FROM
  `{DATASET}.condition_occurrence` co
  JOIN
  `{DATASET}._mapping_condition_occurrence` mco
  ON
  co.condition_occurrence_id = mco.condition_occurrence_id
  GROUP BY 1
  ORDER BY num_rows DESC) total

LEFT JOIN

  (SELECT
  DISTINCT
  mco.src_hpo_id, COUNT(*) as num_bad_rows
  FROM
  `{DATASET}.condition_occurrence` co
  JOIN
  `{DATASET}._mapping_condition_occurrence` mco
  ON
  co.condition_occurrence_id = mco.condition_occurrence_id
  
  WHERE
  co.condition_start_datetime < CAST('1980-01-01 00:00:00' AS TIMESTAMP)
  OR
  co.condition_start_date < CAST('1980-01-01' as DATE)
  
  OR
  
  co.condition_end_datetime < CAST('1980-01-01 00:00:00' AS TIMESTAMP)
  OR
  co.condition_end_date < CAST('1980-01-01' as DATE)
  
  GROUP BY 1
  ORDER BY num_bad_rows DESC) bad_date
  
ON
bad_date.src_hpo_id = total.src_hpo_id

WHERE
LOWER(total.src_hpo_id) NOT LIKE '%rdr%'

GROUP BY 1, 2 --, 3, 4
ORDER BY condition_occurrence DESC
""".format(DATASET=DATASET)

condition_occurrence_df = pd.io.gbq.read_gbq(condition_query, dialect ='standard')

condition_occurrence_df

# ## Procedure Occurrence Table

procedure_query = """
SELECT
total.src_hpo_id,
-- IFNULL(bad_date.num_bad_rows, 0) AS num_bad_rows, 
-- IFNULL(total.num_rows, 0) AS num_rows,
ROUND(IFNULL(bad_date.num_bad_rows, 0) / IFNULL(total.num_rows, 0) * 100 , 2) as procedure_occurrence

FROM

  (SELECT
  DISTINCT
  mpo.src_hpo_id, COUNT(*) as num_rows
  FROM
  `{DATASET}.procedure_occurrence` po
  JOIN
  `{DATASET}._mapping_procedure_occurrence` mpo
  ON
  po.procedure_occurrence_id = mpo.procedure_occurrence_id
  GROUP BY 1
  ORDER BY num_rows DESC) total

LEFT JOIN

  (SELECT
  DISTINCT
  mpo.src_hpo_id, COUNT(*) as num_bad_rows
  FROM
  `{DATASET}.procedure_occurrence` po
  JOIN
  `{DATASET}._mapping_procedure_occurrence` mpo
  ON
  po.procedure_occurrence_id = mpo.procedure_occurrence_id
  
  WHERE
  po.procedure_datetime < CAST('1980-01-01 00:00:00' AS TIMESTAMP)
  OR
  po.procedure_date < CAST('1980-01-01' as DATE)
  
  GROUP BY 1
  ORDER BY num_bad_rows DESC) bad_date
  
ON
bad_date.src_hpo_id = total.src_hpo_id

WHERE
LOWER(total.src_hpo_id) NOT LIKE '%rdr%'

GROUP BY 1, 2 --, 3, 4
ORDER BY procedure_occurrence DESC
""".format(DATASET=DATASET)

procedure_occurrence_df = pd.io.gbq.read_gbq(procedure_query, dialect ='standard')

procedure_occurrence_df

# ## Visit Occurrence

visit_query = """
SELECT
total.src_hpo_id,
-- IFNULL(bad_date.num_bad_rows, 0) AS num_bad_rows, 
-- IFNULL(total.num_rows, 0) AS num_rows,
ROUND(IFNULL(bad_date.num_bad_rows, 0) / IFNULL(total.num_rows, 0) * 100 , 2) as visit_occurrence

FROM

  (SELECT
  DISTINCT
  mvo.src_hpo_id, COUNT(*) as num_rows
  FROM
  `{DATASET}.visit_occurrence` vo
  JOIN
  `{DATASET}._mapping_visit_occurrence` mvo
  ON
  vo.visit_occurrence_id = mvo.visit_occurrence_id
  GROUP BY 1
  ORDER BY num_rows DESC) total

LEFT JOIN

  (SELECT
  DISTINCT
  mvo.src_hpo_id, COUNT(*) as num_bad_rows
  FROM
  `{DATASET}.visit_occurrence` vo
  JOIN
  `{DATASET}._mapping_visit_occurrence` mvo
  ON
  vo.visit_occurrence_id = mvo.visit_occurrence_id
  
  WHERE
  vo.visit_start_datetime < CAST('1980-01-01 00:00:00' AS TIMESTAMP)
  OR
  vo.visit_start_date < CAST('1980-01-01' as DATE)
  
  OR
  
  vo.visit_end_datetime < CAST('1980-01-01 00:00:00' AS TIMESTAMP)
  OR
  vo.visit_end_date < CAST('1980-01-01' as DATE)
  
  GROUP BY 1
  ORDER BY num_bad_rows DESC) bad_date
  
ON
bad_date.src_hpo_id = total.src_hpo_id

WHERE
LOWER(total.src_hpo_id) NOT LIKE '%rdr%'

GROUP BY 1, 2 --, 3, 4
ORDER BY visit_occurrence DESC
""".format(DATASET=DATASET)

visit_occurrence_df = pd.io.gbq.read_gbq(visit_query, dialect ='standard')

visit_occurrence_df

# # Drug Exposure

drug_query = """
SELECT
total.src_hpo_id,
-- IFNULL(bad_date.num_bad_rows, 0) AS num_bad_rows, 
-- IFNULL(total.num_rows, 0) AS num_rows,
ROUND(IFNULL(bad_date.num_bad_rows, 0) / IFNULL(total.num_rows, 0) * 100 , 2) as drug_exposure

FROM

  (SELECT
  DISTINCT
  mde.src_hpo_id, COUNT(*) as num_rows
  FROM
  `{DATASET}.drug_exposure` de
  JOIN
  `{DATASET}._mapping_drug_exposure` mde
  ON
  de.drug_exposure_id = mde.drug_exposure_id
  GROUP BY 1
  ORDER BY num_rows DESC) total

LEFT JOIN

  (SELECT
  DISTINCT
  mde.src_hpo_id, COUNT(*) as num_bad_rows
  FROM
  `{DATASET}.drug_exposure` de
  JOIN
  `{DATASET}._mapping_drug_exposure` mde
  ON
  de.drug_exposure_id = mde.drug_exposure_id
  
  WHERE
  de.drug_exposure_start_datetime < CAST('1980-01-01 00:00:00' AS TIMESTAMP)
  OR
  de.drug_exposure_start_date < CAST('1980-01-01' as DATE)
  
  OR
  
  de.drug_exposure_end_datetime < CAST('1980-01-01 00:00:00' AS TIMESTAMP)
  OR
  de.drug_exposure_end_date < CAST('1980-01-01' as DATE)
  
  GROUP BY 1
  ORDER BY num_bad_rows DESC) bad_date
  
ON
bad_date.src_hpo_id = total.src_hpo_id

WHERE
LOWER(total.src_hpo_id) NOT LIKE '%rdr%'

GROUP BY 1, 2 --, 3, 4
ORDER BY drug_exposure DESC
""".format(DATASET=DATASET)

drug_exposure_df = pd.io.gbq.read_gbq(drug_query, dialect ='standard')

drug_exposure_df

# ## Measurement

measurement_query = """
SELECT
total.src_hpo_id,
-- IFNULL(bad_date.num_bad_rows, 0) AS num_bad_rows, 
-- IFNULL(total.num_rows, 0) AS num_rows,
ROUND(IFNULL(bad_date.num_bad_rows, 0) / IFNULL(total.num_rows, 0) * 100 , 2) as measurement

FROM

  (SELECT
  DISTINCT
  mm.src_hpo_id, COUNT(*) as num_rows
  FROM
  `{DATASET}.measurement` m
  JOIN
  `{DATASET}._mapping_measurement` mm
  ON
  m.measurement_id = mm.measurement_id
  GROUP BY 1
  ORDER BY num_rows DESC) total

LEFT JOIN

  (SELECT
  DISTINCT
  mm.src_hpo_id, COUNT(*) as num_bad_rows
  FROM
  `{DATASET}.measurement` m
  JOIN
  `{DATASET}._mapping_measurement` mm
  ON
  m.measurement_id = mm.measurement_id
  WHERE
  m.measurement_datetime < CAST('1900-01-01 00:00:00' AS TIMESTAMP)
  OR
  m.measurement_date < CAST('1900-01-01' as DATE)
  GROUP BY 1
  ORDER BY num_bad_rows DESC) bad_date
  
ON
bad_date.src_hpo_id = total.src_hpo_id

WHERE
LOWER(total.src_hpo_id) NOT LIKE '%rdr%'

GROUP BY 1, 2 --, 3, 4
ORDER BY measurement DESC
""".format(DATASET=DATASET)

measurement_df = pd.io.gbq.read_gbq(measurement_query, dialect ='standard')

measurement_df

# ## Bring it all together

# +
erroneous_date_df = pd.merge(
    site_df, observation_df, how='outer', on='src_hpo_id')

erroneous_date_df = pd.merge(
    erroneous_date_df, measurement_df, how='outer', on='src_hpo_id')

erroneous_date_df = pd.merge(
    erroneous_date_df, visit_occurrence_df, how='outer', on='src_hpo_id')

erroneous_date_df = pd.merge(
    erroneous_date_df, procedure_occurrence_df, how='outer', on='src_hpo_id')

erroneous_date_df = pd.merge(
    erroneous_date_df, drug_exposure_df, how='outer', on='src_hpo_id')

erroneous_date_df = pd.merge(
    erroneous_date_df, condition_occurrence_df, how='outer', on='src_hpo_id')

# +
erroneous_date_df = erroneous_date_df.fillna(0)

erroneous_date_df
# -

erroneous_date_df.to_csv("{cwd}/erroneous_dates.csv".format(cwd = cwd))


